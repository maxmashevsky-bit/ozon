import json
import pathlib
import re
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import seed

class FakeAPI:
    def __init__(self):
        self.posts=[]; self.comments=[]; self.next_id=1; self.fail=None
    def add(self,collection,**fields):
        row=dict(id=str(self.next_id),authorId='demo',**fields)
        self.next_id+=1; collection.append(row); return row
    def __call__(self,query,variables):
        if query==seed.POSTS:
            rows=self.posts; field='posts'
        elif query==seed.COMMENTS:
            rows=[c for c in self.comments if c['postId']==variables['p'] and c['parentId']==variables['r']]; field='comments'
        elif query==seed.CREATE_POST:
            row=self.add(self.posts,title=variables['title'],text='Demonstration data')
            if self.fail=='post': self.fail=None; raise ConnectionError('response lost after commit')
            return {'createPost':row}
        elif query==seed.CREATE_COMMENT:
            row=self.add(self.comments,postId=variables['p'],parentId=variables['r'],text=variables['text'])
            kind='reply' if variables['r'] else 'root'
            if self.fail==kind: self.fail=None; raise ConnectionError('response lost after commit')
            return {'addComment':row}
        else: raise AssertionError(query)
        start=int(variables.get('after') or 0)
        selected=rows[start:start+2] # Exercise pagination at every level.
        end=start+len(selected)
        return {field:{'edges':[{'node':row} for row in selected], 'pageInfo':{'hasNextPage':end<len(rows),'endCursor':str(end)}}}
    def sql(self,statement):
        if statement.startswith('SELECT count(*)'):
            id_=re.search(r'post_id=(\d+)',statement).group(1)
            return str(sum(c['postId']==id_ and c['authorId']!='demo' for c in self.comments))
        id_=re.search(r'posts WHERE id=(\d+)',statement).group(1)
        row=next((p for p in self.posts if p['id']==id_),None)
        if row and row['authorId']=='demo' and "title='"+row['title']+"'" in statement:
            self.posts.remove(row)
            self.comments=[c for c in self.comments if c['postId']!=id_]

class SeedTests(unittest.TestCase):
    def assert_dataset(self,api):
        self.assertEqual(len(api.posts),3)
        self.assertEqual(len([x for x in api.comments if x['parentId'] is None]),9)
        self.assertEqual(len([x for x in api.comments if x['parentId'] is not None]),9)
    def test_interrupted_mutations_and_write(self):
        for failure in ('post','root','reply','state_write'):
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as directory:
                path=pathlib.Path(directory)/'seed.json'; api=FakeAPI()
                if failure!='state_write':
                    api.fail=failure
                    with self.assertRaises(ConnectionError): seed.seed_dataset(path,api)
                else:
                    original=seed.save; fired=[False]
                    def fail_save(target,state):
                        if state['posts'] and not fired[0]: fired[0]=True; raise OSError('disk failure after DB commit')
                        return original(target,state)
                    with mock.patch.object(seed,'save',side_effect=fail_save):
                        with self.assertRaises(OSError): seed.seed_dataset(path,api)
                self.assertTrue(path.exists())
                self.assertTrue(seed.seed_dataset(path,api)['complete'])
                self.assert_dataset(api)
                seed.seed_dataset(path,api)
                self.assert_dataset(api)
    def test_legacy_partial_and_scoped_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            path=pathlib.Path(directory)/'seed.json'; api=FakeAPI()
            old=api.add(api.posts,title='Demo post 1',text='Demonstration data')
            unrecorded=api.add(api.posts,title='Demo post 2',text='Demonstration data')
            user=api.add(api.posts,title='user data',text='personal'); user['authorId']='alice'
            api.add(api.comments,postId=old['id'],parentId=None,text='Root comment')
            path.write_text(json.dumps([old['id']]))
            state=seed.seed_dataset(path,api)
            self.assertTrue(state['complete']); self.assertEqual(state['legacy_post_ids'],[old['id'],unrecorded['id']])
            self.assertEqual(len(api.posts),4)
            seed.seed_dataset(path,api)
            self.assertEqual(len(api.posts),4)
            self.assertEqual(seed.clean_dataset(path,api,api.sql),3)
            self.assertEqual(api.posts,[user]); self.assertEqual(api.comments,[])
            self.assertFalse(path.exists())
    def test_invalid_legacy_does_not_mutate(self):
        with tempfile.TemporaryDirectory() as directory:
            path=pathlib.Path(directory)/'seed.json'; path.write_text('["1); DROP TABLE posts"]')
            api=FakeAPI()
            with self.assertRaises(ValueError): seed.seed_dataset(path,api)
            self.assertEqual(api.posts,[])
    def test_cleanup_refuses_user_comment_on_seed_post(self):
        with tempfile.TemporaryDirectory() as directory:
            path=pathlib.Path(directory)/'seed.json';api=FakeAPI()
            state=seed.seed_dataset(path,api)
            user=api.add(api.comments,postId=state['posts']['0'],parentId=None,text='user reply')
            user['authorId']='alice'
            with self.assertRaisesRegex(ValueError,'user comments'):
                seed.clean_dataset(path,api,api.sql)
            self.assertTrue(path.exists())
            self.assertEqual(len(api.posts),3)
            self.assertIn(user,api.comments)

if __name__=='__main__': unittest.main()
