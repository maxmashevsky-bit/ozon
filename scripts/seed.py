"""Restartable demo data creation. Exact tags reconcile a committed mutation after a lost response."""
import contextlib
import fcntl
import json
import os
import pathlib
import secrets
import tempfile

POSTS = 'query SeedPosts($after:String){posts(first:100,after:$after){edges{node{id title text authorId}}pageInfo{hasNextPage endCursor}}}'
COMMENTS = 'query SeedComments($p:ID!,$r:ID,$after:String){comments(postId:$p,parentId:$r,first:100,after:$after){edges{node{id text authorId}}pageInfo{hasNextPage endCursor}}}'
CREATE_POST = 'mutation SeedPost($title:String!){createPost(title:$title,text:"Demonstration data"){id}}'
CREATE_COMMENT = 'mutation SeedComment($p:ID!,$r:ID,$text:String!){addComment(postId:$p,parentId:$r,text:$text){id}}'

@contextlib.contextmanager
def locked(path):
    lock = path.with_suffix('.lock')
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open('a+') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        yield

def save(path, state):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.seed-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as handle:
            json.dump(state, handle, indent=2)
            handle.write('\n')
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(name, 0o600)
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(directory)
        finally: os.close(directory)
    finally:
        if os.path.exists(name): os.unlink(name)

def load(path):
    if not path.exists():
        state = {'version':2, 'tag':secrets.token_hex(12), 'legacy_post_ids':[], 'posts':{}, 'roots':{}, 'replies':{}, 'complete':False}
        save(path,state) # The tag must be durable before the first remote mutation.
        return state
    raw = json.loads(path.read_text())
    if isinstance(raw,list):
        if len(raw)>3 or any(not str(value).isdigit() or int(value)<=0 for value in raw):
            raise ValueError('invalid legacy seed.json; no data was changed')
        state = {'version':2, 'tag':secrets.token_hex(12), 'legacy_post_ids':[str(x) for x in raw], 'posts':{}, 'roots':{}, 'replies':{}, 'complete':False}
        save(path,state)
        return state
    if not isinstance(raw,dict) or raw.get('version')!=2 or not isinstance(raw.get('tag'),str) or len(raw['tag'])!=24 or any(c not in '0123456789abcdef' for c in raw['tag']):
        raise ValueError('invalid seed.json; no data was changed')
    for key in ('legacy_post_ids','posts','roots','replies'):
        if key not in raw: raise ValueError('incomplete seed.json structure; no data was changed')
    return raw

def pages(call, query, field, variables):
    after = None
    while True:
        connection = call(query,dict(variables,after=after))[field]
        for edge in connection['edges']: yield edge['node']
        info = connection['pageInfo']
        if not info['hasNextPage']: break
        after = info['endCursor']
        if not after: raise ValueError('pagination cursor missing')

def title(state,i):
    if i < len(state['legacy_post_ids']): return f'Demo post {i+1}'
    return f"Demo post {i+1} [{state['tag']}]"

def comment_text(state,i,j,reply=False):
    if i < len(state['legacy_post_ids']): return 'Reply' if reply else 'Root comment'
    kind = 'reply' if reply else 'root'
    return f"Seed {state['tag']} {kind} {i+1}.{j+1}"

def find_posts(call,state, path=None):
    posts = list(pages(call,POSTS,'posts',{}))
    found = {}
    for i in range(3):
        candidates = [p for p in posts if p['authorId']=='demo' and p['title']==title(state,i) and p['text']=='Demonstration data']
        if not candidates and path is not None and i == len(state['legacy_post_ids']):
            # The old script could commit a post and crash before appending its ID.
            legacy = [p for p in posts if p['authorId']=='demo' and p['title']==f'Demo post {i+1}' and p['text']=='Demonstration data']
            if len(legacy)>1: raise ValueError('ambiguous unrecorded legacy post')
            if legacy:
                state['legacy_post_ids'].append(str(legacy[0]['id']))
                save(path,state)
                candidates = legacy
        if i < len(state['legacy_post_ids']):
            candidates = [p for p in candidates if str(p['id'])==state['legacy_post_ids'][i]]
        if len(candidates)>1: raise ValueError('ambiguous seed post; no new data was added')
        if candidates: found[i]=str(candidates[0]['id'])
    return found

def seed_dataset(path,call):
    state = load(path)
    # Never trust a persisted "complete" flag without checking actual remote state.
    found = find_posts(call,state,path)
    for i in range(3):
        post = found.get(i)
        if post is None:
            if i < len(state['legacy_post_ids']):
                raise ValueError('recorded legacy post is absent or changed; refusing to replace it')
            post = str(call(CREATE_POST,{'title':title(state,i)})['createPost']['id'])
        state['posts'][str(i)] = post
        state['complete'] = False
        save(path,state)
        roots = list(pages(call,COMMENTS,'comments',{'p':post,'r':None}))
        for j in range(3):
            text = comment_text(state,i,j)
            matches = [r for r in roots if r['authorId']=='demo' and r['text']==text]
            if i < len(state['legacy_post_ids']):
                root = str(matches[j]['id']) if len(matches)>j else None
            else:
                if len(matches)>1: raise ValueError('ambiguous seed root')
                root = str(matches[0]['id']) if matches else None
            if root is None:
                root = str(call(CREATE_COMMENT,{'p':post,'r':None,'text':text})['addComment']['id'])
                roots.append({'id':root,'authorId':'demo','text':text})
            state['roots'][f'{i}:{j}'] = root
            save(path,state)
            replies = list(pages(call,COMMENTS,'comments',{'p':post,'r':root}))
            text = comment_text(state,i,j,True)
            matches = [r for r in replies if r['authorId']=='demo' and r['text']==text]
            if len(matches)>1 and i >= len(state['legacy_post_ids']): raise ValueError('ambiguous seed reply')
            if not matches:
                reply = str(call(CREATE_COMMENT,{'p':post,'r':root,'text':text})['addComment']['id'])
            else: reply = str(matches[0]['id'])
            state['replies'][f'{i}:{j}'] = reply
            save(path,state)
    state['complete'] = True
    save(path,state)
    return state

def clean_dataset(path,call,sql):
    if not path.exists(): return 0
    state = load(path)
    found = find_posts(call,state)
    # Only exact post IDs observed through the API, with matching author/title, are deleted.
    # A seed post may have acquired a real user's comment since creation.
    for post in found.values():
        if int(sql(f"SELECT count(*) FROM comments WHERE post_id={int(post)} AND author_id <> 'demo';")):
            raise ValueError('seed post has user comments; refusing cleanup')
    for i,post in found.items():
        value = int(post)
        expected = title(state,i).replace("'","''")
        sql(f"BEGIN; DELETE FROM comments WHERE post_id IN (SELECT id FROM posts WHERE id={value} AND author_id='demo' AND title='{expected}'); DELETE FROM posts WHERE id={value} AND author_id='demo' AND title='{expected}'; COMMIT;")
    path.unlink()
    return len(found)
