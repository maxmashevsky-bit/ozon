#!/usr/bin/env python3
import json, os, sys
from stack import Stack, ROOT, local_stack, command, GO_ENV, data

mode = sys.argv[1] if len(sys.argv) > 1 else "up"
if mode in ["up", "up-scale"]:
    stack = Stack(3 if mode == "up-scale" else 1)
    stack.build()
    stack.up()
    stack.save()
    print("Ready:", stack.url, "| JWT files:", stack.directory)
elif mode == "down":
    local_stack().down()
elif mode == "init":
    Stack()
elif mode == "seed":
    stack = local_stack()
    stack.discover()
    stack.issue_tokens()
    file = ROOT / ".local/seed.json"
    ids = json.loads(file.read_text()) if file.exists() else []
    if ids:
        print("Seed already recorded:", file)
        sys.exit(0)
    for i in range(3):
        post = data(stack.url, 'mutation($title:String!){createPost(title:$title,text:"Demonstration data"){id}}', {"title": f"Demo post {i+1}"}, stack.tokens["demo"])["createPost"]["id"]
        ids.append(post)
        file.write_text(json.dumps(ids))
        for j in range(3):
            root = data(stack.url, 'mutation($p:ID!){addComment(postId:$p,text:"Root comment"){id}}', {"p": post}, stack.tokens["demo"])["addComment"]["id"]
            data(stack.url, 'mutation($p:ID!,$r:ID!){addComment(postId:$p,parentId:$r,text:"Reply"){id}}', {"p": post, "r": root}, stack.tokens["demo"])
    print("Seed created; IDs saved in", file)
elif mode == "clean-seed":
    stack = local_stack()
    file = ROOT / ".local/seed.json"
    ids = json.loads(file.read_text()) if file.exists() else []
    if ids:
        numbers = ",".join(str(int(x)) for x in ids)
        stack.sql(f"BEGIN; DELETE FROM comments WHERE post_id IN (SELECT id FROM posts WHERE id IN ({numbers}) AND author_id='demo' AND title LIKE 'Demo post %'); DELETE FROM posts WHERE id IN ({numbers}) AND author_id='demo' AND title LIKE 'Demo post %'; COMMIT;")
        file.unlink()
    print("Tracked demonstration data removed")
else:
    raise SystemExit("unknown command")
