#!/usr/bin/env python3
import json, os, secrets, sys
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
    # Memory mode needs a signing key, not Docker or database configuration.
    directory = ROOT / ".local"
    directory.mkdir(parents=True, exist_ok=True)
    directory.chmod(0o700)
    key = directory / "jwt.key"
    if not key.exists():
        key.write_text(secrets.token_hex(32))
        key.chmod(0o600)
elif mode in ("seed", "clean-seed"):
    from seed import locked, seed_dataset, clean_dataset
    stack = local_stack()
    stack.discover()
    stack.issue_tokens()
    file = ROOT / ".local/seed.json"
    with locked(file):
        call = lambda query, variables: data(stack.url, query, variables, stack.tokens["demo"])
        if mode == "seed":
            state = seed_dataset(file, call)
            print("Seed complete:", len(state["posts"]), "posts, 9 roots, 9 replies; state:", file)
        else:
            count = clean_dataset(file, call, stack.sql)
            print("Tracked demonstration data removed:", count, "posts")
else:
    raise SystemExit("unknown command")
