#!/usr/bin/env python3
import concurrent.futures, json, pathlib, queue, shutil, subprocess, sys, threading, time, urllib.request
from stack import ROOT, GO_ENV, command, temporary_stack, poll, status, gql, data
from seed import seed_dataset, clean_dataset

class Stream:
    def __init__(self, url, post, token):
        self.events = queue.Queue()
        self.ready = threading.Event()
        self.heartbeat = threading.Event()
        self.closed = threading.Event()
        self.response = None
        def read():
            try:
                body = json.dumps({"query": "subscription($p:ID!){commentAdded(postId:$p){id text}}", "variables": {"p": post}}).encode()
                request = urllib.request.Request(url + "/query", data=body, headers={"Content-Type": "application/json", "Accept": "text/event-stream", "Authorization": "Bearer " + token})
                with urllib.request.urlopen(request, timeout=10) as response:
                    self.response = response
                    for line in response:
                        if line.startswith(b": ready"):
                            self.ready.set()
                        if line.startswith(b": heartbeat"):
                            self.heartbeat.set()
                        if line.startswith(b"data: "):
                            payload = json.loads(line[6:])
                            if payload.get("errors"):
                                self.events.put({"error": True})
                            else:
                                self.events.put(payload["data"]["commentAdded"])
            except Exception:
                pass
            finally:
                self.closed.set()
        self.thread = threading.Thread(target=read, daemon=True)
        self.thread.start()
        if not self.ready.wait(10):
            raise AssertionError("SSE subscription did not become ready")

    def close(self):
        if self.response:
            self.response.close()


def expect_error(stack, query, variables, code, token=None, headers=None):
    _, result = gql(stack.url, query, variables, token, headers)
    codes = [e.get("extensions", {}).get("code") for e in result.get("errors", [])]
    assert code in codes, (code, codes)
    return result


def verify(stack, output):
    stack.db_up()
    # All three migrators really compete on one empty schema.
    with concurrent.futures.ThreadPoolExecutor(3) as pool:
        results = list(pool.map(lambda _: stack.compose("run", "--rm", "--no-deps", "migrate", capture=True), range(3)))
    assert all(p.returncode == 0 for p in results)
    stack.up()
    env = dict(GO_ENV, TEST_DATABASE_URL=stack.test_dsn())
    with (output / "integration.log").open("w") as log:
        result = subprocess.run(["go", "test", "-race", "-tags=integration", "./..."], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    if result.returncode:
        raise AssertionError("integration tests failed; see integration.log")
    print("PASS integration and concurrent migration", flush=True)
    alice, bob = stack.tokens["alice"], stack.tokens["bob"]
    create = 'mutation{createPost(title:"system test",text:"persistent"){id authorId}}'
    expect_error(stack, create, {}, "UNAUTHENTICATED", headers={"X-User-ID": "alice"})
    expect_error(stack, create, {}, "UNAUTHENTICATED", token="malformed-secret-marker")
    p = data(stack.url, create, token=alice)["createPost"]["id"]
    add = 'mutation($p:ID!,$text:String!,$r:ID){addComment(postId:$p,parentId:$r,text:$text){id parentId}}'
    roots = [data(stack.url, add, {"p": p, "text": "root"}, alice)["addComment"]["id"] for _ in range(3)]
    reply = data(stack.url, add, {"p": p, "r": roots[0], "text": "reply"}, bob)["addComment"]["id"]
    assert reply
    query = 'query($p:ID!,$after:String){comments(postId:$p,first:2,after:$after){edges{node{id}}pageInfo{hasNextPage endCursor}}}'
    first = data(stack.url, query, {"p": p})["comments"]
    second = data(stack.url, query, {"p": p, "after": first["pageInfo"]["endCursor"]})["comments"]
    assert len(first["edges"]) == 2 and len(second["edges"]) == 1
    close = 'mutation($p:ID!,$allowed:Boolean!){setCommentsAllowed(postId:$p,allowed:$allowed){id commentsAllowed}}'
    expect_error(stack, close, {"p": p, "allowed": False}, "FORBIDDEN", bob, {"X-User-ID": "alice"})
    data(stack.url, close, {"p": p, "allowed": False}, alice)
    expect_error(stack, add, {"p": p, "text": "late"}, "FORBIDDEN", bob)
    data(stack.url, close, {"p": p, "allowed": True}, alice)
    print("PASS authenticated HTTP, pagination and authorship", flush=True)

    seed_file = output / 'seed-system.json'
    seed_call = lambda query, variables: data(stack.url, query, variables, stack.tokens['demo'])
    seed_state = seed_dataset(seed_file, seed_call)
    before_seed_repeat = stack.sql("SELECT count(*) FROM posts;") + ':' + stack.sql("SELECT count(*) FROM comments;")
    seed_dataset(seed_file, seed_call)
    assert before_seed_repeat == stack.sql("SELECT count(*) FROM posts;") + ':' + stack.sql("SELECT count(*) FROM comments;")
    foreign = data(stack.url, add, {"p":seed_state['posts']['0'], "text":"user comment on seed"}, alice)['addComment']['id']
    try:
        clean_dataset(seed_file, seed_call, stack.sql)
        raise AssertionError('clean-seed removed a user comment')
    except ValueError as exc:
        assert 'user comments' in str(exc)
    assert data(stack.url, 'query($id:ID!){comment(id:$id){authorId}}', {'id':foreign})['comment']['authorId'] == 'alice'
    stack.sql(f"DELETE FROM comments WHERE id={int(foreign)} AND author_id='alice';")
    assert clean_dataset(seed_file, seed_call, stack.sql) == 3
    assert data(stack.url, 'query($p:ID!){post(id:$p){id}}', {"p":p})['post']['id'] == p
    print("PASS restartable seed and scoped cleanup on PostgreSQL", flush=True)

    stream = Stream(stack.urls[0], p, alice)
    emitted = data(stack.urls[1], add, {"p": p, "text": "cross-instance"}, bob)["addComment"]["id"]
    assert stream.events.get(timeout=5)["id"] == emitted
    assert stream.heartbeat.wait(5), "heartbeat missing"
    # The observable buffer/capacity/cancellation cases are unit tests; this tests real sockets and LISTEN.
    stack.sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='ozon-listener' AND datname=current_database();")
    assert stream.closed.wait(5), "LISTEN loss did not finish the old SSE stream"
    poll(lambda: all(json.load(urllib.request.urlopen(urllib.request.Request(u + '/metrics', headers={'Accept':'application/json'})))["listener_ready"] == 1 for u in stack.urls), label="LISTEN reconnect")
    stream = Stream(stack.urls[0], p, alice)
    emitted = data(stack.urls[2], add, {"p": p, "text": "recovered-listener"}, bob)["addComment"]["id"]
    assert stream.events.get(timeout=5)["id"] == emitted
    print("PASS cross-instance delivery, heartbeat and LISTEN recovery", flush=True)

    # Automatic read-only recovery client: it must emit each stored ID once across an app restart.
    watched = output / "watch.jsonl"
    metric_headers = {'Accept':'application/json'}
    recovery_before = [json.load(urllib.request.urlopen(urllib.request.Request(u+'/metrics',headers=metric_headers))) for u in stack.urls]
    before = int(stack.sql(f"SELECT count(*) FROM comments WHERE post_id={int(p)};"))
    with watched.open("w") as file:
        watcher = subprocess.Popen([str(ROOT / "bin/watch"), "-url", stack.urls[0], "-post", p, "-token-file", str(stack.directory / "alice.token"), "-duration", "35s", "-count", str(before + 1)], cwd=ROOT, stdout=file, stderr=subprocess.PIPE, text=True)
        try:
            poll(lambda: len(watched.read_text().splitlines()) >= before, label="initial feed recovery")
            stack.compose("restart", "app1")
            assert stream.closed.wait(5), "restart did not close SSE"
            poll(lambda: status(stack.urls[0] + "/readyz") == 204, label="app restart")
            data(stack.urls[1], add, {"p": p, "text": "during recovery"}, bob)
            assert watcher.wait(timeout=25) == 0, "automatic watcher did not recover"
        finally:
            if watcher.poll() is None:
                watcher.terminate()
                watcher.wait(timeout=5)
    ids = [json.loads(x)["id"] for x in watched.read_text().splitlines()]
    assert len(ids) == len(set(ids)) == before + 1
    recovery_after = [json.load(urllib.request.urlopen(urllib.request.Request(u+'/metrics',headers=metric_headers))) for u in stack.urls]
    (output/'recovery-resources.json').write_text(json.dumps({'before':recovery_before,'after':recovery_after,'watch_events':len(ids)},indent=2))
    assert data(stack.url, 'query($p:ID!){post(id:$p){text}}', {"p": p})["post"]["text"] == "persistent"
    print("PASS application restart, persistence and automatic deduplicated recovery", flush=True)

    stack.compose("stop", "db")
    for url in stack.urls:
        poll(lambda: status(url + "/readyz") == 503, label="unready on database outage")
        assert status(url + "/healthz") == 204
    result = expect_error(stack, 'query($p:ID!){post(id:$p){id}}', {"p": p}, "UNAVAILABLE")
    assert "postgres://" not in json.dumps(result) and "SELECT" not in json.dumps(result)
    stack.compose("start", "db")
    poll(stack.db_ready, label="database restart")
    for url in stack.urls:
        poll(lambda: status(url + "/readyz") == 204, label="readiness recovery")
    assert data(stack.url, 'query($p:ID!){post(id:$p){id}}', {"p": p})["post"]["id"] == p
    print("PASS database outage and recovery", flush=True)

    # One shared service bucket on the LB, not one bucket on each application.
    stack.env.update(GLOBAL_RPS="5", GLOBAL_BURST="1")
    stack.prepare()
    stack.compose("exec", "-T", "lb", "nginx", "-s", "reload")
    poll(lambda: gql(stack.url, '{__typename}', token=alice)[0] == 429, label="new nginx workers enforcing shared rate")
    statuses = [gql(stack.url, '{__typename}', token=alice)[0] for _ in range(30)]
    assert statuses.count(429) >= 20, statuses
    # Direct process gate: same verified actor, irrespective of forged XFF.
    results = [gql(stack.urls[0], '{__typename}', token=alice, headers={"X-Forwarded-For": str(i)+".0.0.1"})[0] for i in range(600)]
    assert 429 in results
    print("PASS process and shared service rate limits", flush=True)
    stack.logs(output)
    logs = (output / "containers.log").read_text()
    for secret in [alice, bob, (stack.directory / "jwt.key").read_text(), (stack.directory / "db-password").read_text(), "malformed-secret-marker", "postgres://"]:
        assert secret not in logs, "secret appeared in container logs"
    (output / "result.json").write_text(json.dumps({"status": "passed", "replicas": 3, "scenarios": ["migration race", "integration with race", "JWT", "pages", "authorship", "cross-instance SSE", "heartbeat", "LISTEN recovery", "application restart", "feed recovery without duplicates", "persistence", "database outage", "readiness", "rate limits", "sanitized logs"]}, indent=2))

if __name__ == "__main__":
    stack, output = temporary_stack("check", 3, {"SSE_HEARTBEAT": "500ms", "STREAM_LIFETIME": "2m"})
    print("Isolated verification:", output, flush=True)
    try:
        stack.build()
        verify(stack, output)
        print("PASS system verification:", output, flush=True)
    except Exception:
        stack.logs(output)
        raise
    finally:
        stack.down(delete=True)
        shutil.rmtree(stack.directory)
