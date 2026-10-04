#!/usr/bin/env python3
"""Local and disposable Docker Compose stacks. Secrets never enter argv or reports."""
import json, os, pathlib, secrets, socket, subprocess, time, urllib.request, urllib.error, urllib.parse, uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
GO_ENV = dict(os.environ, GOTOOLCHAIN="go1.25.0")

def command(args, *, env=None, capture=False, input=None, check=True):
    return subprocess.run(args, cwd=ROOT, env=env, input=input, text=True,
                          stdout=subprocess.PIPE if capture else None,
                          stderr=subprocess.PIPE if capture else None, check=check)

def poll(fn, timeout=60, label="condition"):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = fn()
            if value:
                return value
        except Exception as exc:
            last = type(exc).__name__
        time.sleep(.1)
    raise RuntimeError(f"timeout waiting for {label}; last error: {last}")

def status(url):
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            return response.status
    except urllib.error.HTTPError as exc:
        return exc.code

def gql(url, query, variables=None, token=None, headers=None, timeout=15):
    h = {"Content-Type": "application/json"}
    if token:
        h["Authorization"] = "Bearer " + token
    h.update(headers or {})
    request = urllib.request.Request(url + "/query", data=json.dumps({"query": query, "variables": variables or {}}).encode(), headers=h)
    try:
        response = urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        body = response.read()
        try:
            result = json.loads(body)
        except ValueError:
            result = {"errors": [{"extensions": {"code": "HTTP_" + str(response.status)}}]}
        return response.status, result

def data(url, query, variables=None, token=None):
    code, result = gql(url, query, variables, token)
    if code != 200 or result.get("errors"):
        reasons = [e.get("extensions", {}).get("code", "UNKNOWN") for e in result.get("errors", [])]
        raise AssertionError(f"GraphQL operation failed: HTTP {code}, codes {reasons}")
    return result["data"]

class Stack:
    def __init__(self, replicas=1, *, project="ozon", directory=None, overrides=None):
        self.project = project
        self.replicas = replicas
        self.temporary = project.startswith(("ozon-check-", "ozon-load-"))
        self.directory = pathlib.Path(directory or ROOT / ".local").resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.env = dict(os.environ)
        self.env.update({"SECRET_DIR": str(self.directory), "NGINX_CONFIG": str(self.directory / "nginx.conf"),
                         "DB_NAME": "ozon_test" if self.temporary else "ozon"})
        defaults = {"APP_CPUS": "1.0" if replicas == 1 else "0.34", "APP_MEMORY": "512m" if replicas == 1 else "170m",
                    "DB_POOL_SIZE": "20" if replicas == 1 else "7", "MAX_OPERATIONS": "64" if replicas == 1 else "21"}
        for name, value in defaults.items():
            self.env.setdefault(name, value)
        if self.temporary:
            # Fixed free host ports survive container restart (Docker's port 0 does not).
            reservations = [socket.socket() for _ in range(5)]
            try:
                for sock in reservations:
                    sock.bind(("127.0.0.1", 0))
                for name, sock in zip(["HTTP_PORT", "DB_PORT", "DIRECT_PORT", "DIRECT_PORT_2", "DIRECT_PORT_3"], reservations):
                    self.env[name] = str(sock.getsockname()[1])
            finally:
                for sock in reservations:
                    sock.close()
        self.env.update(overrides or {})
        self.tokens = {}
        self.urls = []
        self.url = None
        self.prepare()

    def prepare(self):
        key = self.directory / "jwt.key"
        if not key.exists():
            key.write_text(secrets.token_hex(32))
        password_file = self.directory / "db-password"
        if not password_file.exists():
            password = secrets.token_hex(24)
            if not self.temporary:
                # Retain credentials of the original local stack when upgrading its volume.
                result = command(["docker", "inspect", self.project + "-db-1", "--format", "{{json .Config.Env}}"], capture=True, check=False)
                if result.returncode == 0:
                    for item in json.loads(result.stdout):
                        if item.startswith("POSTGRES_PASSWORD="):
                            password = item.split("=", 1)[1]
            password_file.write_text(password)
        password = password_file.read_text().strip()
        dsn = f"postgres://ozon:{urllib.parse.quote(password)}@db:5432/{self.env['DB_NAME']}?sslmode=disable"
        (self.directory / "database-url").write_text(dsn)
        # Readable to the unprivileged application inside Docker; parent directory is private.
        self.directory.chmod(0o700)
        for file in [key, password_file, self.directory / "database-url"]:
            file.chmod(0o644)
        template = (ROOT / "deploy/nginx.conf.template").read_text()
        upstreams = "\n".join(f"    server app{i}:8080 max_fails=1 fail_timeout=2s;" for i in range(1, self.replicas + 1))
        template = template.replace("@@UPSTREAMS@@", upstreams).replace("@@GLOBAL_RPS@@", self.env.get("GLOBAL_RPS", "1200")).replace("@@GLOBAL_BURST@@", self.env.get("GLOBAL_BURST", "240"))
        (self.directory / "nginx.conf").write_text(template)

    def compose(self, *args, capture=False, check=True, input=None):
        base = ["docker", "compose", "-p", self.project, "-f", str(ROOT / "compose.yaml")]
        if self.replicas == 3:
            base += ["--profile", "scale"]
        return command(base + list(args), env=self.env, capture=capture, check=check, input=input)

    def build(self):
        self.compose("build", "app1")

    def port(self, service, port):
        value = self.compose("port", service, str(port), capture=True).stdout.strip()
        return int(value.rsplit(":", 1)[1])

    def db_ready(self):
        return self.compose("exec", "-T", "db", "pg_isready", "-U", "ozon", "-d", self.env["DB_NAME"], capture=True, check=False).returncode == 0

    def db_up(self):
        self.compose("up", "-d", "db")
        poll(self.db_ready, label="isolated PostgreSQL")

    def up(self):
        self.compose("up", "-d", "--remove-orphans", *[f"app{i}" for i in range(1, self.replicas + 1)], "lb")
        self.discover()
        for url in self.urls:
            poll(lambda: status(url + "/readyz") == 204, label=url + " readiness")
        poll(lambda: status(self.url + "/readyz") == 204, label="load balancer")
        self.issue_tokens()

    def discover(self):
        self.urls = [f"http://127.0.0.1:{self.port('app'+str(i),8080)}" for i in range(1, self.replicas + 1)]
        self.url = f"http://127.0.0.1:{self.port('lb',8080)}"

    def issue_tokens(self):
        for user in ["alice", "bob", "bench", "demo"]:
            result = self.compose("run", "--rm", "--no-deps", "--entrypoint", "/token", "migrate", "-user", user, "-ttl", "24h", capture=True)
            token = result.stdout.strip()
            if token.count(".") != 2:
                raise RuntimeError("token tool did not return a JWT")
            self.tokens[user] = token
            path = self.directory / (user + ".token")
            path.write_text(token)
            path.chmod(0o600)

    def sql(self, text):
        return self.compose("exec", "-T", "db", "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "ozon", "-d", self.env["DB_NAME"], "-At", capture=True, input=text).stdout.strip()

    def test_dsn(self):
        if not self.temporary:
            raise RuntimeError("refusing to run tests against the development database")
        password = urllib.parse.quote((self.directory / "db-password").read_text().strip())
        return f"postgres://ozon:{password}@127.0.0.1:{self.port('db',5432)}/ozon_test?sslmode=disable"

    def logs(self, output):
        output = pathlib.Path(output)
        output.mkdir(parents=True, exist_ok=True)
        result = self.compose("logs", "--no-color", capture=True, check=False)
        (output / "containers.log").write_text(result.stdout + result.stderr)
        (output / "status.txt").write_text(self.compose("ps", "-a", capture=True, check=False).stdout)

    def down(self, delete=False):
        if delete and not self.temporary:
            raise RuntimeError("refusing automatic deletion of development data")
        args = ["down", "--remove-orphans"] + (["--volumes"] if delete else [])
        self.compose(*args)

    def save(self):
        safe_keys = ["APP_CPUS", "APP_MEMORY", "DB_POOL_SIZE", "MAX_OPERATIONS", "DB_NAME", "GLOBAL_RPS", "GLOBAL_BURST"]
        (self.directory / "state.json").write_text(json.dumps({"replicas": self.replicas, "project": self.project, "url": self.url, "urls": self.urls, "parameters": {k: self.env[k] for k in safe_keys if k in self.env}}, indent=2))

def local_stack():
    state = ROOT / ".local/state.json"
    replicas = json.loads(state.read_text())["replicas"] if state.exists() else 1
    return Stack(replicas)

def temporary_stack(kind, replicas=3, overrides=None):
    name = "ozon-" + kind + "-" + uuid.uuid4().hex[:10]
    directory = ROOT / ".artifacts" / name
    return Stack(replicas, project=name, directory=directory / "secrets", overrides=overrides), directory
