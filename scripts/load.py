#!/usr/bin/env python3
"""Measured HTTP load on an isolated fixture; never touches the development DB."""
import argparse, hashlib, json, os, pathlib, re, shutil, subprocess, threading, time, urllib.request
from stack import ROOT, GO_ENV, command, temporary_stack, poll, status

SCENARIOS = ('read', 'mixed', 'uniform', 'hot', 'branches', 'batch', 'large', 'subscriptions')
DEFAULTS = {
    'smoke': (['read','mixed','uniform','hot','batch','subscriptions'],[8],1,'2s','1s',[1,3]),
    'full': (['read','mixed','uniform','hot','batch','subscriptions','large'],[8],2,'30s','5s',[1,3]),
    'soak': (['subscriptions'],[64],1,'180s','10s',[1,3]),
    'compare': (['read','mixed','uniform','hot','branches','batch'],[1,8,32],1,'4s','1s',[1]),
    'protected': (['read'],[32],1,'5s','1s',[1,3]),
}
PUBLISH_INTERVAL = {'smoke':'10ms','full':'10ms','soak':'100ms','compare':'0s','protected':'0s'}

def seconds(value):
    match = re.fullmatch(r'(\d+(?:\.\d+)?)(ms|s|m)', value)
    if not match:
        raise argparse.ArgumentTypeError('duration must be a positive Go duration such as 500ms, 30s or 2m')
    amount = float(match.group(1)) * {'ms':.001,'s':1,'m':60}[match.group(2)]
    if amount <= 0:
        raise argparse.ArgumentTypeError('duration must be positive')
    return amount

def settings(args):
    scenarios, concurrency, repeats, duration, warmup, replicas = DEFAULTS[args.profile]
    scenarios = args.scenarios.split(',') if args.scenarios else scenarios
    concurrency = [int(x) for x in args.concurrency.split(',')] if args.concurrency else concurrency
    replicas = [int(x) for x in args.replicas.split(',')] if args.replicas else replicas
    repeats = args.repeats if args.repeats is not None else repeats
    duration = args.duration or duration
    warmup = args.warmup or warmup
    if not scenarios or any(x not in SCENARIOS for x in scenarios) or len(set(scenarios)) != len(scenarios):
        raise ValueError('invalid or duplicate scenario; choose from ' + ','.join(SCENARIOS))
    if not concurrency or any(x < 1 or x > 256 for x in concurrency) or len(set(concurrency)) != len(concurrency):
        raise ValueError('concurrency must be unique integers from 1 to 256')
    if not replicas or any(x not in (1,3) for x in replicas) or len(set(replicas)) != len(replicas):
        raise ValueError('replicas must be 1, 3, or 1,3')
    if repeats < 1 or repeats > 20:
        raise ValueError('repeats must be 1..20')
    estimated = len(scenarios)*len(concurrency)*len(replicas)*repeats*(seconds(duration)+seconds(warmup)+2)
    return scenarios,concurrency,repeats,duration,warmup,replicas,estimated

def validate_result(row, profile):
    if row.get('success',0) <= 0 or row.get('unexpected_errors'):
        raise ValueError('no successful operations or unexpected errors')
    if row.get('scenario') == 'subscriptions':
        for key in ('expected_events','received_events','matched_events','missing_events','duplicate_events','unexpected_events','invalid_events','disconnects','disconnect_reasons'):
            if key not in row: raise ValueError('subscription result missing '+key)
        if row['subscribers'] != row['concurrency'] or row['missing_events'] or row['duplicate_events'] or row['unexpected_events'] or row['invalid_events'] or row['disconnects'] or row['disconnect_reasons']:
            raise ValueError('subscription delivery/setup failure')
        if row['matched_events'] != row['expected_events']:
            raise ValueError('subscription event accounting mismatch')
    expected = row.get('expected_errors',{})
    if profile == 'protected':
        if not expected.get('http_429',0) and not expected.get('RATE_LIMITED',0):
            raise ValueError('protected profile did not exercise rate limiting')
    elif expected:
        raise ValueError('capacity profile hit a protective limit')

def metrics(stack):
    return [json.load(urllib.request.urlopen(urllib.request.Request(url + '/metrics', headers={'Accept': 'application/json'}), timeout=3)) for url in stack.urls]

class Sampler:
    def __init__(self, stack):
        self.stack, self.samples, self.stop = stack, [], threading.Event()
        self.sample_errors = []
        self.ids = stack.compose('ps', '-q', capture=True).stdout.split()
        self.error = None
        self.thread = threading.Thread(target=self.safe_run)
    def safe_run(self):
        try:
            self.run()
        except Exception as exc:
            self.error = type(exc).__name__
    def run(self):
        consecutive_errors = 0
        while not self.stop.is_set():
            try:
                sample = {'time': time.time(), 'metrics': metrics(self.stack)}
                result = command(['docker', 'stats', '--no-stream', '--format', '{{json .}}', *self.ids], capture=True)
                sample['containers'] = [json.loads(line) for line in result.stdout.splitlines()]
                self.samples.append(sample)
                consecutive_errors = 0
            except (TimeoutError, OSError) as exc:
                self.sample_errors.append(type(exc).__name__)
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    raise
            self.stop.wait(.25)
    def __enter__(self):
        self.thread.start()
        return self
    def __exit__(self, *args):
        self.stop.set()
        self.thread.join(timeout=15)
        if self.thread.is_alive():
            raise RuntimeError('resource sampler did not stop')
        if self.error:
            raise RuntimeError('resource sampler failed: ' + self.error)
        if not self.samples:
            raise RuntimeError('resource sampler collected no samples')

def reset_fixture(stack, fixture):
    services = ['app'+str(i) for i in range(1, stack.replicas+1)]
    # Do not feed 50,000 fixture notifications into live listeners. The baseline
    # also started the application only after preparing each fixture.
    stack.compose('stop', *services, capture=True)
    stack.sql(fixture + '\nSELECT pg_stat_statements_reset();')
    stack.compose('start', *services, capture=True)
    for url in stack.urls:
        poll(lambda: status(url+'/readyz') == 204, label='fixture application readiness')
    poll(lambda: status(stack.url+'/readyz') == 204, label='fixture load balancer readiness')

def explain(stack):
    queries = [
        'SELECT * FROM posts WHERE id > 50 ORDER BY id LIMIT 21',
        'SELECT * FROM comments WHERE post_id=1 AND parent_id IS NULL AND id > 30 ORDER BY id LIMIT 21',
        'SELECT * FROM comments WHERE post_id=1 AND parent_id=1 AND id > 0 ORDER BY id LIMIT 21',
        'SELECT * FROM comments WHERE post_id=1 AND id > 50 ORDER BY id LIMIT 101',
        '''SELECT b.ordinality,c.* FROM unnest(ARRAY[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]::bigint[]) WITH ORDINALITY b(parent_id,ordinality) CROSS JOIN LATERAL (SELECT * FROM comments WHERE post_id=1 AND parent_id=b.parent_id AND id>0 ORDER BY id LIMIT 21) c'''
    ]
    return '\n\n'.join(q + '\n' + stack.sql('EXPLAIN (ANALYZE, BUFFERS) ' + q + ';') for q in queries)

def run(args):
    scenarios, concurrency, repeats, duration, warmup, replicas, estimated = settings(args)
    publish_interval = args.publish_interval or PUBLISH_INTERVAL[args.profile]
    if publish_interval != '0s': seconds(publish_interval)
    print(f'Plan: {len(scenarios)*len(concurrency)*len(replicas)*repeats} cases, at least {estimated/60:.1f} minutes including SSE drain; fixture reset and startup add time',flush=True)
    groups = []
    for count in replicas:
        # These runs measure capacity. Default protective limits are tested separately.
        overrides = {'SSE_HEARTBEAT': '1s', 'APP_CPUS':'1.0' if count == 1 else '0.34', 'APP_MEMORY':'512m' if count == 1 else '170m', 'DB_POOL_SIZE':'20' if count == 1 else '7', 'MAX_OPERATIONS':'64' if count == 1 else '21'}
        if args.profile != 'protected':
            overrides.update({'RATE_RPS':'100000','RATE_BURST':'100000','GLOBAL_RPS':'100000','GLOBAL_BURST':'100000'})
        stack, output = temporary_stack('load', count, overrides)
        print('Load artifacts:', output, flush=True)
        groups.append(str(output.relative_to(ROOT)))
        try:
            if not args.no_build:
                stack.build()
            stack.up()
            stack.save()
            fixture = (ROOT / 'scripts/fixtures/load.sql').read_text()
            stack.sql('CREATE EXTENSION IF NOT EXISTS pg_stat_statements;')
            stack.sql(fixture)
            (output / 'query-plans.txt').write_text(explain(stack))
            meta = {'profile': args.profile, 'replicas': count, 'duration': duration, 'warmup': warmup, 'publish_interval': publish_interval, 'repeats': repeats,
                    'concurrency': concurrency, 'scenarios': scenarios, 'fixture': {'posts':100,'roots':10000,'replies':40000},
                    'parameters': json.loads((stack.directory / 'state.json').read_text())['parameters'],
                    'postgres': {'cpu':2,'memory':'1g','max_connections':100}, 'load_balancer': {'cpu':.25,'memory':'64m'},
                    'rate_limits': {'per_process_actor_rps':int(overrides.get('RATE_RPS','120')),'shared_lb_rps':int(overrides.get('GLOBAL_RPS','1200'))},
                    'route': 'direct' if args.profile == 'compare' else 'nginx',
                    'commit': command(['git','rev-parse','HEAD'],capture=True).stdout.strip(),
                    'dirty': bool(command(['git','status','--porcelain'],capture=True).stdout),
                    'source_sha256': hashlib.sha256(b''.join(p.read_bytes() for p in sorted(ROOT.rglob('*.go')) if '.artifacts' not in str(p))).hexdigest(),
                    'go': command(['go','version'],env=GO_ENV,capture=True).stdout.strip(),
                    'docker': command(['docker','version','--format','{{.Server.Version}}'],capture=True).stdout.strip(),
                    'postgres_version': stack.sql('SELECT version();'),
                    'image': command(['docker','image','inspect',stack.env.get('APP_IMAGE','ozon-app:local'),'--format','{{.Id}}'],capture=True).stdout.strip(),
                    'metrics_window': 'before warmup through measurement; HTTP latency excludes warmup',
                    'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())}
            (output / 'metadata.json').write_text(json.dumps(meta,indent=2))
            all_results = []
            for repeat in range(1,repeats+1):
                for scenario in scenarios:
                    for level in concurrency:
                        reset_fixture(stack, fixture)
                        case = output / f'{scenario}-c{level}-r{repeat}'
                        case.mkdir()
                        before = metrics(stack)
                        url = stack.urls[0] if args.profile == 'compare' else stack.url
                        with Sampler(stack) as sampler:
                            result = command([str(ROOT/'bin/bench'), '-url',url+'/query','-scenario',scenario,'-concurrency',str(level),'-duration',duration,'-warmup',warmup,'-publish-interval',publish_interval,'-token-file',str(stack.directory/'bench.token'),'-out',str(case/'result.json')],capture=True,check=False)
                        after = metrics(stack)
                        (case/'process.txt').write_text(result.stdout + result.stderr)
                        (case/'resources.json').write_text(json.dumps({'before':before,'after':after,'samples':sampler.samples,'sample_errors':sampler.sample_errors},indent=2))
                        statements = stack.sql("SELECT coalesce(json_agg(t),'[]') FROM (SELECT calls,total_exec_time,mean_exec_time,rows,query FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC LIMIT 30) t;")
                        (case/'sql.json').write_text(statements)
                        if result.returncode:
                            raise RuntimeError('load failed: ' + str(case))
                        row = json.loads((case/'result.json').read_text())
                        validate_result(row,args.profile)
                        row.update(replicas=count,repeat=repeat)
                        all_results.append(row)
                        print(f"PASS replicas={count} {scenario} c={level} repeat={repeat}: {row['successful_rps']:.1f} RPS, p95={row['p95_ms']:.1f}ms",flush=True)
            (output/'results.json').write_text(json.dumps(all_results,indent=2))
            stack.logs(output)
        except Exception:
            stack.logs(output)
            raise
        finally:
            stack.down(delete=True)
            shutil.rmtree(stack.directory)
    (ROOT/'.artifacts'/('load-'+args.profile+'-latest.json')).write_text(json.dumps(groups,indent=2))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('profile',choices=list(DEFAULTS),default='smoke',nargs='?')
    parser.add_argument('--no-build',action='store_true')
    parser.add_argument('--duration',help='measurement time per case, e.g. 30s')
    parser.add_argument('--warmup',help='warmup time per case, e.g. 5s')
    parser.add_argument('--repeats',type=int)
    parser.add_argument('--concurrency',help='comma-separated client counts')
    parser.add_argument('--scenarios',help='comma-separated scenario names')
    parser.add_argument('--replicas',help='1, 3, or 1,3')
    parser.add_argument('--publish-interval',help='SSE mutation spacing (0s is unthrottled)')
    run(parser.parse_args())
