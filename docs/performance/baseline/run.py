import subprocess,json,time,urllib.request,pathlib,threading
root=pathlib.Path('/tmp/ozon-upgrade-baseline')
def run(args,**kw):return subprocess.run(args,check=True,**kw)
end=time.monotonic()+30
while True:
 try:
  with urllib.request.urlopen('http://127.0.0.1:18090/healthz',timeout=1) as r:
   if r.status==204:break
 except Exception:pass
 if time.monotonic()>end:raise RuntimeError('readiness timeout')
 time.sleep(.15)
run(['docker','stop','ozon-upgrade-baseline-app'],stdout=subprocess.DEVNULL)
run(['docker','exec','ozon-upgrade-baseline-db','psql','-U','ozon','-d','ozon_test','-c','CREATE EXTENSION IF NOT EXISTS pg_stat_statements'],stdout=subprocess.DEVNULL)
for scenario in ['read','mixed','uniform','hot','branches']:
 for concurrency in [1,8,32]:
  with (root/'fixture.sql').open() as sql:run(['docker','exec','-i','ozon-upgrade-baseline-db','psql','-v','ON_ERROR_STOP=1','-U','ozon','-d','ozon_test'],stdin=sql,stdout=subprocess.DEVNULL)
  run(['docker','start','ozon-upgrade-baseline-app'],stdout=subprocess.DEVNULL)
  end=time.monotonic()+15
  while True:
   try:
    with urllib.request.urlopen('http://127.0.0.1:18090/healthz',timeout=1) as r:
     if r.status==204:break
   except Exception:pass
   if time.monotonic()>end:raise RuntimeError('readiness timeout')
   time.sleep(.1)
  samples=[];stop=threading.Event()
  def sample():
   while not stop.is_set():
    p=subprocess.run(['docker','stats','--no-stream','--format','{{json .}}','ozon-upgrade-baseline-app','ozon-upgrade-baseline-db'],capture_output=True,text=True)
    samples.extend(json.loads(x) for x in p.stdout.splitlines());stop.wait(.1)
  thread=threading.Thread(target=sample);thread.start()
  file=root/f'{scenario}-{concurrency}.json'
  run([str(root/'bench'),'-url','http://127.0.0.1:18090/query','-scenario',scenario,'-concurrency',str(concurrency),'-duration','4s','-warmup','1s','-out',str(file)],stdout=subprocess.DEVNULL)
  stop.set();thread.join();(root/f'{scenario}-{concurrency}-resources.json').write_text(json.dumps(samples,indent=2))
  data=json.loads(file.read_text());print(scenario,concurrency,round(data['successful_rps'],1),'rps',round(data['p95_ms'],1),'p95',data['errors'],flush=True)
  run(['docker','stop','ozon-upgrade-baseline-app'],stdout=subprocess.DEVNULL)
