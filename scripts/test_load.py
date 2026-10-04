import argparse
import pathlib
import sys
import unittest

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import load

class LoadTests(unittest.TestCase):
    def test_profiles_and_overrides(self):
        args=argparse.Namespace(profile='full',scenarios=None,concurrency=None,repeats=None,duration=None,warmup=None,replicas=None,publish_interval=None)
        scenarios,concurrency,repeats,duration,warmup,replicas,estimate=load.settings(args)
        self.assertEqual((duration,warmup,repeats,replicas),('30s','5s',2,[1,3]))
        self.assertIn('subscriptions',scenarios)
        self.assertGreater(estimate,15*60)
        args.scenarios='subscriptions,large';args.concurrency='64';args.duration='60s';args.warmup='2s';args.repeats=1;args.replicas='3'
        self.assertEqual(load.settings(args)[:6],(['subscriptions','large'],[64],1,'60s','2s',[3]))
    def test_subscription_accounting(self):
        row={'scenario':'subscriptions','success':2,'unexpected_errors':{},'expected_errors':{},'subscribers':2,'concurrency':2,'expected_events':4,'received_events':4,'matched_events':4,'missing_events':0,'duplicate_events':0,'unexpected_events':0,'invalid_events':0,'disconnects':0,'disconnect_reasons':{}}
        load.validate_result(row,'full')
        for changes in ({'missing_events':1,'matched_events':3,'duplicate_events':1}, {'disconnects':1,'disconnect_reasons':{'stream_eof':1}}, {'subscribers':1}):
            bad=dict(row,**changes)
            with self.assertRaises(ValueError):load.validate_result(bad,'full')
    def test_protected_separate(self):
        row={'scenario':'read','success':1,'unexpected_errors':{},'expected_errors':{'http_429':5}}
        load.validate_result(row,'protected')
        with self.assertRaises(ValueError):load.validate_result(row,'full')

if __name__=='__main__':unittest.main()
