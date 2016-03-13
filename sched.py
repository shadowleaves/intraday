# -*- coding: utf-8 -*-
"""
Created on Thu Jul 10 15:42:11 2014

@author: han.yan
"""
# from threading import Timer#, Thread, Event
# from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

# sched = sch.Scheduler()
# sched.print_jobs


class heartbeats():

    def __init__(self, seconds, hFun, start_datetime=None,
                 max_instances=3, **kwargs):
        self.t = seconds
        self.start_datetime = start_datetime
        self.max_instances = max_instances
        self.sched = BackgroundScheduler()
        if hFun is not None:
            self.add_job(hFun, **kwargs)

    def add_job(self, hFun, **kwargs):
        self.sched.add_job(hFun, 'interval', seconds=self.t,
                           start_date=self.start_datetime,
                           max_instances=self.max_instances,
                           kwargs=kwargs)

    def start(self):
        self.sched.start()

    def stop(self):
        self.sched.shutdown()
        print 'canceled'

    def show(self):
        self.sched.print_jobs()


class cronbeats():

    def __init__(self, hFun, second=None, minute=None,  max_instances=10,
                 **kwargs):
        self.second = second if minute is None else None
        self.minute = minute if second is None else None
        self.max_instances = max_instances
        self.sched = BackgroundScheduler()
        if hFun is not None:
            self.add_job(hFun, **kwargs)

    def add_job(self, hFun, **kwargs):
        self.sched.add_job(hFun, 'cron', second=self.second,
                           minute=self.minute,
                           max_instances=self.max_instances,
                           kwargs=kwargs)

    def start(self):
        self.sched.start()

    def stop(self):
        self.sched.shutdown()
        print 'canceled'

    def show(self):
        self.sched.print_jobs()

if __name__ == "__main__":
    def job_function(string):
        print string, datetime.now()
    beat = cronbeats(job_function, second='0-59', string='test')
    beat.start()
    while(1):
        pass
