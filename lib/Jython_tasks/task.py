'''
Created on Sep 14, 2017

@author: riteshagarwal
'''
from com.couchbase.client.java import *;
from com.couchbase.client.java.transcoder import JsonTranscoder
from com.couchbase.client.java.document import *;
from com.couchbase.client.java.document.json import *;
from com.couchbase.client.java.query import *;
import threading
from threading import Thread
import string
import random
import sys
import time
from java.util.concurrent import Callable
from java.util.concurrent import Executors, TimeUnit
import json
from Jython_tasks.shutdown import shutdown_and_await_termination

class DocloaderTask(Callable):
    def __init__(self, bucket, num_items, start_from, k, v):
        self.bucket = bucket
        self.num_items = num_items
        self.start_from = start_from
        self.started = None
        self.completed = None
        self.loaded = 0
        self.thread_used = None
        self.exception = None
        self.key = k
        self.value = v

    def __str__(self):
        if self.exception:
            return "[%s] %s download error %s in %.2fs" % \
                (self.thread_used, self.num_items, self.exception,
                 self.completed - self.started, ) #, self.result)
        elif self.completed:
            print "Time: %s"%str(time.strftime("%H:%M:%S", time.gmtime(time.time())))
            return "[%s] %s items loaded in %.2fs" % \
                (self.thread_used, self.loaded,
                 self.completed - self.started, ) #, self.result)
        elif self.started:
            return "[%s] %s started at %s" % \
                (self.thread_used, self.num_items, self.started)
        else:
            return "[%s] %s not yet scheduled" % \
                (self.thread_used, self.num_items)

    def call(self):
        self.thread_used = threading.currentThread().getName()
        self.started = time.time()
        try:
            var = str(json.dumps(self.value))
            user = JsonTranscoder().stringToJsonObject(var);
#             user = JsonObject.fromJson(str(self.value))
            for i in xrange(self.num_items):
                doc = JsonDocument.create(self.key+str(i+self.start_from), user);
                response = self.bucket.upsert(doc);
                self.loaded += 1
        except Exception, ex:
            self.exception = ex
        self.completed = time.time()
        return self
    
class docloadertask_executor():
    
    def load(self, k, v, docs=10000, server="localhost", bucket="default"):
        cluster = CouchbaseCluster.create(server);
        cluster.authenticate("Administrator","password")
        bucket = cluster.openBucket(bucket);
        
        pool = Executors.newFixedThreadPool(5)
        docloaders=[]
        num_executors = 5
        total_num_executors = 5
        num_docs = docs/total_num_executors
        for i in xrange(total_num_executors):
            docloaders.append(DocloaderTask(bucket, num_docs, i*num_docs, k, v))
        futures = pool.invokeAll(docloaders)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        
        print "Executors completed!!"
        shutdown_and_await_termination(pool, 5)
        if bucket.close() and cluster.disconnect():
            pass
