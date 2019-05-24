'''
Created on Feb 13, 2019

@author: riteshagarwal
'''

import bulk_doc_operations.doc_ops as doc_op
from com.couchbase.client.java.env import DefaultCouchbaseEnvironment
import copy
from com.couchbase.client.java import *;
from com.couchbase.client.java.transcoder import JsonTranscoder
from com.couchbase.client.java.document import *;
from com.couchbase.client.java.document.json import *;
from com.couchbase.client.java.query import *;
import threading
import random
from java.util.concurrent import Callable
from java.util.concurrent import Executors, TimeUnit
import sys, time, traceback
from lib.membase.api.rest_client import RestConnection, RestHelper
from TestInput import TestInputSingleton
from pytests.basetestcase import BaseTestCase

class global_vars:
    
    message_id = 1
    start_message_id = 0
    end_message_id = 0
    
class GleambookMessages_Docloader(Callable):
    def __init__(self, msg_bucket, num_items, start_from,op_type="create",batch_size=1000):
        self.msg_bucket = msg_bucket
        self.num_items = num_items
        self.start_from = start_from
        self.started = None
        self.completed = None
        self.loaded = 0
        self.thread_used = None
        self.exception = None
        self.op_type = op_type
        self.year = range(2001,2018)
        self.month = range(1,12)
        self.day = range(1,28)
        self.batch_size = batch_size
        
    def generate_GleambookMessages(self, num=None,message_id=None):

        date = "%04d"%random.choice(self.year) + "-" + "%02d"%random.choice(self.month) + "-" + "%02d"%random.choice(self.day)
        time = "%02d"%random.choice(range(0,24)) + "-" + "%02d"%random.choice(range(0,60)) + "-" + "%02d"%random.choice(self.day)

        GleambookMessages = {"message_id": "%d"%message_id, "author_id": "%d"%num,
                             "send_time": date+"T"+time, 
                             }
        return GleambookMessages
    
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
            temp=0
            docs=[]
            for i in xrange(self.num_items):
                start_message_id = global_vars.message_id
                if self.op_type == "create":
                    for j in xrange(random.randint(1,10)):
                        var = str(json.dumps(self.generate_GleambookMessages(i+self.start_from , global_vars.message_id)))
                        user = JsonTranscoder().stringToJsonObject(var);
#                         print i+self.start_from,global_vars.message_id
                        doc = JsonDocument.create(str(global_vars.message_id), user);
                        docs.append(doc)
                        temp+=1
                        if temp == self.batch_size:
                            try:
                                doc_op().bulkSet(self.msg_bucket, docs)
                            except:
                                print "Sleeping for 20 secs"
                                time.sleep(20)
                                try:
                                    doc_op().bulkUpsert(self.msg_bucket, docs)
                                except:
                                    print "skipping %s documents upload"%len(docs)
                                    pass
                            temp = 0
                            docs=[]
                        global_vars.message_id += 1
                    end_message_id = global_vars.message_id
                elif self.op_type == "update":
                    var = str(json.dumps(self.generate_GleambookMessages(i+self.start_from , i+start_message_id)))
                    user = JsonTranscoder().stringToJsonObject(var);
                    doc = JsonDocument.create(str(i+start_message_id), user);
                    docs.append(doc)
                    if temp == self.batch_size:
                        try:
                            doc_op().bulkUpsert(self.msg_bucket, docs)
                        except:
                            print "Sleeping for 20 secs"
                            time.sleep(20)
                            try:
                                doc_op().bulkUpsert(self.msg_bucket, docs)
                            except:
                                print "skipping %s documents upload"%len(docs)
                                pass
                        temp = 0
                        docs=[]           
                elif self.op_type == "delete":
                    try:
                        response = self.msg_bucket.remove(str(i+start_message_id));
                    except:
                        pass      
                self.loaded += 1
        except Exception, ex:
            import traceback
            traceback.print_exc()
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            self.exception = ex
        self.completed = time.time()
        return self
    
class GleambookUser_Docloader(Callable):
    def __init__(self, bucket, num_items, start_from,op_type="create", batch_size=2000):
        self.bucket = bucket
        self.num_items = num_items
        self.start_from = start_from
        self.started = None
        self.completed = None
        self.loaded = 0
        self.thread_used = None
        self.exception = None
        self.op_type = op_type
        self.year = range(2001,2018)
        self.month = range(1,12)
        self.day = range(1,28)
        self.hr = range(0,24)
        self.min = range(0,60)
        self.sec = range(0,60)
        self.batch_size = batch_size
        
    def generate_GleambookUser(self, num=None):
        organization = ["Wipro","Infosys","TCS","Tech Mahindra","CTS","Microsoft"]
        date = "%04d"%random.choice(self.year) + "-" + "%02d"%random.choice(self.month) + "-" + "%02d"%random.choice(self.day)
        time = "%02d"%random.choice(self.hr) + "-" + "%02d"%random.choice(self.min) + "-" + "%02d"%random.choice(self.sec)
        employment = []
        start_date = "%04d"%random.choice(self.year) + "-" + "%02d"%random.choice(self.month) + "-" + "%02d"%random.choice(self.day)
        end_date = "%04d"%random.choice(self.year) + "-" + "%02d"%random.choice(self.month) + "-" + "%02d"%random.choice(self.day)
        
        for i in xrange(3):
            EmploymentType = {"organization":random.choice(organization),"start_date":start_date,"end_date":end_date}
            employment.append(EmploymentType)
            EmploymentType = {"organization":random.choice(organization),"start_date":start_date}
            employment.append(EmploymentType)

        GleambookUserType = {"id":num,"alias":"Peter"+"%05d"%num,"name":"Peter Thomas","user_since":date+"T"+time,
                            "employment":random.sample(employment,random.choice(range(6)))
                             }
        return GleambookUserType

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
            docs=[]
            temp=0
            for i in xrange(self.num_items):
                if self.op_type == "create":
                    var = str(json.dumps(self.generate_GleambookUser(i+self.start_from)))
                    user = JsonTranscoder().stringToJsonObject(var);
                    doc = JsonDocument.create(str(i+self.start_from), user);
                    docs.append(doc)
                    temp += 1
                    if temp == self.batch_size:
                        try:
                            doc_op().bulkSet(self.bucket, docs)
                        except:
                            print "Exception from Java SDK - create"
                            time.sleep(20)
                            try:
                                doc_op().bulkUpsert(self.bucket, docs)
                            except:
                                print "skipping %s documents upload"%len(docs)
                                pass
                        temp = 0
                        docs=[]
#                     response = self.bucket.insert(doc);
                elif self.op_type == "update":
                    var = str(json.dumps(self.generate_GleambookUser(i+self.start_from)))
                    user = JsonTranscoder().stringToJsonObject(var);
                    doc = JsonDocument.create(str(i+self.start_from), user);
                    docs.append(doc)
                    temp += 1
                    if temp == self.batch_size:
                        try:
                            doc_op().bulkUpsert(self.bucket, docs)
                        except:
                            print "Exception from Java SDK - create"
                            time.sleep(20)
                            try:
                                doc_op().bulkUpsert(self.bucket, docs)
                            except:
                                print "skipping %s documents upload"%len(docs)
                                pass
                        temp = 0
                        docs=[]
                    
                elif self.op_type == "delete":
                    try:
                        response = self.bucket.remove(str(i+self.start_from));
                    except:
                        print "Exception from Java SDK - remove"
                self.loaded += 1
        except Exception, ex:
            traceback.print_exc()
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            self.exception = ex
        self.completed = time.time()
        return self
    
def shutdown_and_await_termination(pool, timeout):
    pool.shutdown()
    try:
        if not pool.awaitTermination(timeout, TimeUnit.SECONDS):
            pool.shutdownNow()
            if (not pool.awaitTermination(timeout, TimeUnit.SECONDS)):
                print >> sys.stderr, "Pool did not terminate"
    except InterruptedException, ex:
        # (Re-)Cancel if current thread also interrupted
        pool.shutdownNow()
        # Preserve interrupt status
        Thread.currentThread().interrupt()
 
class ups(BaseTestCase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket":False})
        BaseTestCase.setUp(self)
    
    def validate_items_count(self):
        items_GleambookUsers = RestConnection(self.query_node).query_tool('select count(*) from GleambookUsers')['results'][0]['$1']
        items_GleambookMessages = RestConnection(self.query_node).query_tool('select count(*) from GleambookMessages')['results'][0]['$1']
        items_ChirpMessages = RestConnection(self.query_node).query_tool('select count(*) from ChirpMessages')['results'][0]['$1']
        self.log.info("Items in CB GleanBookUsers bucket: %s"%items_GleambookUsers)
        self.log.info("Items in CB GleambookMessages bucket: %s"%items_GleambookMessages)
        self.log.info("Items in CB ChirpMessages bucket: %s"%items_ChirpMessages)
        
    def test_ups_volume(self):
        nodes_in_cluster= [self.servers[0]]
        print "Start Time: %s"%str(time.strftime("%H:%M:%S", time.gmtime(time.time())))
        
        ########################################################################################################################
        self.log.info("Add a KV nodes - 2")
        self.query_node = self.servers[1]
        rest = RestConnection(self.servers[1])
        rest.set_data_path(data_path=self.servers[1].data_path,index_path=self.servers[1].index_path,cbas_path=self.servers[1].cbas_path)
        result = self.add_node(self.servers[1], rebalance=False)
        self.assertTrue(result, msg="Failed to add N1QL/Index node.")
        
        self.log.info("Add a KV nodes - 3")
        rest = RestConnection(self.servers[2])
        rest.set_data_path(data_path=self.kv_servers[1].data_path,index_path=self.kv_servers[1].index_path,cbas_path=self.kv_servers[1].cbas_path)
        result = self.add_node(self.kv_servers[1], services=["kv"], rebalance=False)
        self.assertTrue(result, msg="Failed to add KV node.")

        self.log.info("Add one more KV node")
        rest = RestConnection(self.servers[3])
        rest.set_data_path(data_path=self.kv_servers[3].data_path,index_path=self.kv_servers[3].index_path,cbas_path=self.kv_servers[3].cbas_path)
        result = self.add_node(self.kv_servers[3], services=["kv"], rebalance=False)
        self.assertTrue(result, msg="Failed to add KV node.")

        self.log.info("Add one more KV node")
        rest = RestConnection(self.servers[4])
        rest.set_data_path(data_path=self.kv_servers[4].data_path,index_path=self.kv_servers[4].index_path,cbas_path=self.kv_servers[4].cbas_path)
        result = self.add_node(self.kv_servers[4], services=["kv"], rebalance=False)
        self.assertTrue(result, msg="Failed to add KV node.")
                 
        nodes_in_cluster = nodes_in_cluster + [self.servers[1], self.servers[2], self.servers[3], self.servers[4]]
        ########################################################################################################################
        self.log.info("Step 2: Create Couchbase buckets.")
        self.create_required_buckets()
        
        ########################################################################################################################
        self.log.info("Step 3: Create 10M docs average of 1k docs for 8 couchbase buckets.")
        env = DefaultCouchbaseEnvironment.builder().mutationTokensEnabled(True).computationPoolSize(5).socketConnectTimeout(100000).connectTimeout(100000).maxRequestLifetime(TimeUnit.SECONDS.toMillis(300)).build();
        cluster = CouchbaseCluster.create(env, self.master.ip);
        cluster.authenticate("Administrator","password")
        bucket = cluster.openBucket("GleambookUsers");
        msg_bucket = cluster.openBucket("GleambookMessages")
         
        pool = Executors.newFixedThreadPool(5)
        items_start_from = 0
        total_num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 5
        num_items = total_num_items / num_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
           
        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items

        ########################################################################################################################
        self.log.info("Step 6: Verify the items count.")
        self.validate_items_count()
        
        ########################################################################################################################
        self.log.info("Step 8: Delete 1M docs. Update 1M docs.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
        
        executors.append(GleambookUser_Docloader(bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, deletes_from,"delete"))
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
        ########################################################################################################################
        self.log.info("Step 9: Connect cbas buckets.")
        self.connect_cbas_buckets()
        self.sleep(10,"Wait for the ingestion to complete")
         
        ########################################################################################################################
        self.log.info("Step 10: Verify the items count.")
        self.validate_items_count()
         
        ########################################################################################################################
        self.log.info("Step 12: When 11 is in progress do a KV Rebalance in of 1 nodes.")
        rest = RestConnection(self.servers[5])
        rest.set_data_path(data_path=self.servers[5].data_path,index_path=self.servers[5].index_path,cbas_path=self.servers[5].cbas_path)
        rebalance = self.cluster.async_rebalance(nodes_in_cluster, [self.servers[5]], [])
        nodes_in_cluster += [self.servers[2]]
        ########################################################################################################################
        self.log.info("Step 11: Create 10M docs.")
        pool = Executors.newFixedThreadPool(5)
        total_num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
        num_items = total_num_items / doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
 
        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items
                 
        ########################################################################################################################
        self.log.info("Step 13: Wait for rebalance to complete.")
        rebalance.get_result()
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.sleep(20)
         
        ########################################################################################################################
        self.log.info("Step 14: Verify the items count.")
        self.validate_items_count()
         
        ########################################################################################################################
        self.log.info("Step 15: Delete 1M docs. Update 1M docs.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
         
        executors.append(GleambookUser_Docloader(bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, deletes_from,"delete"))
         
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
        ########################################################################################################################
        self.log.info("Step 16: Verify Results that 1M docs gets deleted from analytics datasets.")
        self.validate_items_count()
         
        ########################################################################################################################
        self.log.info("Step 17: Disconnect CBAS buckets.")
        self.disconnect_cbas_buckets()
         
        ########################################################################################################################
        self.log.info("Step 18: Create 10M docs.")
        pool = Executors.newFixedThreadPool(5)
        total_num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
        num_items = total_num_items / doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
 
        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items
         
        ########################################################################################################################
        self.log.info("Step 20: Verify the docs count.")
        self.validate_items_count()
         
        ########################################################################################################################
        pool = Executors.newFixedThreadPool(5)
        executors=[]
        num_executors = 5
         
        self.log.info("Step 22: When 21 is in progress do a KV Rebalance out of 2 nodes.")
        rebalance = self.cluster.async_rebalance(nodes_in_cluster, [], self.servers[1:3])
        nodes_in_cluster = [node for node in nodes_in_cluster if node not in self.servers[1:3]]
        
        futures = pool.invokeAll(executors)
        self.log.info("Step 23: Wait for rebalance.")
        rebalance.get_result()
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.sleep(20)
         
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
         
        ########################################################################################################################
        self.log.info("Step 24: Create 10M docs.")
        pool = Executors.newFixedThreadPool(5)
        total_num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 6
        doc_executors = 4
        num_items = total_num_items / doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
         
        ##################################################### NEED TO BE UPDATED ##################################################################
        self.log.info("Step 25: When 24 is in progress do a KV Rebalance in of 2 nodes.")
        for node in self.servers[1:3]:
            rest = RestConnection(node)
            rest.set_data_path(data_path=node.data_path,index_path=node.index_path,cbas_path=node.cbas_path)
        rebalance = self.cluster.async_rebalance(nodes_in_cluster, self.servers[1:3],[])
        nodes_in_cluster = nodes_in_cluster + self.servers[1:3]
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
 
        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items
 
        self.log.info("Step 27: Wait for rebalance to complete.")
        rebalance.get_result()
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
         
        ########################################################################################################################
        self.log.info("Step 28: Verify the docs count.")
        self.validate_items_count()
         
        ########################################################################################################################
        self.log.info("Step 29: Delete 1M docs. Update 1M docs.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4

        executors.append(GleambookUser_Docloader(bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, deletes_from,"delete"))
        
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
        ########################################################################################################################
        self.log.info("Step 30: Verify the docs count.")
        self.validate_items_count()
 
        ########################################################################################################################
        self.log.info("Step 31: Create 10M docs.")
        pool = Executors.newFixedThreadPool(5)
        total_num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
        num_items = total_num_items / doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
 
        ###################################################### NEED TO BE UPDATED ##################################################################
        self.log.info("Step 32: When 31 is in progress do a KV Rebalance out of 2 nodes.")
        rebalance = self.cluster.async_rebalance(nodes_in_cluster, [], self.servers[1:3])
        nodes_in_cluster = [node for node in nodes_in_cluster if node not in self.servers[1:3]]
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
 
        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items
        ########################################################################################################################
        self.log.info("Step 33: Wait for rebalance to complete.")
        rebalance.get_result()
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.sleep(20)
 
        ########################################################################################################################
        self.log.info("Step 34: Verify the docs count.")
        self.validate_items_count()
         
        ########################################################################################################################
        self.log.info("Step 35: Delete 1M docs. Update 1M docs.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4

        executors.append(GleambookUser_Docloader(bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, deletes_from,"delete"))
        
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
         
        ########################################################################################################################
        self.log.info("Step 36: Verify the docs count.")
        self.validate_items_count()
         
        ########################################################################################################################
        self.log.info("Step 37: Create 10M docs.")
        pool = Executors.newFixedThreadPool(5)
        total_num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
        num_items = total_num_items / doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
         
        ###################################################### NEED TO BE UPDATED ##################################################################
        self.log.info("Step 38: When 37 is in progress do a CBAS SWAP Rebalance of 2 nodes.")
        for node in self.cbas_servers[-1:]:
            rest = RestConnection(node)
            rest.set_data_path(data_path=node.data_path,index_path=node.index_path,cbas_path=node.cbas_path)
        rebalance = self.cluster.async_rebalance(nodes_in_cluster,self.servers[6], [self.servers[5]],services=["kv"],check_vbucket_shuffling=False)
        nodes_in_cluster += self.servers[6]
        nodes_in_cluster.remove(self.servers[5])
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
         
        ########################################################################################################################
        self.log.info("Step 39: Wait for rebalance to complete.")
        rebalance.get_result()
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.sleep(20)
         
        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items
         
        ########################################################################################################################
        self.log.info("Step 40: Verify the docs count.")
        self.validate_items_count()
 
        ########################################################################################################################
        self.log.info("Step 41: Delete 1M docs. Update 1M docs.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
         
        executors.append(GleambookUser_Docloader(bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, deletes_from,"delete"))
         
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
         
        ########################################################################################################################
        self.log.info("Step 42: Verify the docs count.")
        self.validate_items_count() 
         
        ########################################################################################################################
        self.log.info("Step 43: Create 10M docs.")
        pool = Executors.newFixedThreadPool(5)
        total_num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
        num_items = total_num_items / doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
         
        ###################################################### NEED TO BE UPDATED ##################################################################
        self.log.info("Step 44: When 43 is in progress do a KV Rebalance IN.")
        rest = RestConnection(self.servers[5])
        rest.set_data_path(data_path=self.servers[5].data_path,index_path=self.servers[5].index_path,cbas_path=self.servers[5].cbas_path)
        rebalance = self.cluster.async_rebalance(nodes_in_cluster, [self.servers[5]], [],services=["kv"])
        nodes_in_cluster += [self.servers[5]]
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
         
        ########################################################################################################################
        self.log.info("Step 45: Wait for rebalance to complete.")
        rebalance.get_result()
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.sleep(20)
         
        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items        
         
        ########################################################################################################################
        self.log.info("Step 46: Verify the docs count.")
        self.validate_items_count() 
         
        ########################################################################################################################
        self.log.info("Step 47: Delete 1M docs. Update 1M docs.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
        
        executors.append(GleambookUser_Docloader(bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, deletes_from,"delete"))
         
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
 
        ########################################################################################################################
        self.log.info("Step 48: Verify the docs count.")
        self.validate_items_count() 
 
        ########################################################################################################################
        self.log.info("Step 49: Create 10M docs.")
        pool = Executors.newFixedThreadPool(5)
        total_num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
        num_items = total_num_items / doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
         
        ########################################################################################################################
        self.log.info("Step 50: When 49 is in progress do a KV+CBAS Rebalance OUT.")
        rest = RestConnection(self.servers[6])
        rest.set_data_path(data_path=self.servers[6].data_path,index_path=self.servers[6].index_path,cbas_path=self.kv_servers[6].cbas_path)
        rebalance = self.cluster.async_rebalance(nodes_in_cluster, [], [self.servers[6]])
        nodes_in_cluster.remove(self.servers[6])
        
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
         
        ########################################################################################################################
        self.log.info("Step 51: Wait for rebalance to complete.")
        rebalance.get_result()
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.sleep(20)
        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items  
 
        ########################################################################################################################
        self.log.info("Step 52: Verify the docs count.")
        self.validate_items_count() 
 
        ########################################################################################################################
        self.log.info("Step 53: Delete 1M docs. Update 1M docs.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
         
        executors.append(GleambookUser_Docloader(bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, deletes_from,"delete"))
         
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
 
        ########################################################################################################################
        self.log.info("Step 54: Verify the docs count.")
        self.validate_items_count() 
         
         
        ########################################################################################################################
        self.log.info("Step 55: Create 10M docs.")
        pool = Executors.newFixedThreadPool(5)
        total_num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
        num_items = total_num_items / doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
         
        ########################################################################################################################
        self.log.info("Step 56: When 55 is in progress do a KV+CBAS SWAP Rebalance .")
        rest = RestConnection(self.servers[7])
        rest.set_data_path(data_path=self.servers[7].data_path,index_path=self.servers[7].index_path,cbas_path=self.servers[7].cbas_path)
        rebalance = self.cluster.async_rebalance(nodes_in_cluster, [self.servers[7]], [self.servers[6]])
#         rebalance.get_result()
        nodes_in_cluster.remove(self.servers[6])
        nodes_in_cluster += [self.servers[7]]
         
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
         
        ########################################################################################################################
        self.log.info("Step 57: Wait for rebalance to complete.")
        rebalance.get_result()
        reached = RestHelper(self.rest).rebalance_reached(wait_step=240)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.sleep(20)
         
        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items  
 
        ########################################################################################################################
        self.log.info("Step 58: Verify the docs count.")
        self.validate_items_count() 
         
        ########################################################################################################################
        self.log.info("Step 59: Delete 1M docs. Update 1M docs.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
         
        executors.append(GleambookUser_Docloader(bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, deletes_from,"delete"))
        
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
 
        ########################################################################################################################
        self.log.info("Step 60: Verify the docs count.")
        self.validate_items_count() 
                 
 
        bucket.close()
        msg_bucket.close()
        cluster.disconnect()
        
        print "End Time: %s"%str(time.strftime("%H:%M:%S", time.gmtime(time.time())))

