'''
Created on Apr 23, 2018
@author: riteshagarwal

To run:
/opt/jython/bin/jython -J-cp 'Couchbase-Java-Client-2.5.6/*:jsch-0.1.54.jar:doc_ops.jar' testrunner.py 
-i INI_FILE.ini num_query=100,num_items=10000 -t cbas.cursor_drop_test.volume.test_volume,num_query=100,num_items=10000000

'''
import bulk_doc_operations.doc_ops as doc_op
from com.couchbase.client.java.env import DefaultCouchbaseEnvironment
import copy
from com.couchbase.client.java import *
from com.couchbase.client.java.transcoder import JsonTranscoder
from com.couchbase.client.java.document import *
from com.couchbase.client.java.document.json import *
from com.couchbase.client.java.query import *
import threading
import random
from java.util.concurrent import Callable
from java.util.concurrent import Executors, TimeUnit
from com.couchbase.client.java.analytics import AnalyticsQuery, AnalyticsParams
import sys
import time
import traceback
import json
from lib.membase.api.rest_client import RestConnection, RestHelper
from TestInput import TestInputSingleton
from bucket_utils.bucket_ready_functions import bucket_utils
from basetestcase import BaseTestCase
from lib.remote.remote_util import RemoteMachineShellConnection
from BucketLib.BucketOperations import BucketHelper
from com.couchbase.client.core import CouchbaseException
from java.lang import System
from java.util.logging import Logger, Level, ConsoleHandler

class GleambookMessages_Docloader(Callable):
    def __init__(self, msg_bucket, num_items, start_from, op_type="create", batch_size=1000):
        self.msg_bucket = msg_bucket
        self.num_items = num_items
        self.start_from = start_from
        self.started = None
        self.completed = None
        self.loaded = 0
        self.thread_used = None
        self.exception = None
        self.op_type = op_type
        self.year = range(2001, 2018)
        self.month = range(1, 12)
        self.day = range(1, 28)
        self.batch_size = batch_size

    def generate_GleambookMessages(self, num=None, message_id=None):

        date = "%04d" % random.choice(self.year) + "-" + "%02d" % random.choice(
            self.month) + "-" + "%02d" % random.choice(self.day)
        time = "%02d" % random.choice(range(0, 24)) + "-" + "%02d" % random.choice(
            range(0, 60)) + "-" + "%02d" % random.choice(self.day)

        GleambookMessages = {"message_id": "%d" % message_id, "author_id": "%d" % num,
                             #                              "in_response_to": "%d"%random.choice(range(message_id)),
                             #                              "sender_location": str(round(random.uniform(0, 100), 4))+","+str(round(random.uniform(0, 100), 4)),
                             "send_time": date + "T" + time,
                             #                              "message": ''.join(random.choice(string.lowercase) for x in range(50))
                             }
        return GleambookMessages

    def __str__(self):
        if self.exception:
            return "[%s] %s download error %s in %.2fs" % \
                (self.thread_used, self.num_items, self.exception,
                 self.completed - self.started, )  # , self.result)
        elif self.completed:
            print "Time: %s" % str(time.strftime("%H:%M:%S", time.gmtime(time.time())))
            return "[%s] %s items loaded in %.2fs" % \
                (self.thread_used, self.loaded,
                 self.completed - self.started, )  # , self.result)
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
            temp = 0
            docs = []
            keys = []
            for i in xrange(self.num_items):
                if self.op_type == "create":
                    #                     for j in xrange(3):
                    var = str(json.dumps(self.generate_GleambookMessages(
                        i + self.start_from, self.start_from)))
                    user = JsonTranscoder().stringToJsonObject(var)
                    doc = JsonDocument.create(str(i + self.start_from), user)
                    docs.append(doc)
                    temp += 1
                    if temp == self.batch_size:
                        try:
                            doc_op().bulkSet(self.msg_bucket, docs)
                        except:
                            print "Sleeping for 20 secs"
                            time.sleep(20)
                            try:
                                doc_op().bulkUpsert(self.msg_bucket, docs)
                            except:
                                print "GleambookMessages_Docloader:skipping %s documents create" % len(docs)
                                pass
                        temp = 0
                        docs = []
#                         global_vars.message_id += 1
#                     end_message_id = global_vars.message_id
                elif self.op_type == "update":
                    var = str(json.dumps(self.generate_GleambookMessages(
                        i + self.start_from, i + self.start_from)))
                    user = JsonTranscoder().stringToJsonObject(var)
                    doc = JsonDocument.create(str(i + self.start_from), user)
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
                                print "GleambookMessages_Docloader:skipping %s documents upload" % len(docs)
                                pass
                        temp = 0
                        docs = []
                elif self.op_type == "delete":
                    try:
                        response = self.msg_bucket.remove(
                            str(i + self.start_from))
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
    def __init__(self, bucket, num_items, start_from, op_type="create", batch_size=2000):
        self.bucket = bucket
        self.num_items = num_items
        self.start_from = start_from
        self.started = None
        self.completed = None
        self.loaded = 0
        self.thread_used = None
        self.exception = None
        self.op_type = op_type
        self.year = range(2001, 2018)
        self.month = range(1, 12)
        self.day = range(1, 28)
        self.hr = range(0, 24)
        self.min = range(0, 60)
        self.sec = range(0, 60)
        self.batch_size = batch_size

    def generate_GleambookUser(self, num=None):
        organization = ["Wipro", "Infosys", "TCS",
                        "Tech Mahindra", "CTS", "Microsoft"]
        date = "%04d" % random.choice(self.year) + "-" + "%02d" % random.choice(
            self.month) + "-" + "%02d" % random.choice(self.day)
        time = "%02d" % random.choice(
            self.hr) + "-" + "%02d" % random.choice(self.min) + "-" + "%02d" % random.choice(self.sec)
        employment = []
        start_date = "%04d" % random.choice(
            self.year) + "-" + "%02d" % random.choice(self.month) + "-" + "%02d" % random.choice(self.day)
        end_date = "%04d" % random.choice(self.year) + "-" + "%02d" % random.choice(
            self.month) + "-" + "%02d" % random.choice(self.day)

        for i in xrange(3):
            #             start_date = "%04d"%random.choice(self.year) + "-" + "%02d"%random.choice(self.month) + "-" + "%02d"%random.choice(self.day)
            #             end_date = "%04d"%random.choice(self.year) + "-" + "%02d"%random.choice(self.month) + "-" + "%02d"%random.choice(self.day)

            EmploymentType = {"organization": random.choice(
                organization), "start_date": start_date, "end_date": end_date}
            employment.append(EmploymentType)

#             start_date = "%04d"%random.choice(self.year) + "-" + "%02d"%random.choice(self.month) + "-" + "%02d"%random.choice(self.day)

            EmploymentType = {"organization": random.choice(
                organization), "start_date": start_date}
            employment.append(EmploymentType)

        GleambookUserType = {"id": num, "alias": "Peter" + "%05d" % num, "name": "Peter Thomas", "user_since": date + "T" + time,
                             #                              "friend_ids":random.sample(range(1000),random.choice(range(10))),
                             "employment": random.sample(employment, random.choice(range(6)))
                             }
        return GleambookUserType

    def __str__(self):
        if self.exception:
            return "[%s] %s download error %s in %.2fs" % \
                (self.thread_used, self.num_items, self.exception,
                 self.completed - self.started, )  # , self.result)
        elif self.completed:
            print "Time: %s" % str(time.strftime("%H:%M:%S", time.gmtime(time.time())))
            return "[%s] %s items loaded in %.2fs" % \
                (self.thread_used, self.loaded,
                 self.completed - self.started, )  # , self.result)
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
            docs = []
            keys = []
            temp = 0
            for i in xrange(self.num_items):
                if self.op_type == "create":
                    var = str(json.dumps(
                        self.generate_GleambookUser(i + self.start_from)))
                    user = JsonTranscoder().stringToJsonObject(var)
                    doc = JsonDocument.create(str(i + self.start_from), user)
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
                                print "GleambookUser_Docloader: skipping %s documents create" % len(docs)
                                pass
                        temp = 0
                        docs = []
#                     response = self.bucket.insert(doc);
                elif self.op_type == "update":
                    var = str(json.dumps(
                        self.generate_GleambookUser(i + self.start_from)))
                    user = JsonTranscoder().stringToJsonObject(var)
                    doc = JsonDocument.create(str(i + self.start_from), user)
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
                                print "GleambookUser_Docloader: skipping %s documents upload" % len(docs)
                                pass
                        temp = 0
                        docs = []
                elif self.op_type == "delete":
                    try:
                        response = self.bucket.remove(str(i + self.start_from))
                    except:
                        print "Exception from Java SDK - remove"
                self.loaded += 1
        except Exception, ex:
            import traceback
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


class volume(BaseTestCase):
    def setUp(self, add_defualt_cbas_node=True):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket": False})
        BaseTestCase.setUp(self)
        self.rest = RestConnection(self.master)

    def tearDown(self):
        self.bucket.close()
        self.msg_bucket.close()
        self.c.disconnect()

    def create_required_buckets(self):
        self.log.info("Get the available memory quota")
        bucket_util = bucket_utils(self.master)
        self.info = bucket_util.rest.get_nodes_self()
        threadhold_memory = 0
        total_memory_in_mb = self.info.memoryFree / 1024 ** 2
        total_available_memory_in_mb = total_memory_in_mb
        active_service = self.info.services

        if "index" in active_service:
            total_available_memory_in_mb -= self.info.indexMemoryQuota
        if "fts" in active_service:
            total_available_memory_in_mb -= self.info.ftsMemoryQuota
        if "cbas" in active_service:
            total_available_memory_in_mb -= self.info.cbasMemoryQuota
        if "eventing" in active_service:
            total_available_memory_in_mb -= self.info.eventingMemoryQuota

        print(total_memory_in_mb)
        available_memory = total_available_memory_in_mb - threadhold_memory
        self.rest.set_service_memoryQuota(
            service='memoryQuota', memoryQuota=available_memory)
        self.rest.set_service_memoryQuota(
            service='cbasMemoryQuota', memoryQuota=available_memory - 1024)
        self.rest.set_service_memoryQuota(
            service='indexMemoryQuota', memoryQuota=available_memory - 1024)

        self.log.info("Create CB buckets")

        self.create_bucket(self.master, "GleambookUsers",
                           bucket_ram=available_memory / 3)
        self.create_bucket(self.master, "GleambookMessages",
                           bucket_ram=available_memory / 3)
        shell = RemoteMachineShellConnection(self.master)
        command = 'curl -i -u Administrator:password --data \'ns_bucket:update_bucket_props("ChirpMessages", [{extra_config_string, "cursor_dropping_upper_mark=70;cursor_dropping_lower_mark=50"}]).\' http://%s:8091/diag/eval' % self.master
        shell.execute_command(command)
        command = 'curl -i -u Administrator:password --data \'ns_bucket:update_bucket_props("GleambookMessages", [{extra_config_string, "cursor_dropping_upper_mark=70;cursor_dropping_lower_mark=50"}]).\' http://%s:8091/diag/eval' % self.master
        shell.execute_command(command)

        result = RestConnection(self.query_node).query_tool(
            "CREATE PRIMARY INDEX idx_GleambookUsers ON GleambookUsers;")
        self.sleep(10, "wait for index creation.")
        self.assertTrue(result['status'] == "success")

        result = RestConnection(self.query_node).query_tool(
            "CREATE PRIMARY INDEX idx_GleambookMessages ON GleambookMessages;")
        self.sleep(10, "wait for index creation.")
        self.assertTrue(result['status'] == "success")


    def validate_items_count(self):
        items_GleambookUsers = RestConnection(self.query_node).query_tool(
            'select count(*) from GleambookUsers')['results'][0]['$1']
        items_GleambookMessages = RestConnection(self.query_node).query_tool(
            'select count(*) from GleambookMessages')['results'][0]['$1']
        self.log.info("Items in CB GleanBookUsers bucket: %s" %
                      items_GleambookUsers)
        self.log.info("Items in CB GleambookMessages bucket: %s" %
                      items_GleambookMessages)

    def _validate_seq_no_stats(self, vbucket_stats):
        failure_dict = dict()
        corruption = False
        buckets = vbucket_stats.keys()
        # self.log.info("buckets : {0}".format(buckets))
        for bucket in buckets:
            failure_dict[bucket] = dict()
            nodes = vbucket_stats[bucket].keys()
            # self.log.info("{0} : {1}".format(bucket, nodes))
            for node in nodes:
                failure_dict[bucket][node] = dict()
                vb_list = vbucket_stats[bucket][node].keys()
                for vb in vb_list:
                    last_persisted_seqno = int(vbucket_stats[bucket][node][vb]["last_persisted_seqno"])
                    last_persisted_snap_start = int(vbucket_stats[bucket][node][vb]["last_persisted_snap_start"])
                    last_persisted_snap_end = int(vbucket_stats[bucket][node][vb]["last_persisted_snap_end"])
                    if last_persisted_snap_start > last_persisted_seqno or \
                            last_persisted_snap_start > last_persisted_snap_end:
                        failure_dict[bucket][node][vb] = {}
                        failure_dict[bucket][node][vb]["last_persisted_seqno"] = last_persisted_seqno
                        failure_dict[bucket][node][vb]["last_persisted_snap_start"] = last_persisted_snap_start
                        failure_dict[bucket][node][vb]["last_persisted_snap_end"] = last_persisted_snap_end
                        corruption = True
        return failure_dict, corruption
    
    def _record_vbuckets(self, master, servers):
        map = dict()
        for bucket in self.buckets:
            self.log.info("Record vbucket for the bucket {0}"
                          .format(bucket.name))
            map[bucket.name] = BucketHelper(master)\
                ._get_vbuckets(servers, bucket_name=bucket.name)
        #self.log.info("Map: {0}".format(map))
        return map
    
    def check_snap_start_corruption(self, servers_to_check=None):
        if servers_to_check is None:
            rest = RestConnection(self.master)
            nodes = rest.get_nodes()
            servers_to_check = []
            for node in nodes:
                for server in self.servers:
                    if node.ip == server.ip and str(node.port) == str(server.port):
                        servers_to_check.append(server)
        self.log.info("Servers to check bucket-seqno: {0}"
                      .format(servers_to_check))
        self._record_vbuckets(self.master, servers_to_check)
        vbucket_stats = self.get_vbucket_seqnos(
            servers_to_check, self.buckets, skip_consistency=True)
        failure_dict, corruption = self._validate_seq_no_stats(vbucket_stats)
        if corruption:
            self.fail("snap_start and snap_end corruption found !!! . {0}"
                      .format(failure_dict))

    def load_data(self):
        executors = []
        num_executors = 5
        doc_executors = 5
        pool = Executors.newFixedThreadPool(5)
        
        self.num_items = self.total_num_items/doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(
                self.bucket, self.num_items, self.items_start_from + i * self.num_items, batch_size=2000))
            executors.append(GleambookMessages_Docloader(
                self.msg_bucket, self.num_items, self.items_start_from + i * self.num_items, batch_size=2000))
        futures = pool.invokeAll(executors)

        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
        
        self.updates_from = self.items_start_from
        self.deletes_from = self.items_start_from + self.total_num_items / 10
        self.items_start_from += self.total_num_items

    def update_data(self):
        pool = Executors.newFixedThreadPool(5)
        executors = []
        num_executors = 5
        doc_executors = 4

        executors.append(GleambookUser_Docloader(
            self.bucket, 2 * self.num_items / 10, self.updates_from, "update"))
#         executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        executors.append(GleambookMessages_Docloader(
            self.msg_bucket, 2 * self.num_items / 10, self.updates_from, "update"))
#         executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, deletes_from,"delete"))
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)


    def test_volume(self):
        nodes_in_cluster = [self.servers[0]]
        print "Start Time: %s" % str(time.strftime("%H:%M:%S", time.gmtime(time.time())))

        #######################################################################
        self.log.info("Step 1: Add a N1QL/Index nodes")
        self.query_node = self.servers[1]
        rest = RestConnection(self.query_node)
        rest.set_data_path(data_path=self.query_node.data_path,
                           index_path=self.query_node.index_path, cbas_path=self.query_node.cbas_path)
        result = self.add_node(self.query_node, rebalance=False)
        self.assertTrue(result, msg="Failed to add N1QL/Index node.")

        self.log.info("Step 2: Add a KV nodes")
        result = self.add_node(self.servers[2], services=["kv"], rebalance=True)
        self.assertTrue(result, msg="Failed to add KV node.")

        nodes_in_cluster = nodes_in_cluster + [self.servers[1], self.servers[2]]
        
        #######################################################################
        
        self.log.info("Step 3: Create Couchbase buckets.")
        self.create_required_buckets()

        #######################################################################

        env = DefaultCouchbaseEnvironment.builder().mutationTokensEnabled(True).computationPoolSize(5).socketConnectTimeout(
            10000000).connectTimeout(10000000).maxRequestLifetime(TimeUnit.SECONDS.toMillis(1200)).build()

        try:
            System.setProperty("com.couchbase.forceIPv4", "false");
            logger = Logger.getLogger("com.couchbase.client");
            logger.setLevel(Level.SEVERE);
            for h in logger.getParent().getHandlers():
                if isinstance(h, ConsoleHandler) :
                    h.setLevel(Level.SEVERE);

            cluster = CouchbaseCluster.create(env, self.master.ip)
            cluster.authenticate("Administrator", "password")
            self.bucket = cluster.openBucket("GleambookUsers")
            self.msg_bucket = cluster.openBucket("GleambookMessages")            
        except CouchbaseException:
            print "cannot login from user: %s/%s"%(self.username, self.password)
            raise
        
        self.c = cluster
        self.items_start_from = 0
        self.total_num_items = self.input.param("num_items", 5000)
        self.load_data()

        self.sleep(20, "Sleeping after 4th step.")
        
        self.validate_items_count()
        
        self.log.info("Step 4: Add node")
        result = self.add_node(self.servers[3], rebalance=False)
        self.assertTrue(result, msg="Failed to add node.")
        self.log.info("Step 5: Loading %s items"%self.total_num_items)
        self.load_data()
        
        self.log.info("Step 6: Rebalance Cluster")
        rebalance = self.rebalance()
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        nodes_in_cluster = nodes_in_cluster + [self.servers[3]]
        
        self.log.info("Step 7: Start Verification")
        self.validate_items_count()
        self.check_snap_start_corruption()
        
        #######################################################################
        self.sleep(20)
        self.log.info("Step 8: Delete/Update docs.")
        self.update_data()
        
        self.log.info("Step 9: Verifying Data")
        self.validate_items_count()
        self.check_snap_start_corruption()

        #######################################################################
        self.log.info("Step 10: Removing node and Rebalance cluster")
        rebalance = self.cluster.async_rebalance(
            nodes_in_cluster, [], [self.servers[3]])
        nodes_in_cluster.remove(self.servers[3])
        
        self.log.info("Step 11: Loading %s items"%self.total_num_items)
        self.load_data()
        
        rebalance.get_result()
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        
        self.log.info("Step 12: Verifying Data")
        self.validate_items_count()
        self.check_snap_start_corruption()
        
        #######################################################################
        self.sleep(20)
        self.log.info("Step 13: Delete/Update docs.")
        self.update_data()
        
        self.log.info("Step 14: Verifying Data")
        self.validate_items_count()
        self.check_snap_start_corruption()
        
        #######################################################################
        self.sleep(20)
        self.log.info("Step 15: Add node")
        result = self.add_node(self.servers[3], rebalance=False)
        nodes_in_cluster = nodes_in_cluster + [self.servers[3]]
        
        self.log.info("Step 16: Loading %s items"%self.total_num_items)
        self.load_data()        

        self.log.info("Step 17: Rebalancing Cluster")
        rebalance = self.cluster.async_rebalance(nodes_in_cluster, [], [self.servers[2]])
        
        rebalance.get_result()
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        
        nodes_in_cluster.remove(self.servers[2])
        
        self.log.info("Step 18: Verifying Data")
        self.validate_items_count()
        self.check_snap_start_corruption()
        
        #######################################################################
        self.sleep(20)
        self.log.info("Step 19: Delete/Update docs.")
        self.update_data()
        
        self.log.info("Step 20: Verifying Data")
        self.validate_items_count()
        self.check_snap_start_corruption()
        
        #######################################################################
        self.sleep(20)
        self.log.info("Step 21: Add node")
        result = self.add_node(self.servers[2], rebalance=False)
        
        self.log.info("Step 22: Loading %s items"%self.total_num_items)
        self.load_data()
        
        self.log.info("Step 23: Rebalancing Cluster")
        rebalance = self.rebalance()
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        nodes_in_cluster = nodes_in_cluster + [self.servers[2]]
        
        self.log.info("Step 24: Verifying Data")
        self.validate_items_count()
        self.check_snap_start_corruption()
        
        #######################################################################
        self.sleep(20)
        self.log.info("Step 25: Delete/Update docs.")
        self.update_data()
        
        self.log.info("Step 26: Verifying Data")
        self.validate_items_count()
        self.check_snap_start_corruption()
        
        #######################################################################
        self.sleep(20)
        
        self.log.info("Step 27: Add node")
        result = self.add_node(self.servers[4], rebalance=False)
        
        self.log.info("Step 28: Loading %s items"%self.total_num_items)
        self.load_data()
        
        self.log.info("Step 29: Rebalancing Cluster")
        rebalance = self.rebalance()
        nodes_in_cluster = nodes_in_cluster + [self.servers[4]]
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")

        self.log.info("Step 30: Verifying Data")
        self.validate_items_count()
        self.check_snap_start_corruption()
        
        #######################################################################
        self.sleep(20)
        self.log.info("Step 31: Delete/Update docs.")
        self.update_data()
        
        self.log.info("Step 32: Verifying Data")
        self.validate_items_count()
        self.check_snap_start_corruption()
        
        #######################################################################
        self.sleep(20)
        self.log.info("Step 33: Removing node, Rebalancing Cluster")
        rebalance = self.cluster.async_rebalance(
            nodes_in_cluster, [], [self.servers[3]])
        nodes_in_cluster.remove(self.servers[3])
        
        self.log.info("Step 34: Loading %s items"%self.total_num_items)
        self.load_data()
        
        rebalance.get_result()
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")

        self.log.info("Step 35: Verifying Data")
        self.validate_items_count()
        self.check_snap_start_corruption()
        
        #######################################################################
        self.sleep(20)
        
        self.log.info("Step 36: Adding 4 nodes")
        otp0 = self.add_node(self.servers[4], rebalance=False)
        otp1 = self.add_node(self.servers[5], rebalance=False)
        otp2 = self.add_node(self.servers[6], rebalance=False)
        otp3 = self.add_node(self.servers[7], rebalance=False)

        self.log.info("Step 37: Loading %s items"%self.total_num_items)
        self.load_data()
        
        self.log.info("Step 38: Rebalancing Cluster")
        rebalance = self.rebalance()
        nodes_in_cluster = nodes_in_cluster + [self.servers[5],self.servers[6],self.servers[7]]
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")

        self.log.info("Step 39: Verifying Data")
        self.validate_items_count()
        self.check_snap_start_corruption()
        
        #######################################################################
        self.log.info("Step 40: Graceful failover node")
        self.rest.fail_over(otp3.id, graceful=True)
        self.log.info("Step 41: Loading %s items"%self.total_num_items)
        self.load_data()
        self.sleep(10)
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        
        self.log.info("Step 42: Rebalancing Cluster")
        rebalance = self.rebalance()
        nodes_in_cluster.remove(self.servers[7])
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        
        #######################################################################
        self.log.info("Step 43: Adding node and rebalancing")
        otp3 = self.add_node(self.servers[7], rebalance=True)
        nodes_in_cluster = nodes_in_cluster + [self.servers[7]]
        
        #######################################################################
        
        self.log.info("Step 44: Graceful failover node")
        self.rest.fail_over(otp3.id, graceful=True)
        self.log.info("Step 41: Loading %s items"%self.total_num_items)
        self.load_data()
        self.sleep(10)
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        
        self.log.info("Step 45: Delta recover node")
        self.rest.set_recovery_type(otp3.id, "delta")
        
        self.log.info("Step 46: Add node back to cluster")
        self.rest.add_back_node(otp3.id)
        
        rebalance = self.rebalance()
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        
        
        self.log.info("Step 47: Graceful failover node")
        self.rest.fail_over(otp2.id, graceful=True)
        self.log.info("Step 48: Loading %s items"%self.total_num_items)
        self.load_data()
        self.sleep(10)
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        
        self.log.info("Step 49: Delta recover node")
        self.rest.set_recovery_type(otp2.id, "full")
        
        self.log.info("Step 50: Add node back to cluster")
        self.rest.add_back_node(otp2.id)
        
        rebalance = self.rebalance()
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        
        self.bucket.close()
        self.msg_bucket.close()
        cluster.disconnect()
