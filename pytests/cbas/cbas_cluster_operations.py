# -*- coding: utf-8 -*-
import datetime

from cbas_base import *
from couchbase_helper.tuq_generators import JsonGenerator
from lib.memcached.helper.data_helper import MemcachedClientHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from bucket_utils.bucket_ready_functions import bucket_utils
from sdk_client import SDKClient


class CBASClusterOperations(CBASBaseTest):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket":False})
        self.rebalanceServers = None
        self.nodeType = "KV"        
        self.wait_for_rebalance=True
        super(CBASClusterOperations, self).setUp()
        
        self.create_default_bucket()
#         self.cbas_util.createConn("default")
        if 'nodeType' in self.input.test_params:
            self.nodeType = self.input.test_params['nodeType']

        self.rebalance_both = self.input.param("rebalance_cbas_and_kv", False)
        if not self.rebalance_both:
            if self.nodeType == "KV":
                self.rebalanceServers = self.kv_servers
                self.wait_for_rebalance=False
            elif self.nodeType == "CBAS":
                self.rebalanceServers = [self.cbas_node] + self.cbas_servers
        else:
            self.rebalanceServers = self.kv_servers + [self.cbas_node] + self.cbas_servers
            self.nodeType = "KV" + "-" +"CBAS"
            
        self.assertTrue(len(self.rebalanceServers)>1, "Not enough %s servers to run tests."%self.rebalanceServers)
        self.log.info("This test will be running in %s context."%self.nodeType)
        
    def setup_for_test(self, skip_data_loading=False):
        if not skip_data_loading:
            # Load Couchbase bucket first.
            self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0,
                                                   self.num_items)
        self.cbas_util.createConn(self.cb_bucket_name)
        # Create bucket on CBAS
        self.assertTrue(self.cbas_util.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip),"bucket creation failed on cbas")

        # Create dataset on the CBAS bucket
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                      cbas_dataset_name=self.cbas_dataset_name)

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        if not skip_data_loading:
            # Validate no. of items in CBAS dataset
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cbas_dataset_name,
                    self.num_items):
                self.fail(
                    "No. of items in CBAS dataset do not match that in the CB bucket")
    
    def test_rebalance_in(self):
        '''
        Description: This will test the rebalance in feature i.e. one node coming in to the cluster.
        Then Rebalance. Verify that is has no effect on the data ingested to cbas.
        
        Steps:
        1. Setup cbas. bucket, datasets/shadows, connect.
        2. Add a node and rebalance. Don't wait for rebalance completion.
        3. During rebalance, do mutations and execute queries on cbas.
        
        Author: Ritesh Agarwal/Mihir Kamdar
        Date Created: 18/07/2017
        '''
        query = "select count(*) from {0};".format(self.cbas_dataset_name)

        self.setup_for_test()
        self.add_node(node=self.rebalanceServers[1], rebalance=True, wait_for_rebalance_completion=self.wait_for_rebalance)
        self.log.info("Rebalance state:%s"%self.rest._rebalance_progress_status())
        
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 2)
        
        self.log.info("Rebalance state:%s"%self.rest._rebalance_progress_status())
        self.cbas_util._run_concurrent_queries(query,None,2000,batch_size=self.concurrent_batch_size)
        
        self.log.info("Rebalance state:%s"%self.rest._rebalance_progress_status())
        
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 2,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def test_rebalance_out(self):
        '''
        Description: This will test the rebalance out feature i.e. one node going out of cluster.
        Then Rebalance.
        
        Steps:
        1. Add a node, Rebalance.
        2. Setup cbas. bucket, datasets/shadows, connect.
        3. Remove a node and rebalance. Don't wait for rebalance completion.
        4. During rebalance, do mutations and execute queries on cbas.
        
        Author: Ritesh Agarwal/Mihir Kamdar
        Date Created: 18/07/2017
        '''
        self.add_node(node=self.rebalanceServers[1])
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.setup_for_test()
        otpnodes = []
        nodes = self.rest.node_statuses()
        for node in nodes:
            if node.ip == self.rebalanceServers[1].ip:
                otpnodes.append(node)
        self.remove_node(otpnodes, wait_for_rebalance=self.wait_for_rebalance)
        self.log.info("Rebalance state:%s"%self.rest._rebalance_progress_status())
        
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 2)

        self.cbas_util._run_concurrent_queries(query,"immediate",2000,batch_size=self.concurrent_batch_size)

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 2, 0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def test_swap_rebalance(self):
        '''
        Description: This will test the swap rebalance feature i.e. one node going out and one node coming in cluster.
        Then Rebalance. Verify that is has no effect on the data ingested to cbas.
        
        Steps:
        1. Setup cbas. bucket, datasets/shadows, connect.
        2. Add a node that is to be swapped against the leaving node. Do not rebalance.
        3. Remove a node and rebalance.
        4. During rebalance, do mutations and execute queries on cbas.
        
        Author: Ritesh Agarwal/Mihir Kamdar
        Date Created: 20/07/2017
        '''
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.setup_for_test()
        
        otpnodes=[]
        nodes = self.rest.node_statuses()
        if self.nodeType == "KV":
            service = ["kv"]
        else:
            service = ["cbas"]
        otpnodes.append(self.add_node(node=self.servers[1], services=service))
        self.add_node(node=self.servers[3], services=service,rebalance=False)
        self.remove_node(otpnodes, wait_for_rebalance=self.wait_for_rebalance)
        
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 2)

        self.cbas_util._run_concurrent_queries(query,"immediate",2000,batch_size=self.concurrent_batch_size)

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 2, 0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def test_failover(self):
        '''
        Description: This will test the node failover both graceful and hard failover based on
        graceful_failover param in testcase conf file.
        
        Steps:
        1. Add node to the cluster which will be failed over.
        2. Create docs, setup cbas.
        3. Mark the node for fail over.
        4. Do rebalance asynchronously. During rebalance perform mutations.
        5. Run some CBAS queries.
        6. Check for correct number of items in CBAS datasets.
        
        Author: Ritesh Agarwal/Mihir Kamdar
        Date Created: 20/07/2017
        '''
        
        #Add node which will be failed over later.
        self.add_node(node=self.rebalanceServers[1])
        query = "select count(*) from {0};".format(self.cbas_dataset_name)

        graceful_failover = self.input.param("graceful_failover", False)
        self.setup_for_test()
        failover_task = self._cb_cluster.async_failover(self.input.servers,
                                                        [self.rebalanceServers[1]],
                                                        graceful_failover)
        failover_task.get_result()
        
        self.rebalance()
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 3 / 2)

        self.cbas_util._run_concurrent_queries(query,"immediate",2000,batch_size=self.concurrent_batch_size)

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 3 / 2,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    '''
    -i b/resources/4-nodes-template.ini -t cbas.cbas_cluster_operations.CBASClusterOperations.test_rebalance_in_cb_cbas_together,cb_bucket_name=default,cbas_bucket_name=default_bucket,cbas_dataset_name=default_ds,items=10,nodeType=KV,rebalance_cbas_and_kv=True,wait_for_rebalace=False
    '''

    def test_rebalance_in_cb_cbas_together(self):

        self.log.info("Creates cbas buckets and dataset")
        dataset_count_query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.setup_for_test()

        self.log.info("Rebalance in KV node")
        wait_for_rebalace_complete = self.input.param("wait_for_rebalace", False)
        self.add_node(node=self.rebalanceServers[1], rebalance=False,
                      wait_for_rebalance_completion=wait_for_rebalace_complete)

        self.log.info("Rebalance in CBAS node")
        self.add_node(node=self.rebalanceServers[3], rebalance=True,
                      wait_for_rebalance_completion=wait_for_rebalace_complete)

        self.log.info(
            "Perform document create as rebalance is in progress : Rebalance state:%s" % self.rest._rebalance_progress_status())
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", self.num_items, self.num_items * 2)

        self.log.info(
            "Run queries as rebalance is in progress : Rebalance state:%s" % self.rest._rebalance_progress_status())
        self.cbas_util._run_concurrent_queries(dataset_count_query, None, 2000, batch_size=self.concurrent_batch_size)

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items * 2, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    '''
    -i b/resources/4-nodes-template.ini -t cbas.cbas_cluster_operations.CBASClusterOperations.test_rebalance_out_cb_cbas_together,cb_bucket_name=default,cbas_bucket_name=default_bucket,cbas_dataset_name=default_ds,items=10,nodeType=KV,rebalance_cbas_and_kv=True,wait_for_rebalace=False
    '''

    def test_rebalance_out_cb_cbas_together(self):

        self.log.info("Rebalance in KV node and  wait for rebalance to complete")
        self.add_node(node=self.rebalanceServers[1])

        self.log.info("Rebalance in CBAS node and  wait for rebalance to complete")
        self.add_node(node=self.rebalanceServers[3])

        self.log.info("Creates cbas buckets and dataset")
        dataset_count_query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.setup_for_test()

        self.log.info("Fetch and remove nodes to rebalance out")
        wait_for_rebalace_complete = self.input.param("wait_for_rebalace", False)
        otpnodes = []
        nodes = self.rest.node_statuses()
        for node in nodes:
            if node.ip == self.rebalanceServers[1].ip or node.ip == self.rebalanceServers[3].ip:
                otpnodes.append(node)

        for every_node in otpnodes:
            self.remove_node(otpnodes, wait_for_rebalance=wait_for_rebalace_complete)

        self.log.info(
            "Perform document create as rebalance is in progress : Rebalance state:%s" % self.rest._rebalance_progress_status())
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", self.num_items, self.num_items * 2)

        self.log.info(
            "Run queries as rebalance is in progress : Rebalance state:%s" % self.rest._rebalance_progress_status())
        self.cbas_util._run_concurrent_queries(dataset_count_query, "immediate", 2000,
                                               batch_size=self.concurrent_batch_size)

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items * 2, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    '''
    -i b/resources/4-nodes-template.ini -t cbas.cbas_cluster_operations.CBASClusterOperations.test_swap_rebalance_cb_cbas_together,cb_bucket_name=default,cbas_bucket_name=default_bucket,cbas_dataset_name=default_ds,items=10,rebalance_cbas_and_kv=True,wait_for_rebalance=True
    '''

    def test_swap_rebalance_cb_cbas_together(self):
            
        self.log.info("Creates cbas buckets and dataset")
        wait_for_rebalance = self.input.param("wait_for_rebalance", True)
        dataset_count_query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.setup_for_test()

        self.log.info("Add KV node and don't rebalance")
        self.add_node(node=self.rebalanceServers[1], rebalance=False)

        self.log.info("Add cbas node and don't rebalance")
        self.add_node(node=self.rebalanceServers[3], rebalance=False)
        
        otpnodes = []
        nodes = self.rest.node_statuses()
        for node in nodes:
            if node.ip == self.rebalanceServers[0].ip or node.ip == self.rebalanceServers[2].ip:
                otpnodes.append(node)
                
        self.log.info("Remove master node")
        self.remove_node(otpnode=otpnodes, wait_for_rebalance=wait_for_rebalance)
        
        self.log.info("Create instances pointing to new master nodes")
        c_utils = cbas_utils(self.rebalanceServers[1], self.rebalanceServers[3])
        c_utils.createConn(self.cb_bucket_name)

        self.log.info("Create reference to SDK client")
        client = SDKClient(scheme="couchbase", hosts=[self.rebalanceServers[1].ip], bucket=self.cb_bucket_name,
                           password=self.rebalanceServers[1].rest_password)

        self.log.info("Add more document to default bucket")
        documents = ['{"name":"value"}'] * (self.num_items//10)
        document_id_prefix = "custom-id-"
        client.insert_custom_json_documents(document_id_prefix, documents)
        
        self.log.info(
            "Run queries as rebalance is in progress : Rebalance state:%s" % self.rest._rebalance_progress_status())
        c_utils._run_concurrent_queries(dataset_count_query, "immediate", 2000,
                                               batch_size=self.concurrent_batch_size)

        if not c_utils.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items + (self.num_items//10) , 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    def test_rebalance_in_multiple_cbas_on_a_busy_system(self):

        self.log.info("Setup CBAS")
        self.setup_for_test(skip_data_loading=True)

        self.log.info("Run KV ops in async while rebalance is in progress")
        json_generator = JsonGenerator()
        generators = json_generator.generate_docs_simple(docs_per_day=self.num_items, start=0)
        tasks = self._async_load_all_buckets(self.master, generators, "create", 0)
        
        self.log.info("Run concurrent queries to simulate busy system")
        statement = "select sleep(count(*),5000) from {0} where mutated=0;".format(self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(statement, self.mode, self.num_concurrent_queries)

        self.log.info("Rebalance in CBAS nodes")
        self.add_node(node=self.rebalanceServers[1], services=["cbas"], rebalance=False, wait_for_rebalance_completion=False)
        self.add_node(node=self.rebalanceServers[3], services=["cbas"], rebalance=True, wait_for_rebalance_completion=True)

        self.log.info("Get KV ops result")
        for task in tasks:
            task.get_result()

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    def test_rebalance_out_multiple_cbas_on_a_busy_system(self):

        self.log.info("Rebalance in CBAS nodes")
        self.add_node(node=self.rebalanceServers[1], services=["cbas"])
        self.add_node(node=self.rebalanceServers[3], services=["cbas"])

        self.log.info("Setup CBAS")
        self.setup_for_test(skip_data_loading=True)

        self.log.info("Run KV ops in async while rebalance is in progress")
        json_generator = JsonGenerator()
        generators = json_generator.generate_docs_simple(docs_per_day=self.num_items, start=0)
        tasks = self._async_load_all_buckets(self.master, generators, "create", 0)

        self.log.info("Run concurrent queries to simulate busy system")
        statement = "select sleep(count(*),5000) from {0} where mutated=0;".format(self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(statement, self.mode, self.num_concurrent_queries)

        self.log.info("Fetch and remove nodes to rebalance out")
        self.rebalance_cc = self.input.param("rebalance_cc", False)
        out_nodes = []
        nodes = self.rest.node_statuses()
        for node in nodes:
            if self.rebalance_cc and (node.ip == self.cbas_node.ip or node.ip == self.rebalanceServers[1].ip):
                out_nodes.append(node)
            elif node.ip == self.rebalanceServers[1].ip or node.ip == self.rebalanceServers[3].ip:
                out_nodes.append(node)

        self.log.info("Rebalance out CBAS nodes %s %s" % (out_nodes[0].ip,out_nodes[0].ip))
        self.remove_node([out_nodes[0]], wait_for_rebalance=False)
        self.remove_node([out_nodes[1]])

        self.log.info("Get KV ops result")
        for task in tasks:
            task.get_result()

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    # TODO Refactor and add below test to conf once we have 6 node cluster so we can swap rebalance multiple cbas nodes
    def test_rebalance_swap_multiple_cbas_on_a_busy_system(self):

        self.log.info("Rebalance in CBAS nodes")
        self.add_node(node=self.rebalanceServers[1], services=["cbas"], rebalance=False)
        self.add_node(node=self.rebalanceServers[3], services=["cbas"], rebalance=False)

        self.log.info("Setup CBAS")
        self.setup_for_test(skip_data_loading=True)

        self.log.info("Run KV ops in async while rebalance is in progress")
        json_generator = JsonGenerator()
        generators = json_generator.generate_docs_simple(docs_per_day=self.num_items, start=0)
        tasks = self._async_load_all_buckets(self.master, generators, "create", 0)

        self.log.info("Run concurrent queries to simulate busy system")
        statement = "select sleep(count(*),5000) from {0} where mutated=0;".format(self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(statement, self.mode, self.num_concurrent_queries)

        self.log.info("Fetch and remove nodes to rebalance out")
        self.rebalance_cc = self.input.param("rebalance_cc", False)
        out_nodes = []
        nodes = self.rest.node_statuses()
        for node in nodes:
            if self.rebalance_cc and (node.ip == self.cbas_node.ip or node.ip == self.rebalanceServers[1].ip):
                out_nodes.append(node)
            elif node.ip == self.rebalanceServers[1].ip or node.ip == self.rebalanceServers[3].ip:
                out_nodes.append(node)

        self.log.info("Rebalance out CBAS nodes %s %s" % (out_nodes[0].ip, out_nodes[0].ip))
        self.remove_node([out_nodes[0]], wait_for_rebalance=False)
        self.remove_node([out_nodes[1]])

        self.log.info("Get KV ops result")
        for task in tasks:
            task.get_result()

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    '''
    test_fail_over_node_followed_by_rebalance_out_or_add_back,cb_bucket_name=default,graceful_failover=True,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,items=10000,nodeType=KV,rebalance_out=True,concurrent_batch_size=500
    test_fail_over_node_followed_by_rebalance_out_or_add_back,cb_bucket_name=default,graceful_failover=True,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,items=10000,nodeType=KV,rebalance_out=False,recovery_strategy=full,concurrent_batch_size=500
    test_fail_over_node_followed_by_rebalance_out_or_add_back,cb_bucket_name=default,graceful_failover=True,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,items=10000,nodeType=KV,rebalance_out=False,recovery_strategy=delta,concurrent_batch_size=500

    test_fail_over_node_followed_by_rebalance_out_or_add_back,cb_bucket_name=default,graceful_failover=False,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,items=10000,nodeType=KV,rebalance_out=True,concurrent_batch_size=500
    test_fail_over_node_followed_by_rebalance_out_or_add_back,cb_bucket_name=default,graceful_failover=False,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,items=10000,nodeType=KV,rebalance_out=False,recovery_strategy=full,concurrent_batch_size=500
    test_fail_over_node_followed_by_rebalance_out_or_add_back,cb_bucket_name=default,graceful_failover=False,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,items=10000,nodeType=KV,rebalance_out=False,recovery_strategy=delta,concurrent_batch_size=500

    test_fail_over_node_followed_by_rebalance_out_or_add_back,cb_bucket_name=default,graceful_failover=False,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,items=10000,nodeType=CBAS,rebalance_out=True,concurrent_batch_size=500
    test_fail_over_node_followed_by_rebalance_out_or_add_back,cb_bucket_name=default,graceful_failover=False,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,items=10000,nodeType=CBAS,rebalance_out=False,recovery_strategy=full,concurrent_batch_size=500
    '''

    def test_fail_over_node_followed_by_rebalance_out_or_add_back(self):
        """
        1. Start with an initial setup, having 1 KV and 1 CBAS
        2. Add a node that will be failed over - KV/CBAS
        3. Create CBAS buckets and dataset
        4. Fail over the KV node based in graceful_failover parameter specified
        5. Rebalance out/add back based on input param specified in conf file
        6. Perform doc operations
        7. run concurrent queries
        8. Verify document count on dataset post failover
        """
        self.log.info("Add an extra node to fail-over")
        self.add_node(node=self.rebalanceServers[1])

        self.log.info("Read the failure out type to be performed")
        graceful_failover = self.input.param("graceful_failover", True)

        self.log.info("Set up test - Create cbas buckets and data-sets")
        self.setup_for_test()

        self.log.info("Perform Async doc operations on KV")
        json_generator = JsonGenerator()
        generators = json_generator.generate_docs_simple(docs_per_day=self.num_items * 3 / 2, start=self.num_items)
        kv_task = self._async_load_all_buckets(self.master, generators, "create", 0)

        self.log.info("Run concurrent queries on CBAS")
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(query, "async", 10, batch_size=self.concurrent_batch_size)

        self.log.info("fail-over the node")
        fail_task = self._cb_cluster.async_failover(self.input.servers, [self.rebalanceServers[1]], graceful_failover)
        fail_task.get_result()

        self.log.info("Read input param to decide on add back or rebalance out")
        self.rebalance_out = self.input.param("rebalance_out", False)
        if self.rebalance_out:
            self.log.info("Rebalance out the fail-over node")
            self.rebalance()
        else:
            self.recovery_strategy = self.input.param("recovery_strategy", "full")
            self.log.info("Performing %s recovery" % self.recovery_strategy)
            success = False
            end_time = datetime.datetime.now() + datetime.timedelta(minutes=int(1))
            while datetime.datetime.now() < end_time or not success:
                try:
                    self.sleep(10, message="Wait for fail over complete")
                    self.rest.set_recovery_type('ns_1@' + self.rebalanceServers[1].ip, self.recovery_strategy)
                    success = True
                except Exception:
                    self.log.info("Fail over in progress. Re-try after 10 seconds.")
                    pass
            if not success:
                self.fail("Recovery %s failed." % self.recovery_strategy)
            self.rest.add_back_node('ns_1@' + self.rebalanceServers[1].ip)
            self.rebalance()

        self.log.info("Get KV ops result")
        for task in kv_task:
            task.get_result()

        self.log.info("Validate dataset count on CBAS")
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items * 3 / 2, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")
            
    def tearDown(self):
        super(CBASClusterOperations, self).setUp()       
