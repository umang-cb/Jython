'''
Created on Jan 31, 2018

@author: riteshagarwal
'''

from cbas_base import CBASBaseTest, TestInputSingleton
from lib.memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
import time
from node_utils.node_ready_functions import NodeHelper
from cbas.cbas_utils import cbas_utils
import ast

class MetadataReplication(CBASBaseTest):

    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket":False})
        super(MetadataReplication, self).setUp()
        self.nc_otpNodes = []
        if "add_all_cbas_nodes" in self.input.test_params and self.input.test_params["add_all_cbas_nodes"] and len(self.cbas_servers) > 0:
            self.nc_otpNodes = self.add_all_nodes_then_rebalance(self.cbas_servers)
        elif self.input.param("nc_nodes_to_add",0):
            self.nc_otpNodes = self.add_all_nodes_then_rebalance(self.cbas_servers[:self.input.param("nc_nodes_to_add")])
        self.otpNodes += self.nc_otpNodes
            
        self.create_default_bucket()
        self.shell = RemoteMachineShellConnection(self.master)
        
        #test for number of partitions:
        self.partitions_dict = self.cbas_util.get_num_partitions(self.shell)
        
#         if self.master.cbas_path:
#             for key in self.partitions_dict.keys():
#                 self.assertTrue(self.partitions_dict[key] == len(ast.literal_eval(self.master.cbas_path)), "Number of partitions created are incorrect on cbas nodes.")
        
    def setup_for_test(self, skip_data_loading=False):
        
        if not skip_data_loading:
            # Load Couchbase bucket first.
            self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0,
                                                   self.num_items, batch_size=10000)

        # Create bucket on CBAS
        self.cbas_util.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip)

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
                
    def ingestion_in_progress(self):
        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)
        
        self.perform_doc_ops_in_all_cb_buckets(self.num_items*2, "create", self.num_items,
                                                   self.num_items*3, batch_size=10000)
        
        
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)
    
    def test_rebalance(self):
        self.setup_for_test(skip_data_loading=True)
        self.rebalance_type = self.input.param('rebalance_type','out')
        self.rebalance_node = self.input.param('rebalance_node','CC')
        self.how_many = self.input.param('how_many',1)
        self.restart_rebalance = self.input.param('restart_rebalance',False)
        self.replica_change = self.input.param('replica_change',0)
        
        query = "select sleep(count(*),50000) from {0};".format(self.cbas_dataset_name)
        handles = self.cbas_util._run_concurrent_queries(query,"async",10)
        self.ingestion_in_progress()
        
        if self.rebalance_node == "CC":
            node_in_test = [self.cbas_node]
            otpNodes = [self.otpNodes[0]]
            self.cbas_util = cbas_utils(self.master, self.cbas_servers[0])
            self.cbas_node = self.cbas_servers[0]
        elif self.rebalance_node == "NC":
            node_in_test = self.cbas_servers[:self.how_many]
            otpNodes = self.nc_otpNodes[:self.how_many]
        else:
            node_in_test = [self.cbas_node] + self.cbas_servers[:self.how_many]
            otpNodes = self.otpNodes[:self.how_many+1]
            self.cbas_util = cbas_utils(self.master, self.cbas_servers[self.how_many+1])
            
        replicas_before_rebalance=len(self.cbas_util.get_replicas_info(self.shell))
        
        if self.rebalance_type == 'in':
            if self.restart_rebalance:
                self.cluster_util.add_all_nodes_then_rebalance(self.cbas_servers[self.input.param("nc_nodes_to_add"):self.how_many+self.input.param("nc_nodes_to_add")],wait_for_completion=False)
                if self.rest._rebalance_progress_status() == "running":
                    self.assertTrue(self.rest.stop_rebalance(), "Failed while stopping rebalance.")
                else:
                    self.fail("Rebalance completed before the test could have stopped rebalance.")
                
                self.rebalance(wait_for_completion=False)
            else:
                self.cluster_util.add_all_nodes_then_rebalance(self.cbas_servers[self.input.param("nc_nodes_to_add"):self.how_many+self.input.param("nc_nodes_to_add")],wait_for_completion=False)
            replicas_before_rebalance += self.replica_change
        else:
            if self.restart_rebalance:
                self.cluster_util.remove_node(otpNodes,wait_for_rebalance=False)
                
                if self.rest._rebalance_progress_status() == "running":
                    self.assertTrue(self.rest.stop_rebalance(), "Failed while stopping rebalance.")
                else:
                    self.fail("Rebalance completed before the test could have stopped rebalance.")
                
                self.rebalance(wait_for_completion=False)
            else:
                self.cluster_util.remove_node(otpNodes,wait_for_rebalance=False)
            replicas_before_rebalance -= self.replica_change
        self.sleep(5)
        while self.rest._rebalance_progress_status() == "running":
            replicas = self.cbas_util.get_replicas_info(self.shell)
            if replicas:
                for replica in replicas:
                    self.log.info("replica state during rebalance: %s"%replica['status'])
        self.sleep(5)
        replicas = self.cbas_util.get_replicas_info(self.shell)
        replicas_after_rebalance=len(replicas)
        self.assertEqual(replicas_after_rebalance, replicas_before_rebalance, "%s,%s"%(replicas_after_rebalance,replicas_before_rebalance))
        
        for replica in replicas:
            self.log.info("replica state during rebalance: %s"%replica['status'])
            self.assertEqual(replica['status'], "IN_SYNC","Replica state is incorrect: %s"%replica['status'])
                                
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        self.log.info("Items before service restart: %s"%items_in_cbas_bucket)
                
        items_in_cbas_bucket = 0
        start_time=time.time()
        while (items_in_cbas_bucket == 0 or items_in_cbas_bucket == -1) and time.time()<start_time+60:
            try:
                items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            except:
                pass
            
        self.log.info("After rebalance operation docs in CBAS bucket : %s"%items_in_cbas_bucket)
        if items_in_cbas_bucket < self.num_items*2 and items_in_cbas_bucket>self.num_items:
            self.log.info("Data Ingestion Interrupted successfully")
        elif items_in_cbas_bucket < self.num_items:
            self.log.info("Data Ingestion did interrupted and restarting from 0.")
        else:
            self.log.info("Data Ingestion did not interrupted but complete before rebalance operation.")
            
        run_count = 0
        fail_count = 0
        success_count = 0
        aborted_count = 0
        shell=RemoteMachineShellConnection(node_in_test[0])
        for handle in handles:
            status, hand = self.cbas_util.retrieve_request_status_using_handle(node_in_test, handle, shell)
            if status == "running":
                run_count += 1
                self.log.info("query with handle %s is running."%handle)
            elif status == "failed":
                fail_count += 1
                self.log.info("query with handle %s is failed."%handle)
            elif status == "success":
                success_count += 1
                self.log.info("query with handle %s is successful."%handle)
            else:
                aborted_count +=1
                self.log.info("Queued job is deleted: %s"%status)
                
        self.log.info("After service restart %s queued jobs are Running."%run_count)
        self.log.info("After service restart %s queued jobs are Failed."%fail_count)
        self.log.info("After service restart %s queued jobs are Successful."%success_count)
        self.log.info("After service restart %s queued jobs are Aborted."%aborted_count)
        
        if self.rebalance_node == "NC":
            self.assertTrue(fail_count+aborted_count==0, "Some queries failed/aborted")
        
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(query,"immediate",100)
        
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,self.num_items*2):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    def test_cancel_CC_rebalance(self):
        pass
    
    def test_chain_rebalance_out_cc(self):
        self.setup_for_test(skip_data_loading=True)
        self.ingestion_in_progress()
        
        total_cbas_nodes = len(self.otpNodes)
        while total_cbas_nodes > 1:
            cc_ip = self.cbas_util.retrieve_cc_ip(shell=self.shell)
            for otpnode in self.otpNodes:
                if otpnode.ip == cc_ip:
                    self.cluster_util.remove_node([otpnode],wait_for_rebalance=True)
                    for server in self.cbas_servers:
                        if cc_ip != server.ip:
                            self.cbas_util = cbas_utils(self.master, server)
                            self.cbas_node = server
                            break
                    items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
                    self.log.info("Items before service restart: %s"%items_in_cbas_bucket)
                            
                    items_in_cbas_bucket = 0
                    start_time=time.time()
                    while (items_in_cbas_bucket == 0 or items_in_cbas_bucket == -1) and time.time()<start_time+60:
                        try:
                            items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
                        except:
                            pass
                        
                    self.log.info("After rebalance operation docs in CBAS bucket : %s"%items_in_cbas_bucket)
                    if items_in_cbas_bucket < self.num_items*2 and items_in_cbas_bucket>self.num_items:
                        self.log.info("Data Ingestion Interrupted successfully")
                    elif items_in_cbas_bucket < self.num_items:
                        self.log.info("Data Ingestion did interrupted and restarting from 0.")
                    else:
                        self.log.info("Data Ingestion did not interrupted but complete before rebalance operation.")
                        
                    query = "select count(*) from {0};".format(self.cbas_dataset_name)
                    self.cbas_util._run_concurrent_queries(query,"immediate",10)
                    break
            total_cbas_nodes -= 1
            
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,self.num_items*2):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")
        pass
    
    def test_cc_swap_rebalance(self):
        self.setup_for_test(skip_data_loading=True)
        query = "select sleep(count(*),50000) from {0};".format(self.cbas_dataset_name)
        handles = self.cbas_util._run_concurrent_queries(query,"async",10)
        self.ingestion_in_progress()
        
        replicas_before_rebalance=len(self.cbas_util.get_replicas_info(self.shell))
        
        self.cbas_util = cbas_utils(self.master, self.cbas_servers[0])
        self.cbas_node = self.cbas_servers[0]
        
        self.cluster_util.add_node(node=self.cbas_servers[-1],rebalance=False)
        self.cluster_util.remove_node([self.otpNodes[0]],wait_for_rebalance=False)
        
        self.sleep(5)
        while self.rest._rebalance_progress_status() == "running":
            replicas = self.cbas_util.get_replicas_info(self.shell)
            if replicas:
                for replica in replicas:
                    self.log.info("replica state during rebalance: %s"%replica['status'])
        self.sleep(5)
        
        replicas = self.cbas_util.get_replicas_info(self.shell)
        replicas_after_rebalance=len(replicas)
        self.assertEqual(replicas_after_rebalance, replicas_before_rebalance, "%s,%s"%(replicas_after_rebalance,replicas_before_rebalance))
        
        for replica in replicas:
            self.log.info("replica state during rebalance: %s"%replica['status'])
            self.assertEqual(replica['status'], "IN_SYNC","Replica state is incorrect: %s"%replica['status'])
                                
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        self.log.info("Items before service restart: %s"%items_in_cbas_bucket)
                
        items_in_cbas_bucket = 0
        start_time=time.time()
        while (items_in_cbas_bucket == 0 or items_in_cbas_bucket == -1) and time.time()<start_time+60:
            try:
                items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            except:
                pass
            
        self.log.info("After rebalance operation docs in CBAS bucket : %s"%items_in_cbas_bucket)
        if items_in_cbas_bucket < self.num_items*2 and items_in_cbas_bucket>self.num_items:
            self.log.info("Data Ingestion Interrupted successfully")
        elif items_in_cbas_bucket < self.num_items:
            self.log.info("Data Ingestion did interrupted and restarting from 0.")
        else:
            self.log.info("Data Ingestion did not interrupted but complete before rebalance operation.")
            
        run_count = 0
        fail_count = 0
        success_count = 0
        aborted_count = 0
        shell=RemoteMachineShellConnection(self.master)
        for handle in handles:
            status, hand = self.cbas_util.retrieve_request_status_using_handle(self.master, handle, shell)
            if status == "running":
                run_count += 1
                self.log.info("query with handle %s is running."%handle)
            elif status == "failed":
                fail_count += 1
                self.log.info("query with handle %s is failed."%handle)
            elif status == "success":
                success_count += 1
                self.log.info("query with handle %s is successful."%handle)
            else:
                aborted_count +=1
                self.log.info("Queued job is deleted: %s"%status)
                
        self.log.info("After service restart %s queued jobs are Running."%run_count)
        self.log.info("After service restart %s queued jobs are Failed."%fail_count)
        self.log.info("After service restart %s queued jobs are Successful."%success_count)
        self.log.info("After service restart %s queued jobs are Aborted."%aborted_count)
        
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(query,"immediate",100)
        
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,self.num_items*2):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")
                        
    def test_reboot_nodes(self):
        #Test for reboot CC and reboot all nodes.
        self.setup_for_test(skip_data_loading=True)
        self.node_type = self.input.param('node_type','CC')
        
        replica_nodes = self.cbas_util.get_replicas_info(self.shell)
        
    