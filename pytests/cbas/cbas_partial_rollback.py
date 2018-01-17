'''
Created on Jan 4, 2018

@author: riteshagarwal
'''

from cbas_base import CBASBaseTest, TestInputSingleton
from lib.memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection

class PartialRollback_CBAS(CBASBaseTest):

    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket":False})
        
        super(PartialRollback_CBAS, self).setUp()
            
        ''' Considering all the scenarios where:
        1. There can be 1 KV and multiple cbas nodes(and tests wants to add all cbas into cluster.)
        2. There can be 1 KV and multiple cbas nodes(and tests wants only 1 cbas node)
        3. There can be only 1 node running KV,CBAS service.
        NOTE: Cases pending where there are nodes which are running only cbas. For that service check on nodes is needed.
        '''
        if "add_all_cbas_nodes" in self.input.test_params and self.input.test_params["add_all_cbas_nodes"] and len(self.cbas_servers) > 1:
            self.add_all_cbas_node_then_rebalance()
        
        '''Create default bucket'''
        self.create_default_bucket()
        self.cbas_util.createConn("default")

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
        self.cbas_util.create_dataset_on_bucket_merge_policy(cbas_bucket_name=self.cbas_bucket_name,
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


    def tearDown(self):
        super(PartialRollback_CBAS, self).tearDown()

    def test_ingestion_after_kv_rollback_create_ops(self):
        self.setup_for_test()

        # Stop Persistence on Node A & Node B
        self.log.info("Stopping persistence on NodeA")
        mem_client = MemcachedClientHelper.direct_client(self.master,
                                                         self.cb_bucket_name)
        mem_client.stop_persistence()

        # Perform Create, Update, Delete ops in the CB bucket
        self.log.info("Performing Mutations")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items/2, "create", self.num_items,
                                               self.num_items*3/2)
        
        kv_nodes = self.get_kv_nodes(self.servers, self.master)
        items_in_cb_bucket = 0
        for node in kv_nodes:
            items_in_cb_bucket += self.get_item_count(node,self.cb_bucket_name)
        # Validate no. of items in CBAS dataset
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, items_in_cb_bucket, 0),
                        "No. of items in CBAS dataset do not match that in the CB bucket")
        
        # Count no. of items in CB & CBAS Buckets
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        
        self.log.info("Before Rollback --- # docs in CB bucket : %s, # docs in CBAS bucket : %s",
                      items_in_cb_bucket, items_in_cbas_bucket)

        self.assertTrue(items_in_cb_bucket == items_in_cbas_bucket,
                        "Before Rollback : # Items in CBAS bucket does not match that in the CB bucket")

        # Kill memcached on Node A so that Node B becomes master
        self.log.info("Kill Memcached process on NodeA")
        shell = RemoteMachineShellConnection(self.master)
        shell.kill_memcached()
        self.sleep(5,"Wait for 5 secs for memcached restarts.")
        
        items_in_cb_bucket = 0
        for node in kv_nodes:
            items_in_cb_bucket += self.get_item_count(node,self.cb_bucket_name)
        
        self.log.info("Items in CB bucket after rollback: %s"%items_in_cb_bucket)
        
        self.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name], items_in_cb_bucket)
        # Count no. of items in CB & CBAS Buckets
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        
        self.log.info("After Rollback --- # docs in CB bucket : %s, # docs in CBAS bucket : %s",
                      items_in_cb_bucket, items_in_cbas_bucket)

        self.assertTrue(items_in_cb_bucket == items_in_cbas_bucket,
                        "After Rollback : # Items in CBAS bucket does not match that in the CB bucket")
        
    def test_ingestion_after_kv_rollback_delete_ops(self):
        self.setup_for_test()

        # Stop Persistence on Node A & Node B
        self.log.info("Stopping persistence on NodeA")
        mem_client = MemcachedClientHelper.direct_client(self.master,
                                                         self.cb_bucket_name)
        mem_client.stop_persistence()

        # Perform Create, Update, Delete ops in the CB bucket
        self.log.info("Performing Mutations")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items/2, "delete", 0,
                                               self.num_items / 2)
        
        kv_nodes = self.get_kv_nodes(self.servers, self.master)
        items_in_cb_bucket = 0
        for node in kv_nodes:
            items_in_cb_bucket += self.get_item_count(node,self.cb_bucket_name)
        
        # Validate no. of items in CBAS dataset
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, items_in_cb_bucket, 0),
                        "No. of items in CBAS dataset do not match that in the CB bucket")
        
        # Count no. of items in CB & CBAS Buckets
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        
        self.log.info("Before Rollback --- # docs in CB bucket : %s, # docs in CBAS bucket : %s",
                      items_in_cb_bucket, items_in_cbas_bucket)

        self.assertTrue(items_in_cb_bucket == items_in_cbas_bucket,
                        "Before Rollback : # Items in CBAS bucket does not match that in the CB bucket")

        # Kill memcached on Node A so that Node B becomes master
        self.log.info("Kill Memcached process on NodeA")
        shell = RemoteMachineShellConnection(self.master)
        shell.kill_memcached()
        self.sleep(5,"Wait for 5 secs for memcached restarts.")
        
        items_in_cb_bucket = 0
        for node in kv_nodes:
            items_in_cb_bucket += self.get_item_count(node,self.cb_bucket_name)
        
        self.log.info("Items in CB bucket after rollback: %s"%items_in_cb_bucket)
        
        self.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name], items_in_cb_bucket)
        # Count no. of items in CB & CBAS Buckets
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        
        self.log.info("After Rollback --- # docs in CB bucket : %s, # docs in CBAS bucket : %s",
                      items_in_cb_bucket, items_in_cbas_bucket)

        self.assertTrue(items_in_cb_bucket == items_in_cbas_bucket,
                        "After Rollback : # Items in CBAS bucket does not match that in the CB bucket")


    def test_ingestion_after_kv_rollback_cbas_disconnected(self):
        self.setup_for_test()

        # Stop Persistence on Node A & Node B
        self.log.info("Stopping persistence on NodeA")
        mem_client = MemcachedClientHelper.direct_client(self.master,
                                                         self.cb_bucket_name)
        mem_client.stop_persistence()
        
        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)
        # Perform Create, Update, Delete ops in the CB bucket
        self.log.info("Performing Mutations")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items/2, "delete", 0,
                                               self.num_items / 2)

        # Count no. of items in CB & CBAS Buckets
        kv_nodes = self.get_kv_nodes(self.servers, self.master)

        # Kill memcached on Node A so that Node B becomes master
        self.log.info("Kill Memcached process on NodeA")
        shell = RemoteMachineShellConnection(self.master)
        shell.kill_memcached()
        self.sleep(5,"Wait for 5 secs for memcached restarts.")
        
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        self.assertTrue(items_in_cbas_bucket>0, "Ingestion starting from 0 after rollback.")
        
        self.cbas_util.connect_to_bucket(self.cbas_bucket_name)
        self.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name], self.num_items/2)
        # Count no. of items in CB & CBAS Buckets
        items_in_cb_bucket = 0
        for node in kv_nodes:
            items_in_cb_bucket += self.get_item_count(node,self.cb_bucket_name)
            
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        
        self.log.info("After Rollback --- # docs in CB bucket : %s, # docs in CBAS bucket : %s",
                      items_in_cb_bucket, items_in_cbas_bucket)

        self.assertTrue(items_in_cb_bucket == items_in_cbas_bucket,
                        "After Rollback : # Items in CBAS bucket does not match that in the CB bucket")
