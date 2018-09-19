import time

from cbas_base import *
from couchbase_helper.stats_tools import StatsCommon
from lib.memcached.helper.data_helper import MemcachedClientHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from node_utils.node_ready_functions import NodeHelper
from BucketLib.BucketOperations import BucketHelper
from sdk_client import SDKClient


class CBASBucketOperations(CBASBaseTest):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket":False})
        
        super(CBASBucketOperations, self).setUp()
            
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

    def tearDown(self):
        self.cleanup_cbas()
        super(CBASBucketOperations, self).tearDown()

    def setup_for_test(self, skip_data_loading=False):
        if not skip_data_loading:
            # Load Couchbase bucket first.
            self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0,
                                                   self.num_items)

        # Create bucket on CBAS
        self.cbas_util.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip)

        # Create dataset on the CBAS bucket
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.cb_bucket_name,
                                      cbas_dataset_name=self.cbas_dataset_name)

        # Create indexes on the CBAS bucket
        self.create_secondary_indexes = self.input.param("create_secondary_indexes",True)
        if self.create_secondary_indexes:
            self.index_fields = "profession:string,number:bigint"
            create_idx_statement = "create index {0} on {1}({2});".format(
                self.index_name, self.cbas_dataset_name, self.index_fields)
            status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
                create_idx_statement)
    
            self.assertTrue(status == "success", "Create Index query failed")
    
            self.assertTrue(
                self.cbas_util.verify_index_created(self.index_name, self.index_fields.split(","),
                                          self.cbas_dataset_name)[0])
        
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

    def load_docs_in_cb_bucket_before_cbas_connect(self):
        self.setup_for_test()

    def load_docs_in_cb_bucket_before_and_after_cbas_connect(self):
        self.setup_for_test()

        # Load more docs in Couchbase bucket.
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 2)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 2):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def load_docs_in_cb_bucket_after_cbas_connect(self):
        self.setup_for_test(skip_data_loading=True)

        # Load Couchbase bucket first.
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0,
                                               self.num_items)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def delete_some_docs_in_cb_bucket(self):
        self.setup_for_test()

        # Delete some docs in Couchbase bucket.
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "delete", 0,
                                               self.num_items / 2)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items / 2):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def delete_all_docs_in_cb_bucket(self):
        self.setup_for_test()

        # Delete all docs in Couchbase bucket.
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "delete", 0,
                                               self.num_items)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def update_some_docs_in_cb_bucket(self):
        self.setup_for_test()

        # Update some docs in Couchbase bucket
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "update", 0,
                                               self.num_items / 10)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items,
                                                      self.num_items / 10):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def update_all_docs_in_cb_bucket(self):
        self.setup_for_test()

        # Update all docs in Couchbase bucket
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "update", 0,
                                               self.num_items)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items,
                                                      self.num_items):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def create_update_delete_cb_bucket_then_cbas_connect(self):
        self.setup_for_test()

        # Disconnect from bucket
        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)

        # Perform Create, Update, Delete ops in the CB bucket
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 2)
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "update", 0,
                                               self.num_items)
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "delete", 0,
                                               self.num_items / 2)

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 3 / 2,
                                                      self.num_items / 2):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def create_update_delete_cb_bucket_with_cbas_connected(self):
        self.setup_for_test()

        # Perform Create, Update, Delete ops in the CB bucket
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 2)
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "update", 0,
                                               self.num_items)
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "delete", 0,
                                               self.num_items / 2)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 3 / 2,
                                                      self.num_items / 2):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def flush_cb_bucket_with_cbas_connected(self):
        self.setup_for_test()

        # Flush the CB bucket
        self.cluster.bucket_flush(server=self.master,
                                  bucket=self.cb_bucket_name)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def flush_cb_bucket_then_cbas_connect(self):
        self.setup_for_test()

        # Disconnect from bucket
        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)

        # Flush the CB bucket
        self.cluster.bucket_flush(server=self.master,
                                  bucket=self.cb_bucket_name)

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def delete_cb_bucket_with_cbas_connected(self):
        self.setup_for_test()

        # Delete the CB bucket
        self.cluster.bucket_delete(server=self.master,
                                   bucket=self.cb_bucket_name)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def delete_cb_bucket_then_cbas_connect(self):
        self.setup_for_test()

        # Disconnect from bucket
        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)

        # Delete the CB bucket
        self.cluster.bucket_delete(server=self.master,
                                   bucket=self.cb_bucket_name)

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    """
    cbas.cbas_bucket_operations.CBASBucketOperations.delete_kv_bucket_then_drop_dataset_without_disconnecting_link,cb_bucket_name=default,cbas_bucket_name=default_cbas,cbas_dataset_name=ds,items=10000
    """
    def delete_kv_bucket_then_drop_dataset_without_disconnecting_link(self):
        
        # setup test
        self.setup_for_test()

        # Delete the KV bucket
        self.delete_bucket_or_assert(serverInfo=self.master)

        # Check Bucket state
        start_time = time.time()
        while start_time + 120 > time.time():
            status, content, _ = self.cbas_util.fetch_bucket_state_on_cbas()
            self.assertTrue(status, msg="Fetch bucket state failed")
            content = json.loads(content)
            self.log.info(content)
            if content['buckets'][0]['state'] == "disconnected":
                break
            self.sleep(1)
            
        # Drop dataset with out disconnecting the Link
        self.sleep(2, message="Sleeping 2 seconds after bucket disconnect")
        self.assertTrue(self.cbas_util.drop_dataset(self.cbas_dataset_name), msg="Failed to drop dataset")

    def compact_cb_bucket_with_cbas_connected(self):
        self.setup_for_test()

        # Compact the CB bucket
        self.cluster.compact_bucket(server=self.master,
                                    bucket=self.cb_bucket_name)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def compact_cb_bucket_then_cbas_connect(self):
        self.setup_for_test()

        # Disconnect from bucket
        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)

        # Compact the CB bucket
        self.cluster.compact_bucket(server=self.master,
                                    bucket=self.cb_bucket_name)

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def test_ingestion_resumes_on_reconnect(self):
        self.setup_for_test()

        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "update", 0,
                                               self.num_items / 4)

        self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                               self.num_items,
                                               self.num_items / 4)

        # Disconnect from bucket
        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)

        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "update",
                                               self.num_items / 4,
                                               self.num_items / 2)

        # Connect to Bucket and sleep for 2s to allow ingestion to start
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        self.sleep(5)

        # Validate no. of items in CBAS dataset
        count, mutated_count = self.cbas_util.get_num_items_in_cbas_dataset(
            self.cbas_dataset_name)

        if not (self.num_items / 4 < mutated_count):
            self.fail(
                "Fail : Count after bucket connect = %s. Ingestion has restarted." % mutated_count)
        else:
            self.log.info("Count after bucket connect = %s", mutated_count)

    def test_ingestion_after_kv_rollback(self):
        self.setup_for_test()

        # Stop Persistence on Node A & Node B
        self.log.info("Stopping persistence on NodeA & NodeB")
        mem_client = MemcachedClientHelper.direct_client(self.input.servers[0],
                                                         self.cb_bucket_name)
        mem_client.stop_persistence()
        mem_client = MemcachedClientHelper.direct_client(self.input.servers[1],
                                                         self.cb_bucket_name)
        mem_client.stop_persistence()

        # Perform Create, Update, Delete ops in the CB bucket
        self.log.info("Performing Mutations")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "delete", 0,
                                               self.num_items / 2)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items / 2, 0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

        # Count no. of items in CB & CBAS Buckets
        items_in_cb_bucket = self.get_item_count(self.master,
                                                 self.cb_bucket_name)
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(
            self.cbas_dataset_name)
        self.log.info(
            "Before Rollback --- # docs in CB bucket : %s, # docs in CBAS bucket : %s",
            items_in_cb_bucket, items_in_cbas_bucket)

        if items_in_cb_bucket != items_in_cbas_bucket:
            self.fail(
                "Before Rollback : # Items in CBAS bucket does not match that in the CB bucket")

        # Kill memcached on Node A so that Node B becomes master
        self.log.info("Kill Memcached process on NodeA")
        shell = RemoteMachineShellConnection(self.master)
        shell.kill_memcached()

        # Start persistence on Node B
        self.log.info("Starting persistence on NodeB")
        mem_client = MemcachedClientHelper.direct_client(self.input.servers[1],
                                                         self.cb_bucket_name)
        mem_client.start_persistence()

        # Failover Node B
        self.log.info("Failing over NodeB")
        self.sleep(10)
        failover_task = self._cb_cluster.async_failover(self.input.servers,
                                                        [self.input.servers[1]])
        failover_task.result()

        # Wait for Failover & CBAS rollback to complete
        self.sleep(120)

        # Count no. of items in CB & CBAS Buckets
        items_in_cb_bucket = self.get_item_count(self.master,
                                                 self.cb_bucket_name)
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(
            self.cbas_dataset_name)
        self.log.info(
            "After Rollback --- # docs in CB bucket : %s, # docs in CBAS bucket : %s",
            items_in_cb_bucket, items_in_cbas_bucket)

        if items_in_cb_bucket != items_in_cbas_bucket:
            self.fail(
                "After Rollback : # Items in CBAS bucket does not match that in the CB bucket")
    
    '''
    cbas.cbas_bucket_operations.CBASBucketOperations.test_bucket_flush_while_index_are_created,cb_bucket_name=default,cbas_bucket_name=default_bucket,cbas_dataset_name=default_ds,items=10,index_fields=profession:String-first_name:String
    '''
    def test_bucket_flush_while_index_are_created(self):

        self.log.info('Add documents, create CBAS buckets, dataset and validate count')
        self.setup_for_test()

        self.log.info('Disconnect CBAS bucket')
        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)

        self.log.info('Create secondary index in Async')
        index_fields = self.input.param("index_fields", None)
        index_fields = index_fields.replace('-', ',')
        query = "create index {0} on {1}({2});".format("sec_idx", self.cbas_dataset_name, index_fields)
        create_index_task = self.cluster.async_cbas_query_execute(self.master, self.cbas_node, None, query, 'default')
        
        self.log.info('Flush bucket while index are getting created')
        self.cluster.bucket_flush(server=self.master, bucket=self.cb_bucket_name)
        
        self.log.info('Get result on index creation')
        create_index_task.get_result()
        
        self.log.info('Connect back cbas bucket')
        self.cbas_util.connect_to_bucket(self.cbas_bucket_name)

        self.log.info('Validate no. of items in CBAS dataset')
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")
    
    '''
    cbas.cbas_bucket_operations.CBASBucketOperations.test_kill_memcached_impact_on_bucket,items=100000,bucket_type=ephemeral
    '''
    def test_kill_memcached_impact_on_bucket(self):

        self.log.info('Add documents, create CBAS buckets, dataset and validate count')
        self.setup_for_test()

        self.log.info('Kill memcached service')
        self.kill_memcached()
        
        self.log.info('Validate document count')
        count_n1ql = self.rest.query_tool('select count(*) from %s' % (self.cb_bucket_name))['results'][0]['$1']
        self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, 0)
    
    '''
    cbas.cbas_bucket_operations.CBASBucketOperations.test_restart_kv_server_impact_on_bucket,items=100000,bucket_type=ephemeral
    '''
    def test_restart_kv_server_impact_on_bucket(self):

        self.log.info('Add documents, create CBAS buckets, dataset and validate count')
        self.setup_for_test()

        self.log.info('Restart couchbase')
        NodeHelper.reboot_server_new(self.master, self)
        
        self.log.info('Validate document count')
        count_n1ql = self.rest.query_tool('select count(*) from %s' % (self.cb_bucket_name))['results'][0]['$1']
        self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, count_n1ql)

class CBASEphemeralBucketOperations(CBASBaseTest):

    def setUp(self):

        super(CBASEphemeralBucketOperations, self).setUp()

        self.log.info("Create Ephemeral bucket")
        self.bucket_ram = self.input.param("bucket_ram", 100)
        self.create_bucket(self.master, name=self.cb_bucket_name, bucket_ram=self.bucket_ram, bucket_type=self.bucket_type, evictionPolicy=self.eviction_policy)

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Fetch RAM document load percentage")
        self.document_ram_percentage = self.input.param("document_ram_percentage", 0.90)

    def load_document_until_ram_percentage(self):
        self.start = 0
        self.num_items = 30000
        self.end = self.num_items
        while True:
            self.log.info("Add documents to bucket")
            self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", self.start, self.end)

            self.log.info("Calculate available free memory")
            stats_all_buckets = {}
            stats_all_buckets[self.cb_bucket_name] = StatsCommon()
            memory_used = int(stats_all_buckets[self.cb_bucket_name].get_stats([self.master], self.cb_bucket_name, '', 'mem_used')[self.servers[0]])

            if memory_used < (self.document_ram_percentage * self.bucket_ram * 1000000):
                self.log.info("Continue loading we have more free memory")
                self.start = self.end
                self.end = self.end + self.num_items
            else:
                break

    """
    cbas.cbas_bucket_operations.CBASEphemeralBucketOperations.test_no_eviction_impact_on_cbas,default_bucket=False,items=0,bucket_type=ephemeral,eviction_policy=noEviction,
    cb_bucket_name=default,cbas_dataset_name=ds,bucket_ram=100,document_ram_percentage=0.85
    """
    def test_no_eviction_impact_on_cbas(self):
        
        self.log.info("Create dataset")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)

        self.log.info("Connect to Local link")
        self.cbas_util.connect_link()
        
        self.log.info("Add documents until ram percentage")
        self.load_document_until_ram_percentage()

        self.log.info("Fetch current document count")
        bucket_helper = BucketHelper(self.master)
        item_count = bucket_helper.get_bucket(self.cb_bucket_name).stats.itemCount
        self.log.info("Completed base load with %s items" % item_count)

        self.log.info("Load more until we are out of memory")
        client = SDKClient(hosts=[self.master.ip], bucket=self.cb_bucket_name, password=self.master.rest_password)
        i = item_count
        insert_success = True
        while insert_success:
            insert_success = client.insert_document("key-id" + str(i), '{"name":"dave"}')
            i += 1
            
        self.log.info('Memory is full at {0} items'.format(i))
        self.log.info("As a result added more %s items" % (i - item_count))

        self.log.info("Fetch item count")
        stats = bucket_helper.get_bucket(self.cb_bucket_name).stats
        itemCountWhenOOM = stats.itemCount
        memoryWhenOOM = stats.memUsed
        self.log.info('Item count when OOM {0} and memory used {1}'.format(itemCountWhenOOM, memoryWhenOOM))

        self.log.info("Validate document count on CBAS")
        count_n1ql = self.rest.query_tool('select count(*) from %s' % (self.cb_bucket_name))['results'][0]['$1']
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, count_n1ql), msg="Count mismatch on CBAS")

    """
    cbas.cbas_bucket_operations.CBASEphemeralBucketOperations.test_nru_eviction_impact_on_cbas,default_bucket=False,items=0,bucket_type=ephemeral,eviction_policy=nruEviction,
    cb_bucket_name=default,cbas_dataset_name=ds,bucket_ram=100,document_ram_percentage=0.80
    """
    def test_nru_eviction_impact_on_cbas(self):
        
        self.log.info("Create dataset")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)

        self.log.info("Connect to Local link")
        self.cbas_util.connect_link()

        self.log.info("Add documents until ram percentage")
        self.load_document_until_ram_percentage()

        self.log.info("Fetch current document count")
        bucket_helper = BucketHelper(self.master)
        item_count = bucket_helper.get_bucket(self.cb_bucket_name).stats.itemCount
        self.log.info("Completed base load with %s items" % item_count)

        self.log.info("Fetch initial inserted 100 documents, so they are not removed")
        client = SDKClient(hosts=[self.master.ip], bucket=self.cb_bucket_name, password=self.master.rest_password)
        for i in range(100):
            client.get("test_docs-" + str(i))
            
        self.log.info("Add 20% more items to trigger NRU")
        for i in range(item_count, int(item_count * 1.2)):
            client.insert_document("key-id" + str(i), '{"name":"dave"}')
        
        self.log.info("Validate document count on CBAS")
        count_n1ql = self.rest.query_tool('select count(*) from %s' % (self.cb_bucket_name))['results'][0]['$1']
        if self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, count_n1ql):
            pass
        else:
            self.log.info("Document count mismatch might be due to ejection of documents on KV. Retry again")
            count_n1ql = self.rest.query_tool('select count(*) from %s' % (self.cb_bucket_name))['results'][0]['$1']
            self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, count_n1ql), msg="Count mismatch on CBAS")