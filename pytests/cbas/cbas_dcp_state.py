import time
import json

from cbas.cbas_base import CBASBaseTest
from remote.remote_util import RemoteMachineShellConnection


class CBASDCPState(CBASBaseTest):

    def setUp(self):
        super(CBASDCPState, self).setUp()

        self.log.info("Establish remote connection to CBAS node and Empty analytics log")
        self.shell = RemoteMachineShellConnection(self.cbas_node)
        self.shell.execute_command("echo '' > /opt/couchbase/var/lib/couchbase/logs/analytics.log")
        
        self.log.info("Load documents in the default bucket")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Create dataset")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)

        self.log.info("Add a CBAS nodes")
        self.assertTrue(self.add_node(self.servers[1], services=["cbas"], rebalance=True), msg="Failed to add CBAS node")
        
        self.log.info("Connect to Local link")
        self.cbas_util.connect_link()

        self.log.info("Validate count on CBAS")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items), msg="Count mismatch on CBAS")
        
        self.log.info("Kill CBAS/JAVA Process on NC node")
        self.shell.kill_multiple_process(['java', 'cbas'])

    """
    test_dcp_state_with_cbas_bucket_connected_kv_bucket_deleted,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds,items=10000
    """
    def test_dcp_state_with_cbas_bucket_connected_kv_bucket_deleted(self):
        """
        Cover's the scenario: CBAS bucket is connected, KV bucket is deleted
        Expected Behaviour: Rebalance fail's and we see error in logs "Datasets in different partitions have different DCP states. Mutations needed to catch up = <Mutation_count>. User action: Try again later"
        """
        self.log.info("Delete KV bucket")
        self.delete_bucket_or_assert(serverInfo=self.master)
        
        self.log.info("Add a CBAS nodes")
        self.assertTrue(self.add_node(self.servers[3], services=["cbas"], rebalance=False), msg="Failed to add CBAS node")
        
        self.log.info("Rebalance in CBAS node")
        rebalance_success = False
        try:
            rebalance_success = self.rebalance()
        except Exception as e:
            pass
        self.assertFalse(rebalance_success, msg="Rebalance in of CBAS node must fail")
        
        self.log.info("Grep Analytics logs for user action")
        result, _ = self.shell.execute_command("grep 'Datasets in different partitions have different DCP states.' /opt/couchbase/var/lib/couchbase/logs/analytics.log")
        self.assertTrue("User action: Try again later" in result[0], msg="User error message not found...")
        
        self.log.info("Wait for CBAS bucket to be disconnected")
        start_time = time.time()
        cbas_disconnected = False
        while time.time() < start_time + 120:
            try:
                status, content, _ = self.cbas_util.fetch_bucket_state_on_cbas()
                self.assertTrue(json.loads(content)['buckets'][0]['state'] == "disconnected")
                cbas_disconnected = True
                break
            except:
                self.sleep(15, message="Wait 15 more seconds for CBAS bucket to be disconnected")
                pass
        
        if not cbas_disconnected:
            self.fail(msg="Failing the test case before rebalance.Since, CBAS bucket is connected even after 120 seconds despite no KV bucket")
            
        self.log.info("Rebalance in CBAS node")
        self.assertTrue(self.rebalance(), msg="Rebalance in CBAS node failed")
    
    """
    test_dcp_state_with_cbas_bucket_disconnected_kv_bucket_deleted,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds,items=10000
    """
    def test_dcp_state_with_cbas_bucket_disconnected_kv_bucket_deleted(self):
        """
        Cover's the scenario: CBAS bucket is disconnected, KV bucket deleted
        Expected Behaviour: Rebalance must succeeds and we must see in logs "Bucket Bucket:Default.cbas doesn't exist in KV anymore... nullifying its DCP state"
        """
        self.log.info("Delete KV bucket")
        self.delete_bucket_or_assert(serverInfo=self.master)
        
        self.log.info("Disconnect from CBAS bucket")
        start_time = time.time()
        while time.time() < start_time + 120:
            try:
                self.cbas_util.disconnect_link()
                break
            except Exception as e:
                pass
        
        self.log.info("Add a CBAS nodes")
        self.assertTrue(self.add_node(self.servers[3], services=["cbas"], rebalance=False),
                         msg="Failed to add a CBAS node")
        
        self.log.info("Rebalance in CBAS node")
        self.assertTrue(self.rebalance(), msg="Rebalance in CBAS node failed")
        
        self.log.info("Grep Analytics logs for message")
        result, _ = self.shell.execute_command("grep 'exist in KV anymore... nullifying its DCP state' /opt/couchbase/var/lib/couchbase/logs/analytics.log")
        self.assertTrue("nullifying its DCP state" in result[0], msg="Expected message 'nullifying its DCP state' not found")
    
    """
    test_dcp_state_with_cbas_bucket_disconnected_kv_bucket_deleted_and_recreate,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds,items=10000
    """
    def test_dcp_state_with_cbas_bucket_disconnected_kv_bucket_deleted_and_recreate(self):
        """
        Cover's the scenario: CBAS bucket is disconnected, CB bucket is deleted and then recreated
        Expected Behaviour: Rebalance succeeds with message in logs "Bucket Bucket:Default.cbas doesn't exist in KV anymore... nullifying its DCP state, then again after bucket is re-created, data is re-ingested from 0"
        """
        self.log.info("Delete KV bucket")
        self.delete_bucket_or_assert(serverInfo=self.master)
        
        self.log.info("Disconnect from CBAS bucket")
        start_time = time.time()
        while time.time() < start_time + 120:
            try:
                self.cbas_util.disconnect_link()
                break
            except Exception as e:
                pass
        
        self.log.info("Add a CBAS nodes")
        self.assertTrue(self.add_node(self.servers[3], services=["cbas"], rebalance=False),
                         msg="Failed to add a CBAS node")
        
        self.log.info("Rebalance in CBAS node")
        self.assertTrue(self.rebalance(), msg="Rebalance in CBAS node failed")
        
        self.log.info("Grep Analytics logs for message")
        result, _ = self.shell.execute_command("grep 'exist in KV anymore... nullifying its DCP state' /opt/couchbase/var/lib/couchbase/logs/analytics.log")
        self.assertTrue("nullifying its DCP state" in result[0], msg="Expected message 'nullifying its DCP state' not found")
        
        self.log.info("Recreate KV bucket")
        self.create_default_bucket()
        
        self.log.info("Load documents in the default bucket")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items // 100, "create", 0, self.num_items // 100)

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)
        
        self.log.info("Connect to Local link")
        self.cbas_util.connect_link(with_force=True)
        
        self.log.info("Validate count on CBAS post KV bucket re-created")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items // 100), msg="Count mismatch on CBAS")
          
    """
    test_dcp_state_with_cbas_bucket_disconnected_cb_bucket_exist,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds,items=10000,user_action=connect_cbas_bucket
    test_dcp_state_with_cbas_bucket_disconnected_cb_bucket_exist,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds,items=10000
    """
    def test_dcp_state_with_cbas_bucket_disconnected_cb_bucket_exist(self):
        """
        Cover's the scenario: CBAS bucket is disconnected
        Expected Behaviour: Rebalance fails with user action Connect the bucket or drop the dataset"
        """
        self.log.info("Disconnect from CBAS bucket")
        start_time = time.time()
        while time.time() < start_time + 120:
            try:
                self.cbas_util.disconnect_link()
                break
            except Exception as e:
                pass
        
        self.log.info("Add a CBAS nodes")
        self.assertTrue(self.add_node(self.servers[3], services=["cbas"], rebalance=False),
                         msg="Failed to add a CBAS node")
        
        self.log.info("Rebalance in CBAS node")
        rebalance_success = False
        try:
            rebalance_success = self.rebalance()
        except Exception as e:
            pass
        self.assertFalse(rebalance_success, msg="Rebalance in of CBAS node must fail")
        
        self.log.info("Grep Analytics logs for user action")
        result, _ = self.shell.execute_command("grep 'Datasets in different partitions have different DCP states.' /opt/couchbase/var/lib/couchbase/logs/analytics.log")
        self.assertTrue("User action: Connect the bucket:" in result[0] and "or drop the dataset: Default.ds" in result[0], msg="User action not found.")
        
        user_action = self.input.param("user_action", "drop_dataset")
        if user_action == "connect_cbas_bucket":
            self.log.info("Connect back Local link")
            self.cbas_util.connect_link()
            self.sleep(15, message="Wait for link to be connected")
        else:
            self.log.info("Dropping the dataset")
            self.cbas_util.drop_dataset(self.cbas_dataset_name)
        
        self.log.info("Rebalance in CBAS node")
        self.assertTrue(self.rebalance(), msg="Rebalance in CBAS node must succeed after user has taken the specified action.")
          
    def tearDown(self):
        super(CBASDCPState, self).tearDown() 
