import time

from cbas.cbas_base import CBASBaseTest
from membase.api.exception import RebalanceFailedException
from remote.remote_util import RemoteMachineShellConnection


class CBASDCPState(CBASBaseTest):

    def setUp(self):
        super(CBASDCPState, self).setUp()

        self.log.info("Establish remote connection to CBAS node and truncate log file")
        self.shell = RemoteMachineShellConnection(self.cbas_node)
        self.shell.execute_command("echo '' > /opt/couchbase/var/lib/couchbase/logs/analytics.log")
        
        self.log.info("Load data in the default bucket")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Create dataset")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)

        self.log.info("Add a CBAS nodes")
        self.assertTrue(self.add_node(self.cbas_servers[0], services=["cbas"], rebalance=True), msg="Failed to add CBAS node")
        
        self.log.info("Connect to CBAS bucket")
        self.cbas_util.connect_to_bucket()

        self.log.info("Validate count on CBAS")
        self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items)
        
        self.log.info("Kill CBAS/JAVA Process on NC node")
        self.shell.kill_multiple_process(['java', 'cbas'])

    """
    cbas.cbas_dcp_state.CBASDCPState.test_dcp_state_with_cbas_bucket_connected_cb_bucket_does_not_exist,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,items=10000
    """
    def test_dcp_state_with_cbas_bucket_connected_cb_bucket_does_not_exist(self):
        """
        Cover's the scenario: CBAS bucket is connected, CB bucket is deleted/dropped
        Expected Behaviour: Rebalance fails see user error in logs "Shadows in different partitions have different DCP states. Mutations needed to catch up = <Mutation_count>. User action: Try again later"
        """
        self.log.info("Delete CB bucket")
        self.delete_bucket_or_assert(serverInfo=self.master)
        
        self.log.info("Add a CBAS nodes")
        self.assertTrue(self.add_node(self.cbas_servers[1], services=["cbas"], rebalance=False), msg="Failed to add CBAS node")
        
        self.log.info("Rebalance in CBAS node")
        rebalance_success = False
        try:
            rebalance_success = self.rebalance()
        except Exception as e:
            pass
        self.assertFalse(rebalance_success, msg="Rebalance in of CBAS node must fail")
        
        self.log.info("Grep Analytics logs for user action")
        result, _ = self.shell.execute_command("grep 'Shadows in different partitions have different DCP states.' /opt/couchbase/var/lib/couchbase/logs/analytics.log")
        print("*****************")
        print(result)
        print("*****************")
        self.assertTrue("User action: Try again later" in result[0], msg="User error message not found...")
        
        self.log.info("Sleep for 30 seconds")
        self.sleep(30, message="Try rebalance after 30 seconds")
        
        self.log.info("Rebalance in CBAS node")
        self.assertTrue(self.rebalance(), msg="Rebalance in CBAS node failed")
    
    """
    cbas.cbas_dcp_state.CBASDCPState.test_dcp_state_with_cbas_bucket_disconnected_cb_bucket_does_not_exist,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,items=10000
    """
    def test_dcp_state_with_cbas_bucket_disconnected_cb_bucket_does_not_exist(self):
        """
        Cover's the scenario: CBAS bucket is disconnected, CB bucket is deleted/dropped
        Expected Behaviour: Rebalance succeeds with message in logs "Bucket Bucket:Default.cbas doesn't exist in KV anymore... nullifying its DCP state"
        """
        self.log.info("Delete CB bucket")
        self.delete_bucket_or_assert(serverInfo=self.master)
        
        self.log.info("Disconnect from CBAS bucket")
        start_time = time.time()
        while time.time() < start_time + 120:
            try:
                self.cbas_util.disconnect_from_bucket()
                break
            except Exception as e:
                pass
        
        self.log.info("Add a CBAS nodes")
        self.assertTrue(self.add_node(self.cbas_servers[1], services=["cbas"], rebalance=False),
                         msg="Failed to add a CBAS node")
        
        self.log.info("Rebalance in CBAS node")
        self.assertTrue(self.rebalance(), msg="Rebalance in CBAS node failed")
        
        self.log.info("Grep Analytics logs for user action")
        result, _ = self.shell.execute_command("grep 'exist in KV anymore... nullifying its DCP state' /opt/couchbase/var/lib/couchbase/logs/analytics.log")
        self.assertTrue("nullifying its DCP state" in result[0], msg="User error message not found...")
        
    """
    cbas.cbas_dcp_state.CBASDCPState.test_dcp_state_with_cbas_bucket_disconnected_cb_bucket_exist,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,items=10000,user_action=connect_cbas_bucket
    cbas.cbas_dcp_state.CBASDCPState.test_dcp_state_with_cbas_bucket_disconnected_cb_bucket_exist,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,items=10000
    """
    def test_dcp_state_with_cbas_bucket_disconnected_cb_bucket_exist(self):
        """
        Cover's the scenario: CBAS bucket is disconnected
        Expected Behaviour: Rebalance fails with user action Connect the bucket or drop the dataset"
        """
        self.log.info("Disconnect from CBAS bucket")
        self.cbas_util.disconnect_from_bucket()
        
        self.log.info("Add a CBAS nodes")
        self.assertTrue(self.add_node(self.cbas_servers[1], services=["cbas"], rebalance=False),
                         msg="Failed to add a CBAS node")
        
        self.log.info("Rebalance in CBAS node")
        rebalance_success = False
        try:
            rebalance_success = self.rebalance()
        except Exception as e:
            pass
        self.assertFalse(rebalance_success, msg="Rebalance in of CBAS node must fail")
        
        self.log.info("Grep Analytics logs for user action")
        result, _ = self.shell.execute_command("grep 'Shadows in different partitions have different DCP states.' /opt/couchbase/var/lib/couchbase/logs/analytics.log")
        print("*****************")
        print(result)
        print("*****************")
        self.assertTrue("User action: Connect the bucket: Bucket:Default.cbas or drop the dataset: Default.ds" in result[0], msg="User action not found.")
        
        user_action = self.user.input("user_action", "drop_dataset")
        if user_action == "connect_cbas_bucket":
            self.log.info("Connect to CBAS bucket")
            self.cbas_util.connect_to_bucket()
        else:
            self.log.info("Drop the dataset")
            self.cbas_util.drop_dataset(self.cbas_dataset_name)
        
        self.log.info("Rebalance in CBAS node")
        self.assertTrue(self.rebalance(), msg="Rebalance in CBAS node failed")
          
    def tearDown(self):
        super(CBASDCPState, self).tearDown() 
