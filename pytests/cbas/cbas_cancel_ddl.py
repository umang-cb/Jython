import time
import random

from cbas.cbas_base import CBASBaseTest
from remote.remote_util import RemoteMachineShellConnection


class CBASCancelDDL(CBASBaseTest):

    def setUp(self):
        super(CBASCancelDDL, self).setUp()

        self.log.info("Add all CBAS nodes")
        self.add_all_nodes_then_rebalance(self.cbas_servers)
        self.cbas_servers.append(self.cbas_node)

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

    """
    cbas.cbas_cancel_ddl.CBASCancelDDL.test_cancel_ddl_dataset_creation,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds,items=10000
    """

    def test_cancel_ddl_dataset_creation(self):
        """
        Cover's the scenario: Cancel dataset create DDL statement
        Expected Behaviour: Request sent will now either succeed or fail, or its connection will be abruptly closed
        """
        dataset_created = 0
        dataset_not_created = 0
        times = 0
        start_time = time.time()
        while time.time() < start_time + 600:
            times += 1
            self.log.info("Drop dataset if exists")
            status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("drop dataset %s if exists" % self.cbas_dataset_name)
            self.assertEquals(status, "success", msg="Drop dataset failed")

            self.log.info("Pick a time window between 0 - 300ms for killing of node")
            self.kill_window = random.randint(0, 300) / 1000.0
            print(self.kill_window)

            self.log.info("Pick the cbas node to kill java process")
            server_to_kill_java = self.cbas_servers[random.randint(0, 2)]
            shell = RemoteMachineShellConnection(server_to_kill_java)

            self.log.info("Pick the java process id to kill")
            java_process_id, _ = shell.execute_command("pgrep java")

            # Run the task Create dataset/Sleep window/Kill Java process in parallel
            self.log.info("Create dataset")
            tasks = self.cbas_util.async_query_execute("create dataset %s on %s" % (self.cbas_dataset_name, self.cb_bucket_name), "async", 1)

            self.log.info("Sleep for the window time")
            self.sleep(self.kill_window)

            self.log.info("kill Java process with id %s" % java_process_id[0])
            shell.execute_command("kill -9 %s" % (java_process_id[0]))

            self.log.info("Fetch task result")
            for task in tasks:
                task.get_result(1)

            self.log.info("Wait for request to complete and cluster to be active: Using private ping() function")
            while True:
                try:
                    status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("set `import-private-functions` `true`;ping();")
                    if status == "success":
                        break
                except:
                    pass

            self.log.info("Request sent will now either succeed or fail, or its connection will be abruptly closed. Verify the state")
            status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util('select DatasetName from Metadata.`Dataset` d WHERE d.DataverseName <> "Metadata"')
            self.assertEquals(status, "success", msg="CBAS query failed")
            if cbas_result:
                dataset_created += 1
            else:
                dataset_not_created += 1
        
        self.log.info("Test run summary")  
        self.log.info("Test ran for a total of %d times" % times)      
        self.log.info("Dataset %s was created %d times" % (self.cbas_dataset_name, dataset_created))
        self.log.info("Dataset %s was not created %d times" % (self.cbas_dataset_name,  dataset_not_created))
        

    # def tearDown(self):
    # super(CBASCancelDDL, self).tearDown()
