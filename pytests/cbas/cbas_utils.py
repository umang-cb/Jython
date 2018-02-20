'''
Created on Nov 15, 2017

@author: riteshagarwal
'''
from CbasLib.CBASOperations import CBASHelper
import json
import logger
from remote.remote_util import RemoteMachineShellConnection
from threading import Thread
import threading
# from bucket_utils.bucket_ready_functions import bucket_utils
import time
from couchbase_helper.cluster import Cluster
log = logger.Logger.get_logger()

class cbas_utils():
    def __init__(self, master, cbas_node):
        self.log = logger.Logger.get_logger()
        self.cbas_node = cbas_node
        self.master = master
        self.cbas_helper = CBASHelper(master, cbas_node)
        self.cluster = Cluster() 
#         self.bucket_util = bucket_utils(master)

    def createConn(self, bucket, username=None, password=None):
        self.cbas_helper.createConn(bucket,username, password)
    
    def closeConn(self):
        self.cbas_helper.closeConn()
    
#     def create_default_bucket(self):
#         self.bucket_util.create_default_bucket()
#         self.createConn("default")
    
    def execute_statement_on_cbas_util(self, statement, mode=None, rest=None, timeout=120, client_context_id=None, username=None, password=None):
        """
        Executes a statement on CBAS using the REST API using REST Client
        """
        pretty = "true"
        try:
            log.info("Running query on cbas: %s"%statement)
            response = self.cbas_helper.execute_statement_on_cbas(statement, mode, pretty,
                                                      timeout, client_context_id, username, password)
            if type(response) == str: 
                response = json.loads(response)
            if "errors" in response:
                errors = response["errors"]
                if type(errors) == str:
                    errors = json.loads(errors)
            else:
                errors = None
    
            if "results" in response:
                results = response["results"]
                if type(results) == str:
                    results = json.loads(results)
            else:
                results = None
    
            if "handle" in response:
                handle = response["handle"]
            else:
                handle = None
            
            if "metrics" in response:
                metrics = response["metrics"]
                if type(metrics) == str:
                    metrics = json.loads(metrics)
            else:
                metrics = None
                
            return response["status"], metrics, errors, results, handle
    
        except Exception,e:
            raise Exception(str(e))
        
    def create_bucket_on_cbas(self, cbas_bucket_name, cb_bucket_name,
                              cb_server_ip=None,
                              validate_error_msg=False,
                              username = None, password = None, expected_error=None):
        """
        Creates a bucket on CBAS
        """
        if cb_server_ip:
            cmd_create_bucket = "create bucket " + cbas_bucket_name + " with {\"name\":\"" + cb_bucket_name + "\",\"nodes\":\"" + cb_server_ip + "\"};"
        else:
            '''DP3 doesn't need to specify cb server ip as cbas node is part of the cluster.'''
            cmd_create_bucket = "create bucket " + cbas_bucket_name + " with {\"name\":\"" + cb_bucket_name + "\"};"
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd_create_bucket,username=username, password=password)

        if validate_error_msg:
            return self.validate_error_in_response(status, errors, expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def create_dataset_on_bucket(self, cbas_bucket_name, cbas_dataset_name,
                                 where_field=None, where_value = None,
                                 validate_error_msg=False, username = None,
                                 password = None, expected_error=None):
        """
        Creates a shadow dataset on a CBAS bucket
        """
        cmd_create_dataset = "create shadow dataset {0} on {1};".format(
            cbas_dataset_name, cbas_bucket_name)
        if where_field and where_value:
            cmd_create_dataset = "create shadow dataset {0} on {1} WHERE `{2}`=\"{3}\";".format(
                cbas_dataset_name, cbas_bucket_name, where_field, where_value)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd_create_dataset, username=username, password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors, expected_error)
        else:
            if status != "success":
                return False
            else:
                return True
    
    def create_dataset_on_bucket_merge_policy(self, cbas_bucket_name, cbas_dataset_name,
                                 where_field=None, where_value = None,
                                 validate_error_msg=False, username = None,
                                 password = None, expected_error=None, merge_policy="no-merge",
                                 max_mergable_component_size=16384, max_tolerance_component_count=2
                                 ):
        """
        Creates a shadow dataset on a CBAS bucket
        """
        if merge_policy == "no-merge":
            cmd_create_dataset = 'create shadow dataset %s with { "merge-policy": {"name": "%s"}} on %s;'\
            %(cbas_dataset_name, merge_policy, cbas_bucket_name)
            if where_field and where_value:
                cmd_create_dataset = 'create shadow dataset %s with { "merge-policy": {"name": "%s" }} on %s WHERE `%s`=\"%s\";'%(
                    cbas_dataset_name, merge_policy, cbas_bucket_name, where_field, where_value)
        else:
            cmd_create_dataset = 'create shadow dataset %s with { "merge-policy": {"name": "%s", "parameters": {"max-mergable-component-size": %s, "max-tolerance-component-count": %s}}} on %s;'\
            %(cbas_dataset_name, merge_policy, max_mergable_component_size, max_tolerance_component_count, cbas_bucket_name)
            if where_field and where_value:
                cmd_create_dataset = 'create shadow dataset %s with { "merge-policy": {"name": "%s", "parameters": {"max-mergable-component-size": %s,"max-tolerance-component-count": %s}}} on %s WHERE `%s`=\"%s\";'%(
                                        cbas_dataset_name, merge_policy, 
                                        max_mergable_component_size, max_tolerance_component_count, 
                                        cbas_bucket_name, where_field, where_value)
                                    
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd_create_dataset, username=username, password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors, expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def connect_to_bucket(self, cbas_bucket_name, cb_bucket_password=None,
                          validate_error_msg=False, cb_bucket_username="Administrator",
                          username=None, password=None, expected_error=None):
        """
        Connects to a CBAS bucket
        """
        if cb_bucket_username and cb_bucket_password:
            cmd_connect_bucket = "connect bucket " + cbas_bucket_name + " with {\"username\":\"" + cb_bucket_username + "\",\"password\":\"" + cb_bucket_password + "\"};"
        else:
            '''DP3 doesn't need to specify Username/Password as cbas node is part of the cluster.'''
            cmd_connect_bucket = "connect bucket " + cbas_bucket_name
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd_connect_bucket, username=username, password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors, expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def disconnect_from_bucket(self, cbas_bucket_name,
                               disconnect_if_connected=False,
                               validate_error_msg=False, username=None,
                               password=None, expected_error=None):
        """
        Disconnects from a CBAS bucket
        """
        if disconnect_if_connected:
            cmd_disconnect_bucket = "disconnect bucket {0} if connected;".format(
                cbas_bucket_name)
        else:
            cmd_disconnect_bucket = "disconnect bucket {0};".format(
                cbas_bucket_name)

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd_disconnect_bucket, username=username, password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors, expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def drop_dataset(self, cbas_dataset_name, validate_error_msg=False,
                     username=None, password=None, expected_error=None):
        """
        Drop dataset from CBAS
        """
        cmd_drop_dataset = "drop dataset {0};".format(cbas_dataset_name)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd_drop_dataset, username=username, password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors, expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def drop_cbas_bucket(self, cbas_bucket_name, validate_error_msg=False, username=None, password=None, expected_error=None):
        """
        Drop a CBAS bucket
        """
        cmd_drop_bucket = "drop bucket {0};".format(cbas_bucket_name)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd_drop_bucket, username=username, password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors, expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def wait_for_ingestion_complete(self, cbas_dataset_names, num_items, timeout=300):
        
        total_items = 0
        for ds_name in cbas_dataset_names:
            total_items += self.get_num_items_in_cbas_dataset(ds_name)[0]
        
        counter = 0
        while (timeout > counter):
            self.log.info("Total items in CB Bucket to be ingested in CBAS datasets %s"%num_items)
            if num_items == total_items:
                self.log.info("Data ingestion completed in %s seconds."%counter)
                return True
            else:
                time.sleep(2)
                total_items = 0
                for ds_name in cbas_dataset_names:
                    total_items += self.get_num_items_in_cbas_dataset(ds_name)[0]
                counter += 2
                
        return False

    def get_num_items_in_cbas_dataset(self, dataset_name):
        """
        Gets the count of docs in the cbas dataset
        """
        total_items = -1
        mutated_items = -1
        cmd_get_num_items = "select count(*) from %s;" % dataset_name
        cmd_get_num_mutated_items = "select count(*) from %s where mutated>0;" % dataset_name

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd_get_num_items)
        if status != "success":
            self.log.error("Query failed")
        else:
            self.log.info("No. of items in CBAS dataset {0} : {1}".format(dataset_name,results[0]['$1']))
            total_items = results[0]['$1']

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd_get_num_mutated_items)
        if status != "success":
            self.log.error("Query failed")
        else:
            self.log.info("No. of items mutated in CBAS dataset {0} : {1}".format(dataset_name, results[0]['$1']))
            mutated_items = results[0]['$1']

        return total_items, mutated_items

    def validate_cbas_dataset_items_count(self, dataset_name, expected_count, expected_mutated_count=0):
        """
        Compares the count of CBAS dataset total and mutated items with the expected values.
        """
        count, mutated_count = self.get_num_items_in_cbas_dataset(dataset_name)
        tries = 12
        if expected_mutated_count:
            while (count != expected_count or mutated_count != expected_mutated_count) and tries > 0:
                time.sleep(10)
                count, mutated_count = self.get_num_items_in_cbas_dataset(dataset_name)
                tries -= 1
        else :
            while count != expected_count and tries > 0:
                time.sleep(10)
                count, mutated_count = self.get_num_items_in_cbas_dataset(
                    dataset_name)
                tries -= 1

        self.log.info("Expected Count: %s, Actual Count: %s" % (expected_count, count))
        self.log.info("Expected Mutated Count: %s, Actual Mutated Count: %s" % (expected_mutated_count, mutated_count))

        if count != expected_count:
            return False
        elif mutated_count == expected_mutated_count:
            return True
        else:
            return False

    def delete_request(self, client_context_id, username=None, password=None):
        """
        Deletes a request from CBAS
        """
        try:
            status = self.cbas_helper.delete_active_request_on_cbas(client_context_id,username, password)
            self.log.info (status)
            return status
        except Exception, e:
            raise Exception(str(e))

    def retrieve_request_status_using_handle(self, server, handle, shell = None):
        """
        Retrieves status of a request from /analytics/status endpoint
        """
        if not shell:
            shell = RemoteMachineShellConnection(server)

        output, error = shell.execute_command(
            """curl -v {0} -u {1}:{2}""".format(handle,
                                                self.cbas_node.rest_username,
                                                self.cbas_node.rest_password))

        response = ""
        for line in output:
            response = response + line
        if response:
            response = json.loads(response)
        
        if not shell:
            shell.disconnect()

        status = ""
        handle = ""
        if 'status' in response:
            status = response['status']
        if 'handle' in response:
            handle = response['handle']

        self.log.info("status=%s, handle=%s"%(status,handle))
        return status, handle

    def retrieve_result_using_handle(self, server, handle):
        """
        Retrieves result from the /analytics/results endpoint
        """
        shell = RemoteMachineShellConnection(server)

        output, error = shell.execute_command(
            """curl -v {0} -u {1}:{2}""".format(handle,
                                                self.cbas_node.rest_username,
                                                self.cbas_node.rest_password))

        response = ""
        for line in output:
            response = response + line
        if response:
            response = json.loads(response)
        shell.disconnect()

        return response
    
    def convert_execution_time_into_ms(self, time):
        """
        Converts the execution time into ms
        """
        import re
        match = re.match(r"([0-9]+.[0-9]+)([a-zA-Z]+)", time, re.I)
        if match:
            items = match.groups()

            if items[1] == "s":
                return float(items[0])*1000
            if items[1] == "ms":
                return float(items[0])
            if items[1] == "m":
                return float(items[0])*1000*60
            if items[1] == "h":
                return float(items[0])*1000*60*60
        else:
            return None
        
    def validate_error_in_response(self, status, errors, expected_error):
        """
        Validates if the error message in the response is same as the expected one.
        """
        if status != "success":
            actual_error = errors[0]["msg"]
            if expected_error not in actual_error:
                log.info("Expected: %s, Actual: %s"%(expected_error, actual_error))
                return False
            else:
                return True
        return False

    def async_query_execute(self, statement, mode, num_queries):
        """
        Asynchronously run queries
        """
        self.log.info("Executing %s queries concurrently",num_queries)

        cbas_base_url = "http://{0}:8095/analytics/service".format(
            self.cbas_node.ip)

        pretty = "true"
        tasks = []
        fail_count = 0
        failed_queries = []
        for count in range(0, num_queries):
            tasks.append(self.cluster.async_cbas_query_execute(self.master, self.cbas_node,
                                                                   cbas_base_url,
                                                                   statement,
                                                                   mode,
                                                                   pretty))

        for task in tasks:
            task.get_result()
            if not task.passed:
                fail_count += 1

        if fail_count:
            self.log.info("%s out of %s queries failed!" % (fail_count, num_queries))
        else:
            self.log.info("SUCCESS: %s out of %s queries passed"
                          % (num_queries - fail_count, num_queries))

    def _run_concurrent_queries(self, query, mode, num_queries, rest=None, batch_size = 100):
        self.failed_count = 0
        self.success_count = 0
        self.rejected_count = 0
        self.error_count = 0
        self.cancel_count = 0
        self.timeout_count = 0
        self.handles = []
        self.concurrent_batch_size = batch_size
        # Run queries concurrently
        self.log.info("Running queries concurrently now...")
        threads = []
        if rest:
            self.cbas_util = rest
        for i in range(0, num_queries):
            threads.append(Thread(target=self._run_query,
                                  name="query_thread_{0}".format(i), args=(query,mode,rest)))
        i = 0
        for thread in threads:
            # Send requests in batches, and sleep for 5 seconds before sending another batch of queries.
            i += 1
            if i % self.concurrent_batch_size == 0:
                log.info("submitted {0} queries".format(i))
                time.sleep(5)
            thread.start()
        for thread in threads:
            thread.join()

        self.log.info(
            "%s queries submitted, %s failed, %s passed, %s rejected, %s cancelled, %s timeout" % (
                num_queries, self.failed_count, self.success_count, self.rejected_count, self.cancel_count, self.timeout_count))
        if self.failed_count+self.error_count != 0:
            raise Exception("Queries Failed:%s , Queries Error Out:%s"%(self.failed_count,self.error_count))
        return self.handles
    
    def _run_query(self, query, mode, rest=None, validate_item_count=False, expected_count=0):
        # Execute query (with sleep induced)
        name = threading.currentThread().getName();
        client_context_id = name
        if rest:
            self.cbas_util = rest
        try:
            status, metrics, errors, results, handle = self.execute_statement_on_cbas_util(
                query, mode=mode, rest=rest, timeout=3600,
                client_context_id=client_context_id)
            # Validate if the status of the request is success, and if the count matches num_items
            if mode == "immediate":
                if status == "success":
                    if validate_item_count:
                        if results[0]['$1'] != expected_count:
                            self.log.info("Query result : %s", results[0]['$1'])
                            self.log.info(
                                "********Thread %s : failure**********",
                                name)
                            self.failed_count += 1
                        else:
                            self.log.info(
                                "--------Thread %s : success----------",
                                name)
                            self.success_count += 1
                    else:
                        self.log.info("--------Thread %s : success----------",
                                      name)
                        self.success_count += 1
                else:
                    self.log.info("Status = %s", status)
                    self.log.info("********Thread %s : failure**********", name)
                    self.failed_count += 1

            elif mode == "async":
                if status == "running" and handle:
                    self.log.info("--------Thread %s : success----------", name)
                    print handle
                    self.handles.append(handle)
                    self.success_count += 1
                else:
                    self.log.info("Status = %s", status)
                    self.log.info("********Thread %s : failure**********", name)
                    self.failed_count += 1

            elif mode == "deferred":
                if status == "success" and handle:
                    self.log.info("--------Thread %s : success----------", name)
                    self.handles.append(handle)
                    self.success_count += 1
                else:
                    self.log.info("Status = %s", status)
                    self.log.info("********Thread %s : failure**********", name)
                    self.failed_count += 1
            elif mode == None:
                if status == "success":
                    self.log.info("--------Thread %s : success----------", name)
                    self.success_count += 1
                else:
                    self.log.info("Status = %s", status)
                    self.log.info("********Thread %s : failure**********", name)
                    self.failed_count += 1
                                    
        except Exception, e:
            if str(e) == "Request Rejected":
                self.log.info("Error 503 : Request Rejected")
                self.rejected_count += 1
            elif str(e) == "Request TimeoutException":
                self.log.info("Request TimeoutException")
                self.timeout_count += 1
            elif str(e) == "Request RuntimeException":
                self.log.info("Request RuntimeException")
                self.timeout_count += 1
            elif str(e) == "Request RequestCancelledException":
                self.log.info("Request RequestCancelledException")
                self.cancel_count += 1
            elif str(e) == "CouchbaseException":
                self.log.info("General CouchbaseException")
                self.rejected_count += 1
            elif str(e) == "Capacity cannot meet job requirement":
                self.log.info(
                    "Error 500 : Capacity cannot meet job requirement")
                self.rejected_count += 1
            else:
                self.error_count +=1
                self.log.error(str(e))
                
    def verify_index_created(self, index_name, index_fields, dataset):
        result = True

        statement = "select * from Metadata.`Index` where DatasetName='{0}' and IsPrimary=False".format(
            dataset)
        status, metrics, errors, content, _ = self.execute_statement_on_cbas_util(
            statement)
        if status != "success":
            result = False
            self.log.info("Index not created. Metadata query status = %s",
                          status)
        else:
            index_found = False
            for index in content:
                if index["Index"]["IndexName"] == index_name:
                    index_found = True
                    field_names = []
                    for index_field in index_fields:
                        if "meta()".lower() in index_field:
                            field_names.append(index_field.split(".")[1])
                        else:
                            field_names.append(str(index_field.split(":")[0]))
                            field_names.sort()
                    self.log.info(field_names)
                    actual_field_names = []
                    for index_field in index["Index"]["SearchKey"]:
                        if type(index_field) is list:
                            index_field = ".".join(index_field)
                        actual_field_names.append(str(index_field))
                        actual_field_names.sort()

                    actual_field_names.sort()
                    self.log.info(actual_field_names)
                    if field_names != actual_field_names:
                        result = False
                        self.log.info("Index fields not correct")
                    break
            result &= index_found
        return result, content

    def retrieve_cc_ip(self,shell=None):
        
        if not shell:
            shell = RemoteMachineShellConnection(self.cbas_node)
        url = self.cbas_helper.cbas_base_url + "/analytics/cluster"
        output, error = shell.execute_command(
            """curl -v {0} -u {1}:{2}""".format(url,
                                                self.cbas_node.rest_username,
                                                self.cbas_node.rest_password))

        response = ""
        for line in output:
            response = response + line
        if response:
            response = json.loads(response)
        
        if not shell:
            shell.disconnect()

        ccNodeId = ""
        ccNodeIP = ""
        nodes = None
        ccNodeConfigURL=None
        if 'ccNodeId' in response:
            ccNodeId = response['ccNodeId']
        if 'nodes' in response:
            nodes = response['nodes']
            for node in nodes:
                if node["nodeId"] == ccNodeId:
                    ccNodeConfigURL = node['configUri']
                    ccNodeIP = node['nodeName'][:-5]
                    break
                
        self.log.info("cc_config_urls=%s, ccNodeId=%s, ccNodeIP=%s"%(ccNodeConfigURL,ccNodeId,ccNodeIP))
        return ccNodeIP

    def retrieve_nodes_config(self, only_cc_node_url=True, shell=None):
        """
        Retrieves status of a request from /analytics/status endpoint
        """
        if not shell:
            shell = RemoteMachineShellConnection(self.cbas_node)
        url = self.cbas_helper.cbas_base_url + "/analytics/cluster"
        output, error = shell.execute_command(
            """curl -v {0} -u {1}:{2}""".format(url,
                                                self.cbas_node.rest_username,
                                                self.cbas_node.rest_password))

        response = ""
        for line in output:
            response = response + line
        if response:
            response = json.loads(response)
        
        if not shell:
            shell.disconnect()

        ccNodeId = ""
        nodes = None
        ccNodeConfigURL=None
        if 'ccNodeId' in response:
            ccNodeId = response['ccNodeId']
        if 'nodes' in response:
            nodes = response['nodes']
            for node in nodes:
                if only_cc_node_url and node["nodeId"] == ccNodeId:
                    ccNodeConfigURL = node['configUri']
                    break
                
        self.log.info("cc_config_urls=%s, ccNodeId=%s"%(ccNodeConfigURL,ccNodeId))
        self.log.info("Nodes: %s"%nodes)
        return nodes, ccNodeId, ccNodeConfigURL

    def retrieve_analyticsHttpAdminListen_address_port(self, NodeConfigURL, shell=None):
        """
        Retrieves status of a request from /analytics/status endpoint
        """
        if not shell:
            shell = RemoteMachineShellConnection(self.cbas_node)
        output, error = shell.execute_command(
            """curl -v {0} -u {1}:{2}""".format(NodeConfigURL,
                                                self.cbas_node.rest_username,
                                                self.cbas_node.rest_password))

        response = ""
        for line in output:
            response = response + line
        if response:
            response = json.loads(response)
        
        if not shell:
            shell.disconnect()
            
        analyticsHttpAdminListenAddress = None
        analyticsHttpAdminListenPort = None
        
        if 'analyticsHttpAdminListenAddress' in response:
            analyticsHttpAdminListenAddress = response['analyticsHttpAdminListenAddress']
        if 'analyticsHttpAdminListenPort' in response:
            analyticsHttpAdminListenPort = response['analyticsHttpAdminListenPort']

        return analyticsHttpAdminListenAddress, analyticsHttpAdminListenPort
    
    def retrive_replica_from_storage_data(self, analyticsHttpAdminListenAddress, analyticsHttpAdminListenPort, shell=None):
        url = "http://{0}:{1}/analytics/node/storage".format(analyticsHttpAdminListenAddress,analyticsHttpAdminListenPort)
        
        if not shell:
            shell = RemoteMachineShellConnection(self.cbas_node)
        output, error = shell.execute_command(
            """curl -v {0} -u {1}:{2}""".format(url,
                                                self.cbas_node.rest_username,
                                                self.cbas_node.rest_password))

        response = ""
        for line in output:
            response = response + line
        if response:
            response = json.loads(response)
        self.log.info("Api %s: %s"%(url,response))
        
        if not shell:
            shell.disconnect()    
            
        for partition in response:
            if 'replicas' in partition:
                return partition['replicas']
    
    def get_replicas_info(self,shell=None):
        cc__metadata_replicas_info = []
        start_time = time.time()
        ccNodeId = None
        while not ccNodeId and start_time +60 > time.time():
            nodes,ccNodeId,ccConfigURL = self.retrieve_nodes_config(shell)
        if ccConfigURL:
            address, port = self.retrieve_analyticsHttpAdminListen_address_port(ccConfigURL, shell)
            cc__metadata_replicas_info = self.retrive_replica_from_storage_data(address, port, shell)
        
        return cc__metadata_replicas_info
    
    def get_num_partitions(self,shell=None):
        partitons={}
        nodes,ccNodeId,ccConfigURL = self.retrieve_nodes_config(shell=shell)
        for node in nodes:
            address, port = self.retrieve_analyticsHttpAdminListen_address_port(node['configUri'], shell)
            partitons[node['nodeName']]=self.retrieve_number_of_partitions(address, port, shell)
        
        return partitons
    
    def retrieve_number_of_partitions(self, analyticsHttpAdminListenAddress, analyticsHttpAdminListenPort, shell=None):
        url = "http://{0}:{1}/analytics/node/storage".format(analyticsHttpAdminListenAddress,analyticsHttpAdminListenPort)
        
        if not shell:
            shell = RemoteMachineShellConnection(self.cbas_node)
        output, error = shell.execute_command(
            """curl -v {0} -u {1}:{2}""".format(url,
                                                self.cbas_node.rest_username,
                                                self.cbas_node.rest_password))

        response = ""
        for line in output:
            response = response + line
        if response:
            response = json.loads(response)
        self.log.info("Api %s: %s"%(url,response))
        
        if not shell:
            shell.disconnect()    
            
        return len(response)         