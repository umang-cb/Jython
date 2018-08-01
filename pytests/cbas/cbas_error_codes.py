from Rbac_utils.Rbac_ready_functions import rbac_utils
from cbas.cbas_base import CBASBaseTest
from remote.remote_util import RemoteMachineShellConnection


class CBASError:
    errors = [
        # Error codes starting with 2XXXX
        {
            "id": "user_unauthorized",
            "msg": ["Unauthorized user"],
            "code": 20000,
            "query": "select count(*) from ds"
        },
        {
            "id": "user_permission",
            "msg": ["User must have permission (cluster.bucket[default].analytics!manage)"],
            "code": 20001,
            "query": "drop dataset ds"
        },
       
        
        
        # Error codes starting with 21XXX
        {
            "id": "invalid_duration",
            "msg": ['Invalid duration "tos"'],
            "code": 21000,
            "query": "select sleep(count(*), 2000) from ds"
        },{
            "id": "unknown_duration",
            "msg": ['Unknown duration unit M'],
            "code": 21001,
            "query": "select sleep(count(*), 2000) from ds"
        },
        {
            "id": "request_timeout",
            "msg": ["Request timed out and will be cancelled"],
            "code": 21002,
            "query": "select sleep(count(*), 2000) from ds"
        },
        {
            "id": "unsupported_multiple_statements",
            "msg": ["Unsupported multiple statements."],
            "code": 21003,
            "query": "create dataset ds1 on default;connect link Local"
        },
  
        
        
        # Error codes starting with 22XXX
        {
            "id": "bucket_uuid_change",
            "msg": ["Connect link failed", "Default.Local.default", "Bucket UUID has changed"],
            "code": 22001,
            "query": "connect link Local"
        },
        {
            "id": "connect_link_fail",
            "msg": ["Connect link failed", "Default.Local.default", "Bucket (default) does not exist"],
            "code": 22001,
            "query": "connect link Local"
        },
        {   
            "id": "max_writable_datasets",
            "msg": ["Maximum number of active writable datasets (8) exceeded"],
            "code": 22001,
            "query": "connect link Local"
        },

        
        
        # Error codes starting with 23XXX
        {
            "id": "dataverse_drop_link_connected",
            "msg": ["Dataverse Default cannot be dropped while link Local is connected"],
            "code": 23005,
            "query": "drop dataverse custom"
        },
        {
            "id": "create_dataset_link_connected",
            "msg": ["Dataset cannot be created because the bucket default is connected"],
            "code": 23006,
            "query": "create dataset ds1 on default"
        },
        {
            "id": "drop_dataset_link_connected",
            "msg": ["Dataset cannot be dropped because the bucket", "is connected", '"bucket" : "default"'],
            "code": 23022,
            "query": "drop dataset ds"
        },


        
        
        # Error codes starting with 24XXX
        {
            "id": "syntax_error",
            "msg": ["Syntax error:", "select count(*) for ds1;", "Encountered \"for\""],
            "code": 24000,
            "query": "select count(*) for ds1"
        },
        {   
            "id": "compilation_error",
            "msg": ["Compilation error: count is a SQL-92 aggregate function. The SQL++ core aggregate function array_count could potentially express the intent"],
            "code": 24001,
            "query": "select count(*) ds1"
        },
        {
            "id": "index_on_type",
            "msg": ["Cannot index field [click] on type date. Supported types: bigint, double, string"],
            "code": 24002,
            "query": "create index idx on ds(click:date)"
        },
        {
            "id": "cb_bucket_does_not_exist",
            "msg": ["Bucket (default1) does not exist"],
            "code": 24003,
            "query": "create dataset ds1 on default1"
        },
        {   
            "id": "create_dataset_that_exist",
            "msg": ["A dataset with name ds already exists"],
            "code": 24005,
            "query": "create dataset ds on default"
        },
        {   
            "id": "drop_local_not_exist",
            "msg": ["Link Default.Local1 does not exist"],
            "code": 24006,
            "query": "drop link Local1"
        },
        {   
            "id": "drop_local_link",
            "msg": ["Local link cannot be dropped"],
            "code": 24007,
            "query": "drop link Local"
        },
        {
            "id": "type_mismatch",
            "msg": ["Type mismatch: function contains expects its 2nd input parameter to be of type string, but the actual input type is bigint"],
            "code": 24011,
            "query": 'SELECT CONTAINS("N1QL is awesome", 123) as n1ql'
        },
        {
            "id": "dataverse_not_found",
            "msg": ["Cannot find dataverse with name custom"],
            "code": 24034,
            "query": 'use custom;'
        },
        {
            "id": "dataset_not_found",
            "msg": ["Cannot find dataset ds1 in dataverse Default nor an alias with name ds1!"],
            "code": 24045,
            "query": 'select * from ds1'
        },
        {
            "id": "index_name_already_exist",
            "msg": ["An index with this name sec_idx already exists"],
            "code": 24048,
            "query": "create index sec_idx on ds(name:string)"
        },
        
        
        
        # Error codes starting with 25XXX
        
    ]

    def __init__(self, error_id):
        self.error_id = error_id

    def get_error(self):
        for error in self.errors:
            if error['id'] == self.error_id:
                return error
        return None


class CBASErrorValidator(CBASBaseTest):

    def setUp(self):
        super(CBASErrorValidator, self).setUp()

        self.log.info("Read input param")
        self.error_id = self.input.param('error_id', None)
        self.error_response = CBASError(self.error_id).get_error()
        self.log.info("Test to validate error response :\ %s" % self.error_response)

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

    def create_dataset_connect_link(self):
        self.log.info("Create dataset on the CBAS")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)
        
        self.log.info("Connect to Local link")
        self.cbas_util.connect_link()

    def validate_error_response(self, status, errors, expected_errors, expected_error_code):
        if errors is None:
            self.fail("Query did not fail. No error code and message to validate")
        for expected_error in expected_errors:
            self.assertTrue(self.cbas_util.validate_error_in_response(status, errors, expected_error, expected_error_code), msg="Mismatch. Refer logs for actual and expected error code/msg")
    
    """
    cbas.cbas_error_codes.CBASErrorValidator.test_error_response_for_error_id,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=<passed from conf file>
    """   
    def test_error_response_for_error_id(self):
        
        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()
        
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"])
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])
       
    """
    test_error_response_for_analytics_timeout,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=query_timeout
    """
    def test_error_response_for_analytics_timeout(self):
        
        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()
        
        self.log.info("Read time unit and time out value from test params")
        time_out = self.input.param('time_out', 1)
        time_unit = self.input.param('time_unit', "s")
        
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"], analytics_timeout=time_out, time_out_unit=time_unit)
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])
        
    """
    test_error_response_create_index_with_index_name_already_exist,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=create_index_with_index_name_already_exist
    """
    def test_error_response_create_index_with_index_name_already_exist(self):
        
        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()
        
        self.log.info("Disconnect Local link")
        self.assertTrue(self.cbas_util.disconnect_from_bucket(), msg="Failed to disconnect connected bucket")
        
        self.log.info("Create a secondary index")
        self.assertTrue(self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"]), msg="Failed to create secondary index")
        
        self.log.info("Verify creating a secondary index fails with expected error codes")
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"])
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])
    
    """
    test_error_response_user_permissions,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=user_permission
    """
    def test_error_response_user_permissions(self):
        
        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()
        
        self.log.info("Create a user with analytics reader role")
        rbac_util = rbac_utils(self.master)
        rbac_util._create_user_and_grant_role("reader_admin", "analytics_reader")
        
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"], username="reader_admin", password="password")
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])
    
    """
    test_error_response_user_unauthorized,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=user_unauthorized
    """
    def test_error_response_user_unauthorized(self):
        
        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()
        
        self.log.info("Create remote connection and execute cbas query using curl")
        cbas_url = "http://{0}:{1}/analytics/service".format(self.cbas_node.ip, 8095)
        shell = RemoteMachineShellConnection(self.cbas_node)
        output, _ = shell.execute_command("curl -X POST {0} -u {1}:{2}".format(cbas_url, "Administrator", "pass"))
        
        self.assertTrue(self.error_response["msg"][0] in output[3], msg="Error message mismatch")
        self.assertTrue(str(self.error_response["code"]) in output[2], msg="Error code mismatch")
    
    """
    test_error_response_max_writable_dataset_exceeded,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=max_writable_datasets
    """
    def test_error_response_max_writable_dataset_exceeded(self):
        
        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()
        
        self.log.info("Disconnect Local link")
        self.assertTrue(self.cbas_util.disconnect_from_bucket(), msg="Failed to disconnect Local link")
        
        self.log.info("Create 8 more datasets on CBAS bucket")
        for i in range(1, 9):
            self.assertTrue(self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name + str(i)), msg="Create dataset %s failed" % self.cbas_dataset_name + str(i))
        
        self.log.info("Connect back Local link and verify error response for max dataset exceeded")
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"])
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])
    
    """
    test_error_response_for_cbas_node_unstable,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=unstable_cbas_node
    """
    def test_error_response_for_cbas_node_unstable(self):
        
        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()
        
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"])
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])
    
    """
    test_error_response_for_bucket_uuid_change,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=bucket_uuid_change
    """
    def test_error_response_for_bucket_uuid_change(self):
        
        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()
        
        self.log.info("Disconnect link")
        self.cbas_util.disconnect_link()
        
        self.log.info("Delete KV bucket")
        self.delete_bucket_or_assert(serverInfo=self.master)
        
        self.log.info("Recreate KV bucket")
        self.create_default_bucket()
        
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"])
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])
    
    """
    test_error_response_for_connect_link_failed,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=connect_link_fail
    """
    def test_error_response_for_connect_link_failed(self):
        
        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()
        
        self.log.info("Delete KV bucket")
        self.delete_bucket_or_assert(serverInfo=self.master)
        
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"])
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])

    """
    test_error_response_drop_dataverse_link_connected,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=dataverse_drop_link_connected
    """
    def test_error_response_drop_dataverse_link_connected(self):

        self.log.info("Create dataverse")
        status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("create dataverse custom")
        self.assertEquals(status, "success", msg="Create dataverse query failed")

        self.log.info("Use dataverse")
        status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("use custom")
        self.assertEquals(status, "success", msg="Use dataverse query failed")

        self.log.info("Create dataset and connect link")
        self.create_dataset_connect_link()

        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"])
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])
           
    def tearDown(self):
        super(CBASErrorValidator, self).tearDown()
