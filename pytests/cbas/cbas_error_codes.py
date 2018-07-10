from Rbac_utils.Rbac_ready_functions import rbac_utils
from cbas.cbas_base import CBASBaseTest


class CBASError:
    errors = [
        {
            "id": "query_timeout",
            "msg": "Query timed out and will be cancelled",
            "code": 25000,
            "query": "select sleep(count(*), 2000) from ds"
        },
        {
            "id": "drop_shadow_when_cbas_bucket_connected",
            "msg": "Can't drop shadow dataset because its bucket is in the connected state",
            "code": 25000,
            "query": "drop dataset ds"
        },
        {
            "id": "create_index_with_cbas_bucket_connected",
            "msg": "Dataset Default.ds is currently being fed into by the following active entities.\nDefault.Local.default(CouchbaseMetadataExtension)",
            "code": 25000,
            "query": "create index sec_idx on ds(name:string)"
        },
        {
            "id": "create_index_with_index_name_already_exist",
            "msg": "An index with this name sec_idx already exists",
            "code": 25000,
            "query": "create index sec_idx on ds(name:string)"
        },
        {
            "id": "user_permission",
            "msg": "User must have permission (cluster.bucket[default].analytics!manage)",
            "code": 20001,
            "query": "drop dataset ds"
        },
        {
            "id": "create_shadow_when_cbas_bucket_connected",
            "msg": "Dataset cannot be created because the bucket default is connected",
            "code": 25000,
            "query": "create dataset ds1 on default"
        },
        {
            "id": "user_unauthorized",
            "msg": "Unauthorized user",
            "code": 20000,
            "query": "select count(*) from ds"
        },
        {
            "id": "index_on_unsupported_type",
            "msg": "Cannot index field [click] on type date. Supported types: bigint, double, string",
            "code": 25000,
            "query": "create index idx on ds(click:date)"
        },
        {
            "id": "cb_bucket_does_not_exist",
            "msg": "Bucket (default1) does not exist",
            "code": 25000,
            "query": "create dataset ds1 on default1"
        },
        {   
            "id": "max_writable_datasets",
            "msg": "Maximum number of active writable datasets (8) exceeded",
            "code": 25000,
            "query": "connect link Local"
        },
        {   
            "id": "incorrect_aggregate_query",
            "msg": "count is a SQL-92 aggregate function. The SQL++ core aggregate function array_count could potentially express the intent",
            "code": 25000,
            "query": "select count(*) ds1"
        },
        {
            "id": "unstable_cbas_node",
            "msg": "Cannot execute request, cluster is RECOVERING",
            "code": 25000,
            "query": "select count(*) from ds"
        }
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

        self.log.info("Read input parmas to initialize error object")
        self.error_id = self.input.param('error_id', None)
        self.error_response = CBASError(self.error_id).get_error()
        self.log.info("Test to validate error response \n %s" % self.error_response)

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Create dataset on the CBAS")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)

        self.log.info("Connect to Local link")
        self.cbas_util.connect_to_bucket()

    def validate_error_response(self, status, errors, expected_error, expected_error_code):
        self.assertTrue(self.cbas_util.validate_error_in_response(status, errors, expected_error, expected_error_code), msg="Error msg or Error code mismatch. Refer logs for actual and expected")
    
    """
    cbas.cbas_error_codes.CBASErrorValidator.test_error_response_for_analytics_timeout,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=query_timeout
    """
    def test_error_response_for_analytics_timeout(self):
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"], analytics_timeout=1)
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])
    
    """
    test_error_response_for_error_id,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=<passed from conf file>
    """   
    def test_error_response_for_error_id(self):
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"])
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])
        
    """
    test_error_response_create_index_with_index_name_already_exist,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=create_index_with_index_name_already_exist
    """
    def test_error_response_create_index_with_index_name_already_exist(self):
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
        self.log.info("Create a user with analytics reader role")
        rbac_util = rbac_utils(self.master)
        rbac_util._create_user_and_grant_role("reader_admin", "analytics_reader")
        
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"], username="reader_admin", password="password")
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])
    
    """
    test_error_response_user_unauthorized,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=user_unauthorized
    """
    def test_error_response_user_unauthorized(self):
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"], password="pass")
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])
    
    """
    test_error_response_max_writable_dataset_exceeded,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,error_id=max_writable_datasets
    """
    def test_error_response_max_writable_dataset_exceeded(self):
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
        
        status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.error_response["query"])
        self.validate_error_response(status, errors, self.error_response["msg"], self.error_response["code"])
           
    def tearDown(self):
        super(CBASErrorValidator, self).tearDown()
