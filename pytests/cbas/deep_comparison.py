from cbas.cbas_base import CBASBaseTest


class DeepComparison(CBASBaseTest):
    
    def setUp(self):
        super(DeepComparison, self).setUp()
    
    """
    cbas.deep_comparison.DeepComparison.test_deep_comparision_on_array,default_bucket=False
    """
    def test_deep_comparision_on_array(self):
        
        self.log.info('Execute query on SQL++')
        sql_query = '[1,2,3] = [1,2,3]'
        sql_status, _, _, sql_result, _ = self.cbas_util.execute_statement_on_cbas_util(sql_query)
        self.assertEqual(sql_status, "success", "SQL++ Query %s failed. Actual: %s, Expected:%s" % (sql_query, sql_status, 'success'))
        
        self.log.info('Execute query on N1QL')
        n1ql_query = 'select value ' + sql_query
        n1ql_response = self.rest.query_tool(n1ql_query)
        n1ql_status = n1ql_response['status']
        n1ql_result = n1ql_response['results']
        self.assertEqual(n1ql_status, "success", "Query %s failed. Actual: %s, Expected:%s" % (n1ql_query, n1ql_status, 'success'))
        
        self.log.info('Compare n1ql and sql++ query result')
        self.assertEqual(n1ql_result, sql_result, "Result mismatch. Actual: %s, Expected:%s" % (n1ql_result, sql_result))
    
    """
    cbas.deep_comparison.DeepComparison.test_deep_comparision_on_array,default_bucket=False
    """
    def test_deep_comparision_on_objects(self):
        
        self.log.info('Execute query on SQL++')
        sql_query = '{"name":"Alex", "country":"Sweden", "age":31} = {"age":31, "name":"Alex", "country":"Sweden"}'
        sql_status, _, _, sql_result, _ = self.cbas_util.execute_statement_on_cbas_util(sql_query)
        self.assertEqual(sql_status, "success", "SQL++ Query %s failed. Actual: %s, Expected:%s" % (sql_query, sql_status, 'success'))
        
        self.log.info('Execute query on N1QL')
        n1ql_query = 'select value ' + sql_query
        n1ql_response = self.rest.query_tool(n1ql_query)
        n1ql_status = n1ql_response['status']
        n1ql_result = n1ql_response['results']
        self.assertEqual(n1ql_status, "success", "Query %s failed. Actual: %s, Expected:%s" % (n1ql_query, n1ql_status, 'success'))
        
        self.log.info('Compare n1ql and sql++ query result')
        self.assertEqual(n1ql_result, sql_result, "Result mismatch. Actual: %s, Expected:%s" % (n1ql_result, sql_result))
    
    """
    cbas.deep_comparison.DeepComparison.test_deep_comparision_where_clause_object,default_bucket=False,cb_bucket_name=beer-sample,cbas_dataset_name=ds
    """
    def test_deep_comparision_where_clause_object(self):
        
        self.log.info("Load Beer-Sample bucket")
        self.assertTrue(self.load_sample_buckets(servers=[self.master], bucketName=self.cb_bucket_name, total_items=self.beer_sample_docs_count), msg="Failed to load Beer-Sample, item count mismatch")

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)
        
        self.log.info('Create dataset')
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)
        
        self.log.info('Connect link')
        self.cbas_util.connect_link()
        
        self.log.info('Verify dataset count')
        self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.beer_sample_docs_count)
        
        self.log.info('Execute query on SQL++')
        sql_query = 'SELECT meta().id FROM %s WHERE geo = { "accuracy": "ROOFTOP", "lat": 37.7825, "lon": -122.393 }' % self.cbas_dataset_name
        sql_status, _, _, sql_result, _ = self.cbas_util.execute_statement_on_cbas_util(sql_query)
        self.assertEqual(sql_status, "success", "SQL++ Query %s failed. Actual: %s, Expected:%s" % (sql_query, sql_status, 'success'))
        
        self.log.info('Execute query on N1QL')
        n1ql_query = 'SELECT meta().id FROM `%s` WHERE geo = { "accuracy": "ROOFTOP", "lat": 37.7825, "lon": -122.393 }' % self.cb_bucket_name
        n1ql_response = self.rest.query_tool(n1ql_query)
        n1ql_status = n1ql_response['status']
        n1ql_result = n1ql_response['results']
        self.assertEqual(n1ql_status, "success", "Query %s failed. Actual: %s, Expected:%s" % (n1ql_query, n1ql_status, 'success'))
        
        self.log.info('Compare n1ql and sql++ query result')
        self.assertEqual(n1ql_result, sql_result, "Result mismatch. Actual: %s, Expected:%s" % (n1ql_result, sql_result))
        
    def tearDown(self):
        super(DeepComparison, self).tearDown()
