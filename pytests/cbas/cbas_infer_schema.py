from cbas.cbas_infer_base import CBASInferBase
from cbas.cbas_infer_base import CBASInferCompareJson


class CBASInferSchema(CBASInferBase):

    def setUp(self):
        super(CBASInferSchema, self).setUp()
    
    """
    cbas.cbas_infer_schema.CBASInferSchema.verify_analytics_supports_infer_schema,default_bucket=True,cbas_dataset_name=ds,cb_bucket_name=default,items=1000
    """
    def verify_analytics_supports_infer_schema(self):
        
        self.log.info('Create dataset')
        self.cbas_util.createConn(self.cb_bucket_name)
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)
        
        self.log.info('Connect link')
        self.cbas_util.connect_link()
        
        self.log.info('Execute INFER schema with import private function set to False')
        status, _, _, _, _ = self.cbas_util.execute_statement_on_cbas_util('set `import-private-functions` `False`;SELECT Value array_infer_schema(%s)' % self.cbas_dataset_name)
        self.assertTrue(status == "fatal", msg='Infer schema must fail if import private function is set to False')
        
        self.log.info('Execute INFER schema on an empty dataset')
        status, _, _, _, _ = self.cbas_util.execute_statement_on_cbas_util('set `import-private-functions` `True`;SELECT Value array_infer_schema(%s)' % self.cbas_dataset_name)
        self.assertTrue(status == "fatal", msg='Infer schema must fail if dataset is empty')
        
        self.log.info('Execute INFER schema on single value input')
        status, _, _, _, _ = self.cbas_util.execute_statement_on_cbas_util('set `import-private-functions` `True`;SELECT Value array_infer_schema({"a":1})')
        self.assertTrue(status == "fatal", msg='Infer schema works only on array')
        
        self.log.info('Load documents in default bucket')
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items, batch_size=200)
        
        self.log.info('Execute INFER schema on dataset with documents')
        status, _, _, _, _ = self.cbas_util.execute_statement_on_cbas_util('set `import-private-functions` `True`;SELECT Value array_infer_schema(%s)' % self.cbas_dataset_name)
        self.assertTrue(status == "success", msg='Failed to run infer schema')

    def verify_infer_schema_on_large_dataset_compare_n1ql(self):
        
        self.log.info('Create dataset')
        self.cbas_util.createConn(self.cb_bucket_name)
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)

        self.log.info('Connect link')
        self.cbas_util.connect_link()

        self.log.info('Load documents in default bucket')
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        
        self.log.info('Load more documents with same json property but different property value type')
        gen_load = self.generate_document_with_type_array_and_object()
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items, gen_load=gen_load)
        
        self.log.info('Create primary index on %s' % self.cb_bucket_name) 
        self.rest.query_tool('CREATE PRIMARY INDEX idx on %s' % self.cb_bucket_name)
        
        self.log.info('Fetch INFER response from N1QL')
        result_n1ql = self.rest.query_tool('INFER default')['results']
        infer_dictionary_n1ql = result_n1ql[0][0]
        property_dictionary_n1ql = infer_dictionary_n1ql['properties']
        document_properties_n1ql = sorted(property_dictionary_n1ql.keys())
        
        self.log.info('Execute INFER schema on dataset with documents')
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util('set `import-private-functions` `True`;SELECT Value array_infer_schema(%s)' % self.cbas_dataset_name)
        self.assertTrue(status == "success", msg='Failed to run infer schema')
        
        self.log.info('Verify INFER response')
        infer_dictionary = result[0][0]
        property_dictionary = infer_dictionary['properties']
        document_properties = sorted(property_dictionary.keys())
        
        self.log.info('Verify total document count in INFER response')
        self.assertEqual(infer_dictionary['#docs'], self.num_items * 2, msg='Document count incorrect')
        
        self.log.info('Verify document properties in INFER response')
        self.assertEqual(document_properties_n1ql, document_properties, msg='Properties mismatch')
        
        for document_field in document_properties:
            self.log.info('Verify `%s` field count in INFER response' % document_field)
            document_field_query = 'select count(*) from `%s` where `%s` is not null' %(self.cb_bucket_name, document_field)
            count_n1ql = self.rest.query_tool(document_field_query)['results'][0]['$1']
            self.assertEqual(property_dictionary[document_field]['#docs'], count_n1ql, msg='Count mismatch for %s' % document_field)
        
    def verify_infer_schema_on_array(self):
        single_array_query = 'set `import-private-functions` `true`;SELECT Value array_infer_schema([{"a":1},{"a":"aval"},{"a":[1,2]},{"a":{"b":1,"c":"aval","d":"[1,2]","e":{"f":1}}}])'
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(single_array_query)
        self.assertTrue(status == "success", msg='Failed to run infer schema')

        self.log.info('Compare JSON results')
        response = result[0]
        self.remove_samples_from_result(response)
        compare_responses = CBASInferCompareJson.get_json('infer_schema_on_array')
        for index, value in enumerate(compare_responses):
            self.assertEqual(compare_responses[index], response[index])
    
    def verify_infer_schema_on_dataset(self):
        self.log.info('Load beer-sample bucket')
        self.load_sample_buckets(servers=[self.master], bucketName=self.cb_bucket_name, total_items=self.beer_sample_docs_count)
        self.cbas_util.createConn(self.cb_bucket_name)
        
        self.log.info('Create dataset')
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)
        
        self.log.info('Execute INFER schema on dataset with documents')
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util('set `import-private-functions` `True`;SELECT Value array_infer_schema(%s)' % self.cbas_dataset_name)
        self.assertTrue(status == "success", msg='Failed to run infer schema')
            
    def tearDown(self):
        super(CBASInferSchema, self).tearDown()
