"""
Created on 28-Mar-2018

@author: tanzeem
"""
from cbas.cbas_base import CBASBaseTest
from couchbase_helper.documentgenerator import DocumentGenerator
from membase.helper.cluster_helper import ClusterOperationHelper


class CBASBacklogIngestion(CBASBaseTest):

    def setUp(self):
        super(CBASBacklogIngestion, self).setUp()

    @staticmethod
    def generate_documents(start_at, end_at, role=None):
        age = range(70)
        first = ['james', 'sharon', 'dave', 'bill', 'mike', 'steve']
        if role is None:
            profession = ['teacher']
        else:
            profession = role
        template = '{{ "number": {0}, "first_name": "{1}" , "profession":"{2}", "mutated":0}}'
        documents = DocumentGenerator('test_docs', template, age, first, profession, start=start_at, end=end_at)
        return documents

    '''
    -i b/resources/4-nodes-template.ini -t cbas.cbas_backlog_ingestion.CBASBacklogIngestion.test_document_expiry_with_overlapping_filters_between_datasets,default_bucket=True,items=10000,cb_bucket_name=default,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,where_field=profession,where_value=teacher,batch_size=10000
    -i b/resources/4-nodes-template.ini -t cbas.cbas_backlog_ingestion.CBASBacklogIngestion.test_document_expiry_with_overlapping_filters_between_datasets,default_bucket=True,items=10000,cb_bucket_name=default,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,where_field=profession,where_value=teacher,batch_size=10000,secondary_index=True,index_fields=profession:string
    '''

    def test_document_expiry_with_overlapping_filters_between_datasets(self):
        """
        1. Create default bucket
        2. Load data with profession doctor and lawyer
        3. Load data with profession teacher
        4. Create dataset with no filters
        5. Create filter dataset that holds data with profession teacher
        6. Verify the dataset count
        7. Delete half the documents with profession teacher
        8. Verify the updated dataset count
        9. Expire data with profession teacher
        10. Verify the updated dataset count post expiry
        """
        self.log.info("Set expiry pager on default bucket")
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 1, bucket="default")

        self.log.info("Load data in the default bucket")
        num_items = self.input.param("items", 10000)
        batch_size = self.input.param("batch_size", 10000)
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, num_items, exp=0, batch_size=batch_size)

        self.log.info("Load data in the default bucket, with documents containing profession teacher")
        load_gen = CBASBacklogIngestion.generate_documents(num_items, num_items * 2, role=['teacher'])
        self._async_load_all_buckets(server=self.master, kv_gen=load_gen, op_type="create", exp=0, batch_size=batch_size)

        self.log.info("Create primary index")
        query = "CREATE PRIMARY INDEX ON {0} using gsi".format(self.buckets[0].name)
        self.rest.query_tool(query)

        self.log.info("Create a connection")
        cb_bucket_name = self.input.param("cb_bucket_name")
        self.cbas_util.createConn(cb_bucket_name)

        self.log.info("Create a CBAS bucket")
        cbas_bucket_name = self.input.param("cbas_bucket_name")
        self.cbas_util.create_bucket_on_cbas(cbas_bucket_name=cbas_bucket_name,
                                             cb_bucket_name=cb_bucket_name)

        self.log.info("Create a default data-set")
        cbas_dataset_name = self.input.param("cbas_dataset_name")
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=cbas_bucket_name,
                                                cbas_dataset_name=cbas_dataset_name)

        self.log.info("Read input params for field name and value")
        field = self.input.param("where_field", "")
        value = self.input.param("where_value", "")
        cbas_dataset_with_clause = cbas_dataset_name + "_" + value

        self.log.info("Create data-set with profession teacher")
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=cbas_bucket_name,
                                                cbas_dataset_name=cbas_dataset_with_clause,
                                                where_field=field, where_value=value)

        secondary_index = self.input.param("secondary_index", False)
        datasets = [cbas_dataset_name, cbas_dataset_with_clause]
        index_fields = self.input.param("index_fields", None)
        if secondary_index:
            self.log.info("Create secondary index")
            index_fields = ""
            for index_field in self.index_fields:
                index_fields += index_field + ","
                index_fields = index_fields[:-1]
            for dataset in datasets:
                create_idx_statement = "create index {0} on {1}({2});".format(
                    dataset + "_idx", dataset, index_fields)
                status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
                    create_idx_statement)

                self.assertTrue(status == "success", "Create Index query failed")
                self.assertTrue(self.cbas_util.verify_index_created(dataset + "_idx", self.index_fields,
                                                                    dataset)[0])

        self.log.info("Connect to CBAS bucket")
        self.cbas_util.connect_to_bucket(cbas_bucket_name=cbas_bucket_name,
                                         cb_bucket_password=self.cb_bucket_password)

        self.log.info("Wait for ingestion to complete on both data-sets")
        self.cbas_util.wait_for_ingestion_complete([cbas_dataset_name], num_items * 2)
        self.cbas_util.wait_for_ingestion_complete([cbas_dataset_with_clause], num_items)

        self.log.info("Validate count on data-set")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(cbas_dataset_name, num_items * 2))
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(cbas_dataset_with_clause, num_items))

        self.log.info("Delete half of the teacher records")
        self.perform_doc_ops_in_all_cb_buckets(num_items // 2, "delete", num_items + (num_items // 2), num_items * 2)

        self.log.info("Wait for ingestion to complete")
        self.cbas_util.wait_for_ingestion_complete([cbas_dataset_name], num_items + (num_items // 2))
        self.cbas_util.wait_for_ingestion_complete([cbas_dataset_with_clause], num_items // 2)

        self.log.info("Validate count on data-set")
        self.assertTrue(
            self.cbas_util.validate_cbas_dataset_items_count(cbas_dataset_name, num_items + (num_items // 2)))
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(cbas_dataset_with_clause, num_items // 2))

        self.log.info("Update the documents with profession teacher to expire in next 1 seconds")
        self.perform_doc_ops_in_all_cb_buckets(num_items // 2, "update", num_items, num_items + (num_items // 2), exp=1)
        
        self.log.info("Wait for documents to expire")
        self.sleep(15, message="Waiting for documents to expire")
        
        self.log.info("Wait for ingestion to complete")
        self.cbas_util.wait_for_ingestion_complete([cbas_dataset_name], num_items)
        self.cbas_util.wait_for_ingestion_complete([cbas_dataset_with_clause], 0)

        self.log.info("Validate count on data-set")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(cbas_dataset_name, num_items))
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(cbas_dataset_with_clause, 0))

    '''
    -i b/resources/4-nodes-template.ini -t cbas.cbas_backlog_ingestion.CBASBacklogIngestion.test_multiple_cbas_bucket_with_overlapping_filters_between_datasets,default_bucket=True,
    cb_bucket_name=default,cbas_bucket_name=default_cbas_,num_of_cbas_buckets=4,items=10000,cbas_dataset_name=default_ds_,where_field=profession,join_operator=or,batch_size=10000
    '''

    def test_multiple_cbas_bucket_with_overlapping_filters_between_datasets(self):
        """
        1. Create default bucket
        2. Load data in default bucket with professions picked from the predefined list
        3. Create CBAS bucket and a dataset in each CBAS bucket such that dataset between the cbas buckets have overlapping filter
        4. Verify dataset count
        """
        self.log.info("Load data in the default bucket")
        num_of_cbas_buckets = self.input.param("num_of_cbas_buckets", 4)
        batch_size = self.input.param("batch_size", 10000)
        cbas_bucket_name = self.input.param("cbas_bucket_name", "default_cbas_")
        professions = ['teacher', 'doctor', 'engineer', 'dentist', 'racer', 'dancer', 'singer', 'musician', 'pilot',
                       'finance']
        load_gen = CBASBacklogIngestion.generate_documents(0, self.num_items, role=professions[:num_of_cbas_buckets])
        self._async_load_all_buckets(server=self.master, kv_gen=load_gen, op_type="create", exp=0, batch_size=batch_size)

        self.log.info("Create primary index")
        query = "CREATE PRIMARY INDEX ON {0} using gsi".format(self.buckets[0].name)
        self.rest.query_tool(query)

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Create CBAS buckets")
        num_of_cbas_buckets = self.input.param("num_of_cbas_buckets", 2)
        for index in range(num_of_cbas_buckets):
            self.assertTrue(self.cbas_util.create_bucket_on_cbas(cbas_bucket_name=cbas_bucket_name + str(index),
                                                                 cb_bucket_name=self.cb_bucket_name),
                            "Failed to create cbas bucket " + self.cbas_bucket_name + str(index))

        self.log.info("Create data-sets")
        field = self.input.param("where_field", "")
        join_operator = self.input.param("join_operator", "or")
        for index in range(num_of_cbas_buckets):
            tmp = "\"" + professions[index] + "\" "
            join_values = professions[index + 1:index + 2]
            for join_value in join_values:
                tmp += join_operator + " `" + field + "`=\"" + join_value + "\""
            tmp = tmp[1:-1]
            self.assertTrue(self.cbas_util.create_dataset_on_bucket((self.cbas_bucket_name + str(index)),
                                                                    cbas_dataset_name=(self.cbas_dataset_name + str(
                                                                        index)),
                                                                    where_field=field, where_value=tmp))

        self.log.info("Connect to CBAS bucket")
        for index in range(num_of_cbas_buckets):
            self.cbas_util.connect_to_bucket(cbas_bucket_name=cbas_bucket_name + str(index),
                                             cb_bucket_password=self.cb_bucket_password)

        self.log.info("Wait for ingestion to completed and assert count")
        for index in range(num_of_cbas_buckets):
            n1ql_query = 'select count(*) from `{0}` where {1} = "{2}"'.format(self.cb_bucket_name, field,
                                                                               professions[index])
            tmp = " "
            join_values = professions[index + 1:index + 2]
            for join_value in join_values:
                tmp += join_operator + " `" + field + "`=\"" + join_value + "\""
            n1ql_query += tmp
            count_n1ql = self.rest.query_tool(n1ql_query)['results'][0]['$1']
            self.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name + str(index)], count_n1ql)
            _, _, _, results, _ = self.cbas_util.execute_statement_on_cbas_util(
                'select count(*) from `%s`' % (self.cbas_dataset_name + str(index)))
            count_ds = results[0]["$1"]
            self.assertEqual(count_ds, count_n1ql, msg="result count mismatch between N1QL and Analytics")
