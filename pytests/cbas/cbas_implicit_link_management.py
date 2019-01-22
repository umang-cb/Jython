import json
from cbas.cbas_base import CBASBaseTest

class CBASImplicitLinkManagement(CBASBaseTest):

    def setUp(self):
        super(CBASImplicitLinkManagement, self).setUp()
    
    def verify_bucket_state(self, num_of_buckets, bucket_name, bucket_state):
        status, content, _ = self.cbas_util.fetch_bucket_state_on_cbas()
        self.assertTrue(status, msg='Fetch bucket state failed')
        content = json.loads(content)
        self.assertEqual(len(content['buckets']), num_of_buckets, msg='Bucket length incorrect')
        for index in range(num_of_buckets):
            self.assertEqual(content['buckets'][index]['name'], bucket_name, msg='Dataset name incorrect')
            self.assertEqual(content['buckets'][index]['state'], bucket_state, msg='Bucket state incorrect')

    def create_sec_index(self, index_name, dataset_name, index_field):
        create_idx_statement = 'create index {0} if not exists on {1}({2})'.format(index_name, dataset_name, index_field)
        status, _, _, _, _ = self.cbas_util.execute_statement_on_cbas_util(create_idx_statement)
        self.assertTrue(status == 'success', 'Create Index query failed')

    '''
    verify_ddl_statements_can_be_executed_with_link_connected,default_bucket=True,items=10000,index_name=idx,cbas_dataset_name=ds,cb_bucket_name=default
    '''
    def verify_ddl_statements_can_be_executed_with_link_connected(self):

        self.log.info('Create connection')
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info('Load documents in KV')
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, 'create', 0, self.num_items)
        
        self.log.info('Create dataset on {0} bucket'.format(self.cb_bucket_name))
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name), msg='Fail to create dataset')

        self.log.info('Connect Local link')
        self.assertTrue(self.cbas_util.connect_link(), msg='Fail to connect Local link')

        self.log.info('Validate document count on CBAS')
        count_n1ql = self.rest.query_tool('select count(*) from %s' % (self.cb_bucket_name))['results'][0]['$1']
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, count_n1ql), msg='Count mismatch on CBAS')

        self.log.info('Verify link state before executing the DDL\'s')
        self.verify_bucket_state(1, self.cb_bucket_name, 'connected')

        self.log.info('Create index on {0} bucket with link connected'.format(self.cb_bucket_name))
        self.index_field = self.input.param('index_field', 'name:string')
        self.create_sec_index(self.index_name, self.cbas_dataset_name, self.index_field)

        self.log.info('Drop index on {0} bucket with link connected'.format(self.cb_bucket_name))
        drop_idx_statement = 'drop index {0}.{1};'.format(self.cbas_dataset_name, self.index_name)
        status, _, _, _, _ = self.cbas_util.execute_statement_on_cbas_util(drop_idx_statement)
        self.assertTrue(status == 'success', 'Drop Index query failed')

        self.log.info('Drop dataset on {0} bucket with link connected'.format(self.cb_bucket_name))
        self.assertTrue(self.cbas_util.drop_dataset(self.cbas_dataset_name), msg='Fail to drop dataset')

        self.log.info('Create dataset on {0} bucket with link connected'.format(self.cb_bucket_name))
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name), msg='Fail to create dataset')

        self.log.info('Verify link state after executing the DDL\'s')
        self.verify_bucket_state(1, self.cb_bucket_name, 'disconnected')

    '''
    verify_ddl_statements_can_be_executed_with_link_connected_custom_dataverse,default_bucket=True,items=10000,index_name=idx
    '''
    def verify_ddl_statements_can_be_executed_with_link_connected_custom_dataverse(self):
        
        self.log.info('Create connection')
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info('Load documents in KV')
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, 'create', 0, self.num_items)

        self.log.info('Create dataset on {0} bucket'.format(self.cb_bucket_name))
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name), msg='Fail to create dataset')

        self.log.info('Connect Local link')
        self.assertTrue(self.cbas_util.connect_link(), msg='Fail to connect Local link')

        self.log.info('Validate document count on CBAS')
        count_n1ql = self.rest.query_tool('select count(*) from %s' % (self.cb_bucket_name))['results'][0]['$1']
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, count_n1ql), msg='Count mismatch on CBAS')

        self.log.info('Verify link state before executing the DDL\'s')
        self.verify_bucket_state(1, self.cb_bucket_name, 'connected')

        self.log.info('Create dataverse custom')
        self.dataverse = self.input.param('dataverse', 'custom')
        self.cbas_util.create_dataverse_on_cbas(dataverse_name=self.dataverse)

        self.log.info('Create dataset on {0} bucket with link connected'.format(self.cb_bucket_name))
        self.cbas_dataset_name =  self.dataverse + '.' + self.cbas_dataset_name
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name), msg='Fail to create dataset')

        self.log.info('Connect to Local link on dataverse')
        self.cbas_util.connect_link(link_name=self.dataverse + '.Local')
        
        self.log.info('Verify link state before executing the DDL\'s')
        self.verify_bucket_state(2, self.cb_bucket_name, 'connected')
        self.verify_bucket_state(2, self.cb_bucket_name, 'connected')

        self.log.info('Create index on {0} bucket with link connected'.format(self.cb_bucket_name))
        self.index_field = self.input.param('index_field', 'name:string')
        self.create_sec_index(self.index_name, self.cbas_dataset_name, self.index_field)

        self.log.info('Drop index on {0} bucket with link connected'.format(self.cb_bucket_name))
        drop_idx_statement = 'drop index {0}.{1};'.format(self.cbas_dataset_name, self.index_name)
        status, _, _, _, _ = self.cbas_util.execute_statement_on_cbas_util(drop_idx_statement)
        self.assertTrue(status == 'success', 'Drop Index query failed')

        self.log.info('Drop dataset on {0} bucket with link connected'.format(self.cb_bucket_name))
        self.assertTrue(self.cbas_util.drop_dataset(self.cbas_dataset_name), msg='Fail to drop dataset')

        self.log.info('Verify link state after executing the DDL\'s')
        self.verify_bucket_state(1, self.cb_bucket_name, 'connected')
    
    '''
    verify_dataset_creation_fails_if_max_dataset_exceeds,default_bucket=True,cbas_dataset_name=ds,cb_bucket_name=default
    '''
    def verify_dataset_creation_fails_if_max_dataset_exceeds(self):
        
        self.log.info('Create connection')
        self.cbas_util.createConn(self.cb_bucket_name)
        
        self.log.info('Update storageMaxActiveWritableDatasets count')
        active_data_set_count = 1
        update_config_map = {'storageMaxActiveWritableDatasets': active_data_set_count}
        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(update_config_map)
        self.assertTrue(status, msg='Failed to update config')

        self.log.info('Restart the cbas node using the api')
        status, _, _ = self.cbas_util.restart_analytics_cluster_uri()
        self.assertTrue(status, msg='Failed to restart cbas')
        self.cbas_util.wait_for_cbas_to_recover()
        
        self.log.info('Create dataset on {0} bucket'.format(self.cb_bucket_name))
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name), msg='Fail to create dataset')

        self.log.info('Connect Local link')
        self.assertTrue(self.cbas_util.connect_link(), msg='Fail to connect Local link')
        
        self.log.info('Verify link state before executing the DDL\'s')
        self.verify_bucket_state(1, self.cb_bucket_name, 'connected')
        
        self.log.info('Verify creating dataset fails with link connected')
        self.assertFalse(self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name + str(active_data_set_count)), msg='Dataset creation must fail as storageMaxActiveWritableDatasets is set to 1')

    '''
    verify_ddl_statement_execution_suspends_corresponding_dcp_stream,default_bucket=True,cbas_dataset_name=ds,cb_bucket_name=default,items=200000
    '''
    def verify_ddl_statement_execution_suspends_corresponding_dcp_stream(self):
        
        self.log.info('Create connection')
        self.cbas_util.createConn(self.cb_bucket_name)
        
        self.log.info('Load {0} documents in KV'.format(self.num_items))
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, 'create', 0, self.num_items, batch_size=20000)
        
        self.log.info('Create dataset on {0} bucket - Default dataset'.format(self.cb_bucket_name))
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name), msg='Fail to create dataset on Default dataverse')
        
        self.log.info('Create dataverse custom')
        self.dataverse = self.input.param('dataverse', 'custom')
        self.cbas_util.create_dataverse_on_cbas(dataverse_name=self.dataverse)
        
        self.log.info('Create dataset on {0} bucket - custom dataverse'.format(self.cb_bucket_name))
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.dataverse + '.' + self.cbas_dataset_name), msg='Fail to create dataset on custom dataverse')
        
        self.log.info('Connect to Local link on default dataverse')
        self.cbas_util.connect_link()

        self.log.info('Connect to Local link on custom dataverse')
        self.cbas_util.connect_link(link_name=self.dataverse + '.Local')
        
        self.log.info('Verify link state before executing the DDL\'s')
        self.verify_bucket_state(2, self.cb_bucket_name, 'connected')
        
        self.log.info('Capture dataset count and verify dcp stream is suspended')
        self.count_n1ql = self.rest.query_tool('select count(*) from %s' % (self.cb_bucket_name))['results'][0]['$1']
        self.default_dataverse_dcp_state = []
        self.custom_dataverse_dcp_state = []
        create_sec_index_while_dcp_state_connected = True
        while True:
            item_count_default_ds, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            item_count_custom_ds, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.dataverse + '.' + self.cbas_dataset_name)
            
            if item_count_custom_ds == self.count_n1ql:
                break
            
            status, content, response = self.cbas_util.fetch_pending_mutation_on_cbas_node(self.cbas_node.ip)
            content = json.loads(content)
            self.default_dataverse_dcp_state.append(self.cbas_dataset_name in content["default:all:reader_stats"]["remaining"])
            self.custom_dataverse_dcp_state.append(self.dataverse + '.' + self.cbas_dataset_name in content["default:all:reader_stats"]["remaining"])
            
            if create_sec_index_while_dcp_state_connected and (item_count_custom_ds > 0 and item_count_default_ds > 0):
                self.log.info('Async create index on custom dataverse dataset')
                self.index_field = self.input.param('index_field', 'profession:string')
                create_idx_statement = 'create index idx if not exists on {0}({1})'.format(self.dataverse + '.' + self.cbas_dataset_name, self.index_field)
                self.query_tasks = self.cbas_util.async_query_execute(create_idx_statement, 'immediate', 1)
                create_sec_index_while_dcp_state_connected = False
        
        self.log.info('Complete task')
        for task in self.query_tasks:
            task.get_result()

        self.log.info('Verify only impacted dcp streams are suspended')
        self.assertEqual(len(list(set(self.default_dataverse_dcp_state))), 1, msg="DCP state must not be suspended for default dataset")
        self.assertEqual(len(list(set(self.custom_dataverse_dcp_state))), 2, msg="DCP state must be suspended for custom dataset as index was created while ingestion was in progress")

    def verify_ddl_are_executed_in_serial(self):

        self.log.info('Create connection')
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info('Load {0} documents in KV'.format(self.num_items))
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, 'create', 0, self.num_items, batch_size=20000)

        self.log.info('Create dataset on {0} bucket - Default dataset'.format(self.cb_bucket_name))
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name), msg='Fail to create dataset on Default dataverse')

        self.log.info('Connect to Local link on default dataverse')
        self.cbas_util.connect_link()
        
        self.log.info('Validate document count on CBAS')
        count_n1ql = self.rest.query_tool('select count(*) from %s' % (self.cb_bucket_name))['results'][0]['$1']
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, count_n1ql), msg='Count mismatch on CBAS')

        self.log.info('Async create multiple indexes on dataset')
        self.index_field = self.input.param('index_field', 'profession:string')
        self.query_tasks = []
        self.num_of_indexes = 10
        for index in range(self.num_of_indexes):
            create_idx_statement = 'create index idx_{0} if not exists on {1}({2})'.format(index, self.cbas_dataset_name, self.index_field)
            self.query_tasks.append(self.cbas_util.async_query_execute(create_idx_statement, 'immediate', 1))

        self.log.info('Complete task')
        for tasks in self.query_tasks:
            for task in tasks:
                task.get_result()

        self.log.info('Validate metadata timestamps to verify index creation was sequential')
        response, _, _, results, _ = self.cbas_util.execute_statement_on_cbas_util('select value SPLIT(Timestamp, " ")[3] from Metadata.`Index` where IsPrimary = false')
        self.assertEqual(len(list(set(results))), self.num_of_indexes, msg='Index creation must be sequential')

    def tearDown(self):
        super(CBASImplicitLinkManagement, self).tearDown()