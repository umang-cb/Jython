import time
import json

from cbas.cbas_base import CBASBaseTest
from remote.remote_util import RemoteMachineShellConnection


class CBASAdvanceDcpState(CBASBaseTest):

    def setUp(self):
        super(CBASAdvanceDcpState, self).setUp()
        self.txn_dataset_checkpoint_interval_default_value = 600
        self.txn_dataset_checkpoint_interval_update_value = 30
        self.analytics_broadcast_dcp_state_mutation_count_default_value = 10000
        self.analytics_broadcast_dcp_state_mutation_count_update_value = 12000
        self.checkpoint_thread_time = 120
    
    def verify_updating_service_parameters(self, parameter_name, default_parameter_value, updated_parameter_value):

        self.log.info('Verify default service level parameter {0} value'.format(default_parameter_value))
        status, content, _ = self.cbas_util.fetch_service_parameter_configuration_on_cbas()
        self.assertTrue(status, msg='Failed to fetch config services')
        self.assertEqual(json.loads((content.decode('utf-8')))[parameter_name], default_parameter_value, msg='Value mismatch for {0}'.format(parameter_name))

        self.log.info('Update service level parameter {0}'.format(parameter_name))
        update_config_map = {parameter_name: updated_parameter_value}
        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(update_config_map)
        self.assertTrue(status, msg='Failed to update {0}'.format(parameter_name))

        self.log.info('Restart cbas node to apply the parameter change')
        status, _, _ = self.cbas_util.restart_analytics_cluster_uri()
        self.assertTrue(status, msg='Failed to restart cbas node')
        self.cbas_util.wait_for_cbas_to_recover()

        self.log.info('Verify service level parameter {0} is updated'.format(parameter_name))
        status, content, _ = self.cbas_util.fetch_service_parameter_configuration_on_cbas()
        self.assertTrue(status, msg='Failed to fetch config services')
        self.assertEqual(json.loads((content.decode('utf-8')))[parameter_name], updated_parameter_value, msg='Value mismatch for {0}'.format(parameter_name))

    '''
    MB-31914
    cbas.cbas_advance_dcp_state_storage.CBASAdvanceDcpState.test_dcp_state_for_datasets_is_advanced_after_checkpoint_interval,cb_bucket_name=default,cbas_dataset_name=ds,cbas_dataset2_name=ds_filter,items=100000
    '''   
    def test_dcp_state_for_datasets_is_advanced_after_checkpoint_interval(self):

        self.log.info('Updating service level param txnDatasetCheckpointInterval')
        self.verify_updating_service_parameters('txnDatasetCheckpointInterval', self.txn_dataset_checkpoint_interval_default_value, self.txn_dataset_checkpoint_interval_update_value)
        
        self.log.info('Create connection')
        self.cbas_util.createConn(self.cb_bucket_name)
        
        self.log.info('Create dataset with out filters')
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name), msg='Fail to create dataset without filters')

        self.log.info('Create dataset with filters such that it matches only 1 record in KV')
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset2_name, where_field='_id', where_value='test_docs-0'), msg='Fail to create dataset with filters')
        
        self.log.info('Connect Local link')
        self.assertTrue(self.cbas_util.connect_link(), msg='Fail to connect link')

        self.log.info('Load documents in KV')
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, 'create', 0, self.num_items)

        self.log.info('Validate document count on CBAS')
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items), msg='Count mismatch on CBAS dataset without filter')
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset2_name, 1), msg='Count mismatch on CBAS dataset with filter')
        
        self.log.info('Wait for time greater than the checkpoint thread time + txnDatasetCheckpointInterval')
        # since there are no more mutations coming and after the above specified time is exceeded, dcp state will be persisted
        self.sleep(self.checkpoint_thread_time + self.txn_dataset_checkpoint_interval_update_value, message='wait for checkpoint thread time')

        self.log.info('Kill Analytics service')
        RemoteMachineShellConnection(self.cbas_node).kill_java()
        
        self.log.info('Verify when analytics restarts there are no pending mutations - No more data to be ingested on Analytics')
        self.dataset_without_filter_pending_count = []
        self.dataset_with_filter_pending_count = []
        
        start_time = time.time()
        while time.time() < start_time + 60:
            try:
                status, content, response = self.cbas_util.fetch_pending_mutation_on_cbas_node(self.cbas_node.ip)
                content = json.loads(content)
                self.dataset_without_filter_pending_count.append(content['default:all:reader_stats']['remaining']['Default.' + self.cbas_dataset_name])
                self.dataset_with_filter_pending_count.append(content['default:all:reader_stats']['remaining']['Default.' +self.cbas_dataset2_name])
            except:
                pass
        
        self.log.info('Verify dcp state was advanced and there were no pending mutations post analytics service crash')
        self.assertEqual(len(list(set(self.dataset_without_filter_pending_count))), 1, msg='DCP state was not advanced for dataset without filter')
        self.assertEqual(list(set(self.dataset_without_filter_pending_count))[0], 0, msg='Pending mutation for dataset without filter must be 0')
        self.assertEqual(len(list(set(self.dataset_with_filter_pending_count))), 1, msg='DCP state was not advanced for dataset with filter')
        self.assertEqual(list(set(self.dataset_with_filter_pending_count))[0], 0, msg='Pending mutation for dataset with filter must be 0')
        
        self.log.info('Validate document count on CBAS post analytics service restart')
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items), msg='Count mismatch on CBAS dataset without filter')
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset2_name, 1), msg='Count mismatch on CBAS dataset with filter')
    
    '''
    cbas.cbas_advance_dcp_state_storage.CBASAdvanceDcpState.test_dcp_states_are_updated_based_on_mutation_count_parameter,cb_bucket_name=default,cbas_dataset_name=ds,items=20000
    '''
    def test_dcp_states_are_updated_based_on_mutation_count_parameter(self):

        self.log.info('Load {0} documents in KV'.format(self.num_items))
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, 'create', 0, self.num_items)

        self.log.info('Updating service level param txnDatasetCheckpointInterval')
        self.verify_updating_service_parameters('txnDatasetCheckpointInterval', self.txn_dataset_checkpoint_interval_default_value, self.txn_dataset_checkpoint_interval_update_value)

        self.log.info('Create connection')
        self.cbas_util.createConn(self.cb_bucket_name)
        
        self.log.info('Create dataset with filters')
        # Filter matches no document in kv
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name, where_field='_id', where_value='test_docs'), msg='Fail to create dataset with filters')
        
        self.log.info('Connect Local link')
        self.assertTrue(self.cbas_util.connect_link(), msg='Fail to connect link')

        self.log.info('Validate no documents are ingested in Analytics')
        self.sleep()
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, 0), msg='Count mismatch on CBAS dataset without filter')
        
        self.log.info('Wait for time greater than the checkpoint thread time + txnDatasetCheckpointInterval')
        # since there are no more mutations coming and after the above specified time is exceeded, dcp state will be persisted
        self.sleep(self.checkpoint_thread_time + self.txn_dataset_checkpoint_interval_update_value, message='wait for checkpoint thread time')
        
        self.log.info('Load {0} more documents in KV, such that total documents will be twice {0}'.format(self.num_items, self.num_items))
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, 'create', self.num_items, self.num_items * 2)

        self.log.info('Kill Analytics service')
        RemoteMachineShellConnection(self.cbas_node).kill_java()

        self.log.info('Verify when analytics restarts pending mutations must not catch up from zero as dcp state was persisted after {0} documents were inserted'.format(self.num_items))
        self.dataset_with_filter_pending_count = []
        start_time = time.time()
        while time.time() < start_time + 120:
            try:
                status, content, response = self.cbas_util.fetch_pending_mutation_on_cbas_node(self.cbas_node.ip)
                content = json.loads(content)
                self.dataset_with_filter_pending_count.append(content['default:all:reader_stats']['remaining']['Default.' +self.cbas_dataset_name])
            except:
                pass
        pending_mutation_post_service_kill = sorted(list(set(self.dataset_with_filter_pending_count)), reverse=True)
        # since we inserted num.items more documents after dcp state was persisted, the ingestion must start only for last added documents and not from 0
        self.assertTrue(pending_mutation_post_service_kill[0] != 0 and pending_mutation_post_service_kill[0] <= self.num_items, msg='Pending mutation count must pick for remaning {0} documents'.format(self.num_items))


    def test_verify_updating_service_parameters(self):
        
        self.log.info('Verify updating service level param analyticsBroadcastDcpStateMutationCount')
        self.verify_updating_service_parameters('analyticsBroadcastDcpStateMutationCount', self.analytics_broadcast_dcp_state_mutation_count_default_value, self.analytics_broadcast_dcp_state_mutation_count_update_value)

        self.log.info('Verify updating service level param analyticsBroadcastDcpStateMutationCount')
        self.verify_updating_service_parameters('txnDatasetCheckpointInterval', self.txn_dataset_checkpoint_interval_default_value, self.txn_dataset_checkpoint_interval_update_value)


    def tearDown(self):
        super(CBASAdvanceDcpState, self).tearDown()
