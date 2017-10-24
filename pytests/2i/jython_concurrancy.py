import logging
from base_2i import BaseSecondaryIndexingTests


log = logging.getLogger(__name__)


class JythonConcurrancy(BaseSecondaryIndexingTests):
    def setUp(self):
        super(JythonConcurrancy, self).setUp()

    def tearDown(self):
        super(JythonConcurrancy, self).tearDown()

    def test_recovery_dummy(self):
        import pdb
        pdb.set_trace()
        pass

    def test_dummy(self):
        tasks = self.async_run_multi_operations(buckets=self.buckets,
                                                query_definitions=self.query_definitions,
                                                create_index=True, drop_index=False)
        for task in tasks:
            task.get_result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                self.nodes_in_list, [], services=self.services_in)
        query_tasks = self.async_run_multi_operations(buckets=self.buckets,
                                                query_definitions=self.query_definitions,
                                                create_index=False, query=True)
        query_tasks.append(rebalance)
        for task in query_tasks:
            task.get_result()