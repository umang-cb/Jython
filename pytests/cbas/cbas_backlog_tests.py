from cbas.cbas_base import CBASBaseTest

"""
cbas.cbas_backlog_tests.Backlog_CBAS.test_cb_collect
"""

class Backlog_CBAS(CBASBaseTest):

    def test_cb_collect(self):
        nodes = ['ns_1@10.111.182.101','ns_1@10.111.182.102']
        uploadHost = "s3.amazonaws.com/bugdb/jira/1684_test_case_3"
        customer = "1684_test_case_3"
        status, _, _ = self.perform_cb_collect(nodes, uploadHost, customer)
        self.assertTrue(status, msg='Fail to perform CB Collect')
    
    def test_cb_collect_without_params(self):
        status, _, _ = self.perform_cb_collect()
        self.assertTrue(status, msg='Fail to perform CB Collect')
