import json

from cbas.cbas_base import CBASBaseTest
from security_utils.audit_ready_functions import audit


class CBASAuditLogs(CBASBaseTest):
    
    service_parameter_expected_dict = {}
    node_parameter_expected_dict = {}
    
    def build_service_parameter_expected_dict(self):
        self.log.info("Fetch configuration service parameters")
        status, content, response = self.cbas_util.fetch_service_parameter_configuration_on_cbas()
        self.assertTrue(status, msg="Response status incorrect for GET request")

        self.log.info("Create server configuration expected dictionary")
        actual_dict = json.loads(content)
        for key in actual_dict:
            CBASAuditLogs.service_parameter_expected_dict["config_before:" + key] = actual_dict[key]
            CBASAuditLogs.service_parameter_expected_dict["config_after:" + key] = actual_dict[key]
    
    def build_node_parameter_expected_dict(self):
        self.log.info("Fetch configuration node parameters")
        status, content, response = self.cbas_util.fetch_node_parameter_configuration_on_cbas()
        self.assertTrue(status, msg="Response status incorrect for GET request")

        self.log.info("Create node configuration expected dictionary")
        actual_dict = json.loads(content)
        for key in actual_dict:
            CBASAuditLogs.node_parameter_expected_dict["config_before:" + key] = actual_dict[key]
            CBASAuditLogs.node_parameter_expected_dict["config_after:" + key] = actual_dict[key]

    def setUp(self):
        super(CBASAuditLogs, self).setUp()

        self.log.info("Enable audit on cluster")
        audit_obj = audit(host=self.master)
        current_state = audit_obj.getAuditStatus()
        if current_state:
            audit_obj.setAuditEnable('false')
        audit_obj.setAuditEnable('true')
        self.sleep(30)

        self.log.info("Read audit input id")
        self.audit_id = self.input.param("audit_id", None)
        
        self.log.info("Build service/node configuration dictionaries")
        self.build_service_parameter_expected_dict()
        self.build_node_parameter_expected_dict()

    def validate_audit_event(self, event_id, host, expected_audit):
        auditing = audit(eventID=event_id, host=host)
        _, audit_match = auditing.validateEvents(expected_audit)
        self.assertTrue(audit_match, "Values for one of the fields mismatch, refer test logs for mismatch value")

    """
    cbas.cbas_audit.CBASAuditLogs.test_service_configuration_audit,default_bucket=False,audit_id=36865
    """
    def test_service_configuration_audit(self):

        self.log.info("Update configuration service parameters for storageMaxActiveWritableDatasets")
        service_configuration_map = {"storageMaxActiveWritableDatasets": 10, "storageMemorycomponentNumcomponents":3}
        status, content, response = self.cbas_util.update_service_parameter_configuration_on_cbas(service_configuration_map)
        self.assertTrue(status, msg="Incorrect status for configuration service PUT request")

        self.log.info("Updated expected dictionary key value for entries in service_configuration_map")
        expected_dict = CBASAuditLogs.service_parameter_expected_dict
        for key in service_configuration_map:
            expected_dict["config_after:" + key] = service_configuration_map[key]

        self.log.info("Validate audit log for service configuration update")
        self.validate_audit_event(self.audit_id, self.cbas_node, expected_dict)
    
    """
    cbas.cbas_audit.CBASAuditLogs.test_node_configuration_audit,default_bucket=False,audit_id=36866
    """
    def test_node_configuration_audit(self):

        self.log.info("Update configuration service parameters for storageMaxActiveWritableDatasets")
        node_configuration_map = {"storageBuffercacheSize": 259489793}
        status, content, response = self.cbas_util.update_node_parameter_configuration_on_cbas(node_configuration_map)
        self.assertTrue(status, msg="Incorrect status for configuration node PUT request")

        self.log.info("Updated expected dictionary key value for entries in node_configuration_map")
        expected_dict = CBASAuditLogs.node_parameter_expected_dict
        for key in node_configuration_map:
            expected_dict["config_after:" + key] = node_configuration_map[key]

        self.log.info("Validate audit log for node configuration update")
        self.validate_audit_event(self.audit_id, self.cbas_node, expected_dict)
    
    """
    cbas.cbas_audit.CBASAuditLogs.test_audit_logs_are_not_generated_for_failed_service_configuration_update,default_bucket=False,audit_id=36865
    """
    def test_audit_logs_are_not_generated_for_failed_service_configuration_update(self):

        self.log.info("Update configuration service parameters for logLevel")
        service_configuration_map = {"logLevel": "TRACE1"}
        status, content, response = self.cbas_util.update_service_parameter_configuration_on_cbas(service_configuration_map)
        self.assertFalse(status, msg="Incorrect status for configuration service PUT request")

        self.log.info("Updated expected dictionary key value for entries in service_configuration_map")
        expected_dict = CBASAuditLogs.service_parameter_expected_dict
        for key in service_configuration_map:
            expected_dict["config_after:" + key] = service_configuration_map[key]

        self.log.info("Validate audit log for service configuration update")
        audit_obj = audit(eventID=self.audit_id, host=self.cbas_node)
        self.assertFalse(audit_obj.check_if_audit_event_generated(), msg="Audit event must not be generated")
        
    def tearDown(self):
        super(CBASAuditLogs, self).tearDown()

