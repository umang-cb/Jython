'''
Created on Sep 25, 2017

@author: riteshagarwal
'''

from Rest_Connection import RestConnection
import logger
import json

log = logger.Logger.get_logger()

class ClusterHelper(RestConnection):

    def __init__(self, server):
        super(ClusterHelper, self).__init__(server)

    def get_nodes_data_from_cluster(self, param="nodes"):
        api = self.baseUrl + "pools/default/"
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            if param in json_parsed:
                return json_parsed[param]
        else:
            return None