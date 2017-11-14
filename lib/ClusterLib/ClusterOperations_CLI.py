'''
Created on Nov 14, 2017

@author: riteshagarwal
'''

import logger
from ClusterLib.ClusterOperations_Rest import ClusterHelper as cluster_helper_rest
log = logger.Logger.get_logger()
    
class ClusterHelper(cluster_helper_rest):

    def __init__(self, server, username, password, cb_version=None):
        self.server = server
        self.hostname = "%s:%s" % (server.ip, server.port)
        self.username = username
        self.password = password
        self.cb_version = cb_version
        super(ClusterHelper, self).__init__(server)
        