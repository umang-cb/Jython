'''
Created on Sep 25, 2017

@author: riteshagarwal
'''

from ClusterLib.ClusterOperations_Rest import ClusterHelper as cluster_helper_rest
from Java_Connection import SDKClient
import logger

log = logger.Logger.get_logger()

class ClusterHelper(cluster_helper_rest, SDKClient):

    def __init__(self,server):
        self.server = server
        super(ClusterHelper, self).__init__(server)
        super(SDKClient, self).__init__(server)
