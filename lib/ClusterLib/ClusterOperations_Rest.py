'''
Created on Sep 25, 2017

@author: riteshagarwal
'''

from Rest_Connection import RestConnection
import logger

log = logger.Logger.get_logger()

class ClusterHelper(RestConnection):

    def __init__(self, server):
        super(ClusterHelper, self).__init__(server)

