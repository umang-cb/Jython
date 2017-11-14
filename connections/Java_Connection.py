'''
Created on Nov 6, 2017
Java based SDK client interface

@author: riteshagarwal

'''
from com.couchbase.client.java import CouchbaseCluster
from com.couchbase.client.core import CouchbaseException

class SDKClient(object):
    """Java SDK Client Implementation for testrunner - master branch Implementation"""

    def __init__(self, server):
        self.server = server
        self.username = self.server.rest_username
        self.password = self.server.rest_password
        self.cluster = None
        self.clusterManager = None

    def connectCluster(self):
        try:
            self.cluster = CouchbaseCluster.create(self.server.ip)
            self.cluster.authenticate(self.username, self.password)
            self.clusterManager = self.cluster.clusterManager()
        except CouchbaseException:
            raise

    def reconnectCluster(self):
        self.disconnectCluster()
        self.connectCluster()

    def disconnectCluster(self):
        self.cluster.disconnect()