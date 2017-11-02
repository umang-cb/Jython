'''
Created on Sep 25, 2017

@author: riteshagarwal
'''
from com.couchbase.client.java import *;
from com.couchbase.client.java.transcoder import JsonTranscoder
from com.couchbase.client.java.document import *;
from com.couchbase.client.java.document.json import *;
from com.couchbase.client.java.query import *;
from com.couchbase.client.java.error import BucketDoesNotExistException
from com.couchbase.client.core.endpoint.kv import AuthenticationException
from com.couchbase.client.java.cluster import DefaultBucketSettings
from com.couchbase.client.java.bucket import BucketType

import json
import time
import urllib
from bucket import *
import logger
from membase.api.rest_client import Node
from memcached.helper.kvstore import KVStore
from rest import Rest_Connection
from BucketOperations_Rest import BucketHelper as bucket_helper_rest
from com.couchbase.client.java import Bucket
log = logger.Logger.get_logger()

class BucketHelper(bucket_helper_rest):

    def __init__(self,server):
        self.server = server
        super(BucketHelper, self).__init__(server)
        pass
    
    def bucket_exists(self, bucket):
        try:
            cluster = CouchbaseCluster.create(self.server.ip);
            cluster.authenticate(self.server.rest_username,self.server.rest_password)
            clusterManager = cluster.clusterManager()
            return clusterManager.hasBucket(bucket);
            cluster.disconnect()
            return True
        except BucketDoesNotExistException as e:
            log.info(e)
            cluster.disconnect()
            return False
        except AuthenticationException as e:
            log.info(e)
            cluster.disconnect()
            return False

    def delete_bucket(self, bucket_name):
        '''
        Boolean removeBucket(String name)
        Removes a Bucket identified by its name with the default management timeout.
        
        This method throws:
        
        java.util.concurrent.TimeoutException: If the timeout is exceeded.
        com.couchbase.client.core.CouchbaseException: If the underlying resources could not be enabled properly.
        com.couchbase.client.java.error.TranscodingException: If the server response could not be decoded.
        Note: Removing a Bucket is an asynchronous operation on the server side, so even if the response is returned there is no guarantee that the operation has finished on the server itself.
        
        Parameters:
        name - the name of the bucket.
        Returns:
        true if the removal was successful, false otherwise.
        '''
        try:
            cluster = CouchbaseCluster.create(self.server.ip)
            cluster.authenticate(self.server.rest_username, self.server.rest_password)
            clusterManager = cluster.clusterManager()
            clusterManager.removeBucket(bucket_name);
            cluster.disconnect()
            return True
        except BucketDoesNotExistException as e:
            log.error(e)
            cluster.disconnect()
            return False
        except AuthenticationException as e:
            log.error(e)
            cluster.disconnect()
            return False
        
    def create_bucket(self, bucket='',
                      ramQuotaMB=1,
                      authType='none',
                      saslPassword='',
                      replicaNumber=1,
                      proxyPort=11211,
                      bucketType='membase',
                      replica_index=1,
                      threadsNumber=3,
                      flushEnabled=1,
                      evictionPolicy='valueOnly',
                      lww=False):
        
        cluster = CouchbaseCluster.create(self.server.ip);
        cluster.authenticate(self.server.rest_username,self.server.rest_password)
        clusterManager = cluster.clusterManager()
        
        try:
            bucketSettings = DefaultBucketSettings.builder()
            
            if bucketType == "memcached":
                bucketSettings.type(BucketType.MEMCACHED)
            elif bucketType == "ephemeral":
                bucketSettings.type(BucketType.EPHEMERAL)
            else:
                bucketSettings.type(BucketType.COUCHBASE)
                
            bucketSettings.password(saslPassword)
            bucketSettings.replicas(replicaNumber)
            bucketSettings.name(bucket)
            bucketSettings.quota(ramQuotaMB)
            bucketSettings.enableFlush(flushEnabled)
            bucketSettings.indexReplicas(replica_index)
            bucketSettings.build()
            clusterManager.insertBucket(bucketSettings)
    
            cluster.disconnect()
            return True
        except BucketDoesNotExistException as e:
            log.info(e)
            cluster.disconnect()
            return False
        except AuthenticationException as e:
            log.info(e)
            cluster.disconnect()
            return False
