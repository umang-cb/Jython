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
from BucketOperations_Rest import BucketHelper as bucket_helper_rest
from com.couchbase.client.java import Bucket
from Java_Connection import SDKClient

log = logger.Logger.get_logger()

class BucketHelper(bucket_helper_rest, SDKClient):

    def __init__(self,server):
        self.server = server
        super(BucketHelper, self).__init__(server)
        super(SDKClient, self).__init__(server)
        pass
    
    def bucket_exists(self, bucket):
        try:
            self.connectCluster()
            hasBucket = self.clusterManager.hasBucket(bucket);
            self.disconnectCluster()
            return hasBucket
        except BucketDoesNotExistException as e:
            log.info(e)
            self.disconnectCluster()
            return False
        except AuthenticationException as e:
            log.info(e)
            self.disconnectCluster()
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
            self.connectCluster()
            self.clusterManager.removeBucket(bucket_name);
            self.disconnectCluster()
            return True
        except BucketDoesNotExistException as e:
            log.error(e)
            self.disconnectCluster()
            return False
        except AuthenticationException as e:
            log.error(e)
            self.disconnectCluster()
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
        
        self.connectCluster()        
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
            self.clusterManager.insertBucket(bucketSettings)
    
            self.disconnectCluster()
            return True
        except BucketDoesNotExistException as e:
            log.info(e)
            self.disconnectCluster()
            return False
        except AuthenticationException as e:
            log.info(e)
            self.disconnectCluster()
            return False
