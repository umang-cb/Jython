'''
Created on Oct 24, 2017

@author: riteshagarwal
'''
java_sdk = True
use_cli = False
use_rest = False
use_memcached = True

if java_sdk:
    from ClusterLib.ClusterOperations_JavaSDK import BucketHelper as bucketlib
elif use_cli:
    from ClusterLib.ClusterOperations_CLI import BucketHelper as bucketlib
elif use_rest:
    from ClusterLib.ClusterOperations_Rest import BucketHelper as bucketlib
    
class BucketHelper(bucketlib):
    pass