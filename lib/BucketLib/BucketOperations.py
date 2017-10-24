'''
Created on Oct 24, 2017

@author: riteshagarwal
'''
java_sdk = True
use_cli = False
use_rest = False

if java_sdk:
    from BucketOperations_JavaSDK import BucketHelper as bucketlib
elif use_cli:
    from BucketOperations_CLI import BucketHelper as bucketlib
elif use_rest:
    from BucketOperations_Rest import BucketHelper as bucketlib
    
class BucketHelper(bucketlib):
    pass