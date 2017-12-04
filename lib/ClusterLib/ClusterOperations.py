'''
Created on Oct 24, 2017

@author: riteshagarwal
'''
java_sdk = True
use_cli = False
use_rest = False
use_memcached = True

if java_sdk:
    from ClusterLib.ClusterOperations_JavaSDK import ClusterHelper as clusterlib
elif use_cli:
    from ClusterLib.ClusterOperations_CLI import ClusterHelper as clusterlib
elif use_rest:
    from ClusterLib.ClusterOperations_Rest import ClusterHelper as clusterlib
    
class ClusterHelper(clusterlib):
    pass