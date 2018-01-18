'''
Created on Oct 24, 2017

@author: riteshagarwal
'''
import mode

if mode.java:
    from ClusterLib.ClusterOperations_JavaSDK import ClusterHelper as clusterlib
elif mode.cli:
    from ClusterLib.ClusterOperations_CLI import ClusterHelper as clusterlib
elif mode.rest:
    from ClusterLib.ClusterOperations_Rest import ClusterHelper as clusterlib
    
class ClusterHelper(clusterlib):
    pass