'''
Created on Oct 24, 2017

@author: riteshagarwal
'''
java_sdk = True
use_cli = False
use_rest = False
use_memcached = True

if java_sdk:
    from CbasLib.CBASOperations_JavaSDK import CBASHelper as cbaslib
elif use_cli:
    from CbasLib.CBASOperations_CLI import CBASHelper as cbaslib
else:
    from CbasLib.CBASOperations_Rest import CBASHelper as cbaslib
    
class CBASHelper(cbaslib):
    pass