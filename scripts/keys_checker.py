'''
Created on Apr 26, 2018

@author: riteshagarwal
'''
from couchbase.exceptions import CouchbaseError, CouchbaseNetworkError,\
    CouchbaseTransientError


import logging
log = logging.getLogger()

def check_dataloss(ip, bucket, num_items):
    from couchbase.bucket import Bucket
    from couchbase.exceptions import NotFoundError
    bkt = Bucket('couchbase://{0}/{1}'.format(ip, bucket), username="Administrator", password="password")
    batch_start = 2000000
    batch_end = 0
    batch_size = 10000
    errors = []
    missing_keys = []
    errors_replica = []
    missing_keys_replica = []
    while num_items > batch_end:
        batch_end = batch_start + batch_size
        keys = []
        for i in xrange(batch_start, batch_end, 1):
            keys.append(str(i))
        try:
            bkt.get_multi(keys)
            print("Able to fetch keys starting from {0} to {1}".format(keys[0], keys[len(keys) - 1]))
        except CouchbaseError as e:
            print(e)
            ok, fail = e.split_results()
            if fail:
                for key in fail:
                    try:
                        bkt.get(key)
                    except NotFoundError:
                        errors.append("Missing key: {0}".
                                      format(key))
                        missing_keys.append(key)
        try:
            bkt.get_multi(keys, replica=True)
            print("Able to fetch keys starting from {0} to {1} in replica ".format(keys[0], keys[len(keys) - 1]))
        except (CouchbaseError, CouchbaseNetworkError, CouchbaseTransientError) as e:
            print(e)
            ok, fail = e.split_results()
            if fail:
                for key in fail:
                    try:
                        bkt.get(key)
                    except NotFoundError:
                        errors_replica.append("Missing key: {0}".
                                      format(key))
                        missing_keys_replica.append(key)
        batch_start += batch_size
    return errors, missing_keys, errors_replica, missing_keys_replica

print check_dataloss("172.23.104.67", "GleambookUsers", 20000000)