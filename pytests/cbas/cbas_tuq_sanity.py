import json
import math

'''
Created on Mar 16, 2018

@author: riteshagarwal
'''
from tuqquery.tuq_sanity import QuerySanityTests

class CBASTuqSanity(QuerySanityTests):
    
    def test_array_length(self):
        for bucket in self.buckets:
            self.query = "SELECT _id, array_length(hikes) as hike_count" +\
            " FROM %s " % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['_id']))

            tmp_ids = set([doc['_id'] for doc in self.full_list])
            expected_result = [{"_id" : id,
                                "hike_count" : len([x["hikes"] for x in self.full_list
                                           if x["_id"] == id][0])}
                               for id in tmp_ids]
            expected_result = sorted(expected_result, key=lambda doc: (doc['_id']))
            self._verify_results(actual_result, expected_result)
        
    def test_floor(self):
        for bucket in self.buckets:
            self.query = "select name, floor(test_rate) as rate from %s"  % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name'], doc['rate']))

            expected_result = [{"name" : doc['name'], "rate" : math.floor(doc['test_rate'])}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['rate']))
            self._verify_results(actual_result, expected_result)

            self.query = "select name from %s where floor(test_rate) > 5"  % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['name']))
            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if math.floor(doc['test_rate']) > 5]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_array_avg(self):
        for bucket in self.buckets:
            self.query = "SELECT _id, array_avg(hikes)" +\
            " as avg_hike FROM %s " % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['_id']))
            for doc in actual_result:
                doc['avg_hike'] = round(doc['avg_hike'])
            tmp_ids = set([doc['_id'] for doc in self.full_list])
            expected_result = [{"_id" : id,
                                "avg_hike" : round(sum([x["hikes"] for x in self.full_list
                                           if x["_id"] == id][0]) / float(len([x["hikes"]
                                                                                     for x in self.full_list
                                           if x["_id"] == id][0])))}
                               for id in tmp_ids]
            expected_result = sorted(expected_result, key=lambda doc: (doc['_id']))
            self._verify_results(actual_result, expected_result)

    def test_array_count(self):
        for bucket in self.buckets:
            self.query = "SELECT _id, array_count(hikes) as hike_count" +\
            " FROM %s " % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['_id']))

            tmp_ids = set([doc['_id'] for doc in self.full_list])
            expected_result = [{"_id" : id,
                                "hike_count" : len([x["hikes"] for x in self.full_list
                                           if x["_id"] == id][0])}
                               for id in tmp_ids]
            expected_result = sorted(expected_result, key=lambda doc: (doc['_id']))
            self._verify_results(actual_result, expected_result)

    def test_array_max(self):
        for bucket in self.buckets:
            self.query = "SELECT _id, array_max(hikes) as max_hike" +\
            " FROM %s " % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['_id']))

            tmp_ids = set([doc['_id'] for doc in self.full_list])
            expected_result = [{"_id" : id,
                                "max_hike" : max([x["hikes"] for x in self.full_list
                                           if x["_id"] == id][0])}
                               for id in tmp_ids]
            expected_result = sorted(expected_result, key=lambda doc: (doc['_id']))
            self._verify_results(actual_result, expected_result)
            
    def test_array_min(self):
        for bucket in self.buckets:
            self.query = "SELECT _id, array_min(hikes) as min_hike" +\
            " FROM %s " % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['_id']))

            tmp_ids = set([doc['_id'] for doc in self.full_list])
            expected_result = [{"_id" : id,
                                "min_hike" : min([x["hikes"] for x in self.full_list
                                           if x["_id"] == id][0])}
                               for id in tmp_ids]
            expected_result = sorted(expected_result, key=lambda doc: (doc['_id']))
            self._verify_results(actual_result, expected_result)

    def test_array_sum(self):
        for bucket in self.buckets:
            self.query = "SELECT _id, round(array_sum(hikes)) as total_hike" +\
            " FROM %s" % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['_id']))
            
            tmp_ids = set([doc['_id'] for doc in self.full_list])
            expected_result = [{"_id" : id,
                                "total_hike" : round(sum([x["hikes"] for x in self.full_list
                                           if x["_id"] == id][0]))}
                               for id in tmp_ids]
            expected_result = sorted(expected_result, key=lambda doc: (doc['_id']))
            self._verify_results(actual_result, expected_result)
            
            
    def test_check_types(self):
        types_list = [("name", "ISSTR", True), ("skills[0]", "ISSTR", True),
                      ("test_rate", "ISSTR", False), ("VMs", "ISSTR", False),
                      ("false", "ISBOOL", True), ("join_day", "ISBOOL", False),
                      ("VMs", "ISARRAY", True), ("VMs[0]", "ISARRAY", False),
                      ("VMs", "ISATOM", False), ("hikes[0]", "ISATOM", True),("name", "ISATOM", True),
                      ("hikes[0]", "ISNUMBER", True), ("hikes", "ISNUMBER", False),
                      ("skills[0]", "ISARRAY", False), ("skills", "ISARRAY", True)]
        for bucket in self.buckets:
            for name_item, fn, expected_result in types_list:
                self.query = 'SELECT %s(%s) as type_output FROM %s' % (
                                                        fn, name_item, bucket.name)
                actual_result = self.run_cbq_query()
                for doc in actual_result['results']:
                    self.assertTrue(doc["type_output"] == expected_result,
                                    "Expected output for fn %s( %s) : %s. Actual: %s" %(
                                                fn, name_item, expected_result, doc["type_output"]))
                self.log.info("Fn %s(%s) is checked. (%s)" % (fn, name_item, expected_result))
                
    def test_to_string(self):
        for bucket in self.buckets:
            self.query = "SELECT TOSTRING(join_mo) month FROM %s" % bucket.name
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = [{"month" : str(doc['join_mo'])} for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_regex_contains(self):
        for bucket in self.buckets:
            self.query = "select email from %s where REGEXP_CONTAINS(email, '-m..l')" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"email" : doc["email"]}
                               for doc in self.full_list
                               if len(re.compile('-m..l').findall(doc['email'])) > 0]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)
            
    def test_title(self):
        for bucket in self.buckets:
            self.query = "select TITLE(VMs[0].os) as OS from %s" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])

            expected_result = [{"OS" : (doc["VMs"][0]["os"][0].upper() + doc["VMs"][0]["os"][1:])}
                               for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)
