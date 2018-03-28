import json
import math

'''
Created on Mar 16, 2018

@author: riteshagarwal
'''
from tuqquery.tuq_sanity import QuerySanityTests
from membase.api.rest_client import RestConnection
from cbas.cbas_base import CBASBaseTest
import re

class cbas_object_tests(CBASBaseTest):
    def setup_cbas_bucket_dataset_connect(self):
        # Create bucket on CBAS
        self.query = 'insert into %s (KEY, VALUE) VALUES ("test",{"type":"testType","indexMap":{"key1":"val1", "key2":"val2"},"data":{"foo":"bar"}})'%(self.default_bucket_name)
        result = RestConnection(self.master).query_tool(self.query)
        self.assertTrue(result['status'] == "success")
        
        self.cbas_util.createConn(self.default_bucket_name)
        self.assertTrue(self.cbas_util.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                       cb_bucket_name=self.default_bucket_name),"bucket creation failed on cbas")
        
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.cbas_bucket_name,
                          cbas_dataset_name=self.cbas_dataset_name), "dataset creation failed on cbas")
        
        self.assertTrue(self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name),"Connecting cbas bucket to cb bucket failed")
        
        self.assertTrue(self.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name], 1),"Data ingestion to cbas couldn't complete in 300 seconds.")
        
    def test_object_pairs(self):
        self.setup_cbas_bucket_dataset_connect()
        self.query = "SELECT object_pairs(indexMap) from %s;"%self.cbas_dataset_name
        
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.query,"immediate")
        expected_result = [{
                        "name": "key1",
                        "value": "val1"
                      },
                      {
                        "name": "key2",
                        "value": "val2"
                      }]
        self.assertTrue(status=="success")
        self.assertTrue(result[0]['$1']==expected_result)

    def test_object_length(self):
        self.setup_cbas_bucket_dataset_connect()
        self.query = "SELECT object_length(indexMap) from %s;"%self.cbas_dataset_name
        
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.query,"immediate")
        self.assertTrue(status=="success")
        self.assertTrue(result[0]['$1']==2)
        
    def test_object_names(self):
        self.setup_cbas_bucket_dataset_connect()
        self.query = "SELECT object_names(indexMap) from %s;"%self.cbas_dataset_name
        
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.query,"immediate")

        expected_result = [{
                        "name": "key1",
                      },
                      {
                        "name": "key2",
                      }]
        self.assertTrue(status=="success")
        self.assertTrue(result[0]['$1']==expected_result)
        
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

    def test_case_and_like(self):
        for bucket in self.buckets:
            self.query = "SELECT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN" +\
            " 'winter' ELSE 'other' END AS period FROM %s WHERE (CASE WHEN" % (bucket.name) +\
            " join_mo < 3 OR join_mo > 11 THEN 'winter' ELSE 'other' END) LIKE 'win%'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                doc['name'],doc['period']))
            expected_result = [{"name" : doc['name'],
                                "period" : ('other','winter')
                                            [doc['join_mo'] in [12,1,2]]}
                               for doc in self.full_list
                               if ('other','winter')[doc['join_mo'] in [12,1,2]].startswith(
                                                                                'win')]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['period']))
            self._verify_results(actual_result, expected_result)

    def test_case_and_logic_exp(self):
        for bucket in self.buckets:
            self.query = "SELECT DISTINCT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN" +\
            " 'winter' ELSE 'other' END AS period FROM %s WHERE (CASE WHEN join_mo < 3" %(bucket.name) +\
            " OR join_mo > 11 THEN 1 ELSE 0 END) > 0 AND job_title='Sales'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name'],doc['period']))
            expected_result = [{"name" : doc['name'],
                                "period" : ('other','winter')
                                            [doc['join_mo'] in [12,1,2]]}
                               for doc in self.full_list
                               if (0, 1)[doc['join_mo'] in [12,1,2]] > 0 and\
                                  doc['job_title'] == 'Sales']
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['period']))
            self._verify_results(actual_result, expected_result)

    def test_case_and_comparision_exp(self):
        for bucket in self.buckets:
            self.query = "SELECT DISTINCT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN" +\
            " 'winter' ELSE 'other' END AS period FROM %s WHERE (CASE WHEN join_mo < 3" %(bucket.name) +\
            " OR join_mo > 11 THEN 1 END) = 1 AND job_title='Sales'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name'],doc['period']))
            expected_result = [{"name" : doc['name'],
                                "period" : ('other','winter')
                                            [doc['join_mo'] in [12,1,2]]}
                               for doc in self.full_list
                               if (doc['join_mo'], 1)[doc['join_mo'] in [12,1,2]] == 1 and\
                                  doc['job_title'] == 'Sales']
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['period']))
            self._verify_results(actual_result, expected_result)

    def test_contains(self):
        for bucket in self.buckets:
            self.query = "select name from %s where contains(job_title, 'Sale')" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])

            expected_result = [{"name" : doc["name"]}
                               for doc in self.full_list
                               if doc['job_title'].find('Sale') != -1]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)
            
    def test_between_bigint(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM {0} WHERE join_mo BETWEEN -9223372036854775808 AND 9223372036854775807 ORDER BY name".format(bucket.name)
            actual_result = self.run_cbq_query()

            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if doc["join_mo"] >= -9223372036854775808 and doc["join_mo"] <= 9223372036854775807]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT name FROM {0} WHERE join_mo NOT BETWEEN -9223372036854775808 AND 9223372036854775807 ORDER BY name".format(bucket.name)
            actual_result = self.run_cbq_query()

            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if not(doc["join_mo"] >= -9223372036854775808 and doc["join_mo"] <= 9223372036854775807)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)
            
    def test_between_double(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM {0} WHERE join_mo BETWEEN -1.79769313486231570E308  AND 1.79769313486231570E308 ORDER BY name".format(bucket.name)
            actual_result = self.run_cbq_query()

            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if doc["join_mo"] >= -1.79769313486231570E308 and doc["join_mo"] <= 1.79769313486231570E308]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT name FROM {0} WHERE join_mo NOT BETWEEN -1.79769313486231570E308  AND 1.79769313486231570E308 ORDER BY name".format(bucket.name)
            actual_result = self.run_cbq_query()

            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if not(doc["join_mo"] >= -1.79769313486231570E308 and doc["join_mo"] <= 1.79769313486231570E308)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)
            
    def test_concatenation_where(self):
        for bucket in self.buckets:
            self.query = 'SELECT name, skills' +\
            ' FROM %s WHERE skills[0]=("skill" || "2010")' % (bucket.name)

            actual_list = self.run_cbq_query()

            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"], "skills" : doc["skills"]}
                               for doc in self.full_list
                               if doc["skills"][0] == 'skill2010']
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)
