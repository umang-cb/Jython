# -*- coding: utf-8 -*-
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
import time
import datetime
from pytests.tuqquery.date_time_functions import *


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

        expected_result = [{"$1": ["key1","key2"]}]
        
        self.assertTrue(status=="success")
        self.assertTrue(result==expected_result)
        
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

    def test_meta(self):
        for bucket in self.buckets:
            expected_result = [{"name" : doc['name']} for doc in self.full_list]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))

            self.query = "SELECT distinct name FROM %s WHERE META(%s).id IS NOT NULL"  % (
                                                                                   bucket.name, bucket.name)
            actual_result = self.run_cbq_query()

            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)


    def test_clock_millis(self):
        self.query = "select clock_millis() as now"
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)
        
    def test_clock_local(self):
        self.query = "select clock_local() as now"
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)
        now = datetime.datetime.now()
        expected = "%s-%02d-%02dT" % (now.year, now.month, now.day)
        self.assertTrue(res["results"][0]["now"].startswith(expected),
                        "Result expected: %s. Actual %s" % (expected, res["results"]))
    
    def test_millis_to_local(self):
        now_millis = time.time()
        now_time = datetime.datetime.fromtimestamp(now_millis)
        expected = "%s-%02d-%02dT%02d:%02d" % (now_time.year, now_time.month, now_time.day,
                                         now_time.hour, now_time.minute)
        self.query = "select millis_to_local(%s) as now" % (now_millis * 1000)
        res = self.run_cbq_query()
        self.assertTrue(res["results"][0]["now"].startswith(expected),
                        "Result expected: %s. Actual %s" % (expected, res["results"]))
        
        self.query = "SELECT MILLIS_TO_STR(1463284740000) as full_date,\
        MILLIS_TO_STR(1463284740000, 'invalid format') as invalid_format,\
        MILLIS_TO_STR(1463284740000, '1111-11-11') as short_date;"
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [{
            "full_date": "2016-05-14T20:59:00-07:00",
            "invalid_format": "2016-05-14T20:59:00-07:00",
            "short_date": "2016-05-14"
            }]
        
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)
          
    def test_now_local(self):
        self.query = "select now_local() as now"
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)
        now = datetime.datetime.now()
        expected = "%s-%02d-%02dT" % (now.year, now.month, now.day)
        self.assertTrue(res["results"][0]["now"].startswith(expected),
                        "Result expected: %s. Actual %s" % (expected, res["results"]))
        
    def test_clock_utc(self):
        self.query = "select clock_utc() as now"
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)        

    def test_DATE_TRUNC_MILLIS(self):
        self.query = "SELECT DATE_TRUNC_MILLIS(1463284740000, 'day') as day,\
       DATE_TRUNC_MILLIS(1463284740000, 'month') as month,\
       DATE_TRUNC_MILLIS(1463284740000, 'year') as year;"
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                      {
                        "day": 1463270400000,
                        "month": 1462147200000,
                        "year": 1451696400000
                      }
                    ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)   
        
    def test_DATE_TRUNC_STR(self):
        self.query = "SELECT DATE_TRUNC_STR('2016-05-18T03:59:00Z', 'day') as day,\
        DATE_TRUNC_STR('2016-05-18T03:59:00Z', 'month') as month,\
        DATE_TRUNC_STR('2016-05-18T03:59:00Z', 'year') as year;"
        
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                      {
                        "day": "2016-05-18T00:00:00Z",
                        "month": "2016-05-01T00:00:00Z",
                        "year": "2016-01-01T00:00:00Z"
                      }
                    ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)
    
    def test_DURATION_TO_STR(self):
        self.query = "SELECT DURATION_TO_STR(2000) as microsecs,\
        DURATION_TO_STR(2000000) as millisecs,\
        DURATION_TO_STR(2000000000) as secs;"
        
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                      {
                        "microsecs": "2µs",
                        "millisecs": "2ms",
                        "secs": "2s"
                      }
                    ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)

    def test_MILLIS_TO_UTC(self):
        self.query = "SELECT MILLIS_TO_UTC(1463284740000) as full_date,\
        MILLIS_TO_UTC(1463284740000, 'invalid format') as invalid_format,\
        MILLIS_TO_UTC(1463284740000, '1111-11-11') as short_date;"
        
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                    {
                        "full_date": "2016-05-15T03:59:00Z",
                        "invalid_format": "2016-05-15T03:59:00Z",
                        "short_date": "2016-05-15"
                    }
                    ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)

    def test_STR_TO_DURATION(self):
        
        self.query = "SELECT STR_TO_DURATION('1h') as hour,\
        STR_TO_DURATION('1us') as microsecond,\
        STR_TO_DURATION('1ms') as millisecond,\
        STR_TO_DURATION('1m') as minute,\
        STR_TO_DURATION('1ns') as nanosecond,\
        STR_TO_DURATION('1s') as second;"
        
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                      {
                        "hour": 3600000000000,
                        "microsecond": 1000,
                        "millisecond": 1000000,
                        "minute": 60000000000,
                        "nanosecond": 2,
                        "second": 1000000000
                      }
                    ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)

    def test_WEEKDAY_MILLIS(self):
        self.query = "SELECT WEEKDAY_MILLIS(1486237655742) as Day;"
        
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                    {
                        "Day": "Saturday"
                    }
                   ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)

    def test_WEEKDAY_STR(self):
        self.query = "SELECT WEEKDAY_STR('2017-02-05') as Day;"
        
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                    {
                        "Day": "Sunday"
                    }
                   ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)

    def test_MILLIS(self):
        self.query = 'SELECT MILLIS("2016-05-15T03:59:00Z") as DateStringInMilliseconds;'
        
        res = self.run_cbq_query()
        self.assertTrue(res['status']=="success", "Query %s failed."%self.query)         
        
        expected = [
                    {
                        "DateStringInMilliseconds": 1463284740000
                    }
                   ]
        self.assertTrue(res['results']==expected, "Query %s failed."%self.query)
    
class DateTimeFunctionClass_cbas(DateTimeFunctionClass):
    
    def test_date_part_millis(self):
        for count in range(5):
            if count == 0:
                milliseconds = 0
            else:
                milliseconds = random.randint(658979899785, 876578987695)
            for part in PARTS:
                expected_local_query = 'SELECT DATE_PART_STR(MILLIS_TO_STR({0}), "{1}")'.format(milliseconds, part)
                expected_local_result = self.run_cbq_query(expected_local_query)
                actual_local_query = self._generate_date_part_millis_query(milliseconds, part)
                self.log.info(actual_local_query)
                actual_local_result = self.run_cbq_query(actual_local_query)
                self.assertEqual(actual_local_result["results"][0]["$1"], expected_local_result["results"][0]["$1"],
                                 "Actual result {0} and expected result {1} don't match for {2} milliseconds and \
                                 {3} parts".format(actual_local_result["results"][0], expected_local_result["results"][0]["$1"],
                                                   milliseconds, part))