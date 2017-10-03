'''
Created on Sep 14, 2017

@author: riteshagarwal
'''
# import unittest
import json

from Jython_tasks.task import docloadertask_executor
from basetestcase import BaseTestCase

class KVTest(BaseTestCase):
    
    def read_json_tempelate(self, path):
        istream = open(path);
        with istream as data_file:    
            data = json.load(data_file)
        return data["key"], data["value"]

    def test_loadkv(self):
        path = "/Users/riteshagarwal/CB/garrit/testrunner/b/testdata.json"
        k,v = self.read_json_tempelate(path)
        docloadertask_executor().load(k,v,1000000)
