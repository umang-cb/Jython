import json
import base64
import urllib
from membase.api import httplib2


class WindowFunctionsTest:

    def __init__(self):
        self.window_functions = {'RANK':
                                     {'name': 'RANK',
                                      'params': '()',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''},
                                 'NTILE':
                                     {'name': 'NTILE',
                                      'params': '(10)',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''},
                                 'FIRST_VALUE':
                                     {'name': 'FIRST_VALUE',
                                      'params': '(decimal_field)',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''
                                      },
                                 'DENSE_RANK':
                                     {'name': 'DENSE_RANK',
                                      'params': '()',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''
                                      },
                                 'RATIO_TO_REPORT':
                                     {'name': 'RATIO_TO_REPORT',
                                      'params': '(decimal_field)',
                                      'partition': 'partition by char_field',
                                      'order': '',
                                      'range': ''
                                      },
                                 'LAST_VALUE':
                                     {'name': 'LAST_VALUE',
                                      'params': '(decimal_field)',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': 'RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING'
                                      },
                                 'PERCENT_RANK':
                                     {'name': 'PERCENT_RANK',
                                      'params': '()',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''
                                      },
                                 'ROW_NUMBER':
                                     {'name': 'ROW_NUMBER',
                                      'params': '()',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''
                                      },
                                 'NTH_VALUE':
                                     {'name': 'NTH_VALUE',
                                      'params': '(decimal_field, 3)',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''
                                      },
                                 'CUME_DIST':
                                     {'name': 'CUME_DIST',
                                      'params': '()',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''
                                      },
                                 'LAG':
                                     {'name': 'LAG',
                                      'params': '(decimal_field)',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''
                                      },
                                 'LEAD':
                                     {'name': 'LEAD',
                                      'params': '(decimal_field)',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''
                                      }
                                 }

        self.aggregate_functions = {
            'AVG':
                {'name': 'AVG',
                 'params': '(decimal_field)',
                 'partition': 'partition by char_field',
                 'order': '',
                 'range': ''},
            'COUNT':
                {'name': 'COUNT',
                 'params': '(decimal_field)',
                 'partition': 'partition by char_field',
                 'order': '',
                 'range': ''
                 },
            """
            'COUNTN':
                {'name': 'COUNTN',
                 'params': '(decimal_field)',
                 'partition': 'partition by char_field',
                 'order': '',
                 'range': ''
                 },
            """
            'MAX':
                {'name': 'MAX',
                 'params': '(decimal_field)',
                 'partition': 'partition by char_field',
                 'order': '',
                 'range': ''
                 },
            """
            'MEAN':
                {'name': 'MEAN',
                 'params': '(decimal_field)',
                 'partition': 'partition by char_field',
                 'order': '',
                 'range': ''
                 },
            """
            'MIN':
                {'name': 'MIN',
                 'params': '(decimal_field)',
                 'partition': 'partition by char_field',
                 'order': '',
                 'range': ''
                 },
            'SUM':
                {'name': 'SUM',
                 'params': '(decimal_field)',
                 'partition': 'partition by char_field',
                 'order': '',
                 'range': ''
                 },
            'STDDEV':
                {'name': 'STDDEV',
                 'params': '(decimal_field)',
                 'partition': 'partition by char_field',
                 'order': '',
                 'range': ''
                 },
            """
            'STDDEV_SAMP':
                {'name': 'STDDEV_SAMP',
                 'params': '(decimal_field)',
                 'partition': 'partition by char_field',
                 'order': '',
                 'range': ''
                 },
            """
            'STDDEV_POP':
                {'name': 'STDDEV_POP',
                 'params': '(decimal_field)',
                 'partition': 'partition by char_field',
                 'order': '',
                 'range': ''
                 },
            """
            'VARIANCE':
                {'name': 'VARIANCE',
                 'params': '(decimal_field)',
                 'partition': 'partition by char_field',
                 'order': '',
                 'range': ''
                 },
            """
            'VAR':
                {'name': 'VAR',
                 'params': '(decimal_field)',
                 'partition': 'partition by char_field',
                 'order': '',
                 'range': ''
                 },
            """
            'VAR_SAMP':
                {'name': 'VAR_SAMP',
                 'params': '(decimal_field)',
                 'partition': 'partition by char_field',
                 'order': '',
                 'range': ''
                 },
            """
            'VAR_POP':
                {'name': 'VAR_POP',
                 'params': '(decimal_field)',
                 'partition': 'partition by char_field',
                 'order': '',
                 'range': ''
                 }
        }

        self.union_intersect_except = ['UNION', 'INTERSECT', 'EXCEPT']
        self.all_distinct = [' ', 'ALL', 'DISTINCT']
        self.raw_element_value = ['RAW', 'ELEMENT', 'VALUE']
        self.alphabet = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']

    def verify_results(self, failed_window_function=[], failed_aggregate_window_function=[], failed_exceptional_window_function=[]):
        if len(failed_window_function) > 0 or len(failed_aggregate_window_function) > 0 or len(failed_exceptional_window_function) > 0:
            print("--------------------- Failed Queries ----------------")
            print('failed_window_function')
            for s in failed_window_function:
                print(s)
            print('\nfailed_aggregate_window_function')
            for s in failed_aggregate_window_function:
                print(s)
            print('\nfailed_exceptional_window_function')
            for s in failed_exceptional_window_function:
                print(s)
            print("Above queries failed")
        else:
            print('Test passed')

    def _construct_window_function_call(self, v):
        return v['name'] + v['params'] + " over(" + v['partition'] + " " + v['order'] + " " + v['range'] + ")"

    def _get_auth(self, headers):
        key = 'Authorization'
        if key in headers:
            val = headers[key]
            if val.startswith("Basic "):
                return "auth: " + base64.decodestring(val[6:])
        return ""

    def _create_headers(self):
        authorization = base64.encodestring('%s:%s' % ('Administrator', 'password'))
        return {'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic %s' % authorization,
                'Connection': 'close',
                'Accept': '*/*'}

    def _http_request(self, api, method='GET', params='', headers=None, timeout=120):
        if not headers:
            headers = self._create_headers()
        while True:
            try:
                response, content = httplib2.Http(timeout=timeout).request(api, method, params, headers)
                if response['status'] in ['200', '201', '202']:
                    return True, content, response
                else:
                    try:
                        json_parsed = json.loads(content)
                    except ValueError as e:
                        json_parsed = {}
                        json_parsed["error"] = "status: {0}, content: {1}".format(response['status'], content)
                    reason = "unknown"
                    if "error" in json_parsed:
                        reason = json_parsed["error"]
                    message = '{0} {1} body: {2} headers: {3} error: {4} reason: {5} {6} {7}'. \
                        format(method, api, params, headers, response['status'], reason,
                               content.rstrip('\n'), self._get_auth(headers))
                    print(message)
                    return False, content, response
            except:
                print('ERROR..............')

    def _create_capi_headers(self, username='Administrator', password='password'):
        authorization = base64.encodestring('%s:%s' % (username, password))
        return {'Content-Type': 'application/json',
                'Authorization': 'Basic %s' % authorization,
                'Connection': 'close',
                'Accept': '*/*'}

    def _create_headers_encoded_prepared(self):
        authorization = base64.encodestring('%s:%s' % ('Administrator', 'password'))
        return {'Content-Type': 'application/json',
                'Connection': 'close',
                'Authorization': 'Basic %s' % authorization}

    def _create_headers_with_auth(self, username, password):
        authorization = base64.encodestring('%s:%s' % ('Administrator', 'password'))
        return {'Authorization': 'Basic %s' % authorization}

    def query_tool(self, query, port=8093, timeout=650, query_params={}, is_prepared=False, named_prepare=None,
                   verbose=True, encoded_plan=None, servers=None):
        key = 'prepared' if is_prepared else 'statement'
        headers = None
        content = ""
        prepared = json.dumps(query)
        if is_prepared:
            if named_prepare and encoded_plan:
                http = httplib2.Http()
                if len(servers) > 1:
                    url = "http://%s:%s/query/service" % ('http://10.112.183.101', port)
                else:
                    url = "http://%s:%s/query/service" % ('http://10.112.183.101', port)

                headers = self._create_headers_encoded_prepared()
                body = {'prepared': named_prepare, 'encoded_plan': encoded_plan}

                response, content = http.request(url, 'POST', headers=headers, body=json.dumps(body))

                return eval(content)

            elif named_prepare and not encoded_plan:
                params = 'prepared=' + urllib.quote(prepared, '~()')
                params = 'prepared="%s"' % named_prepare
            else:
                prepared = json.dumps(query)
                prepared = str(prepared.encode('utf-8'))
                params = 'prepared=' + urllib.quote(prepared, '~()')
            if 'creds' in query_params and query_params['creds']:
                headers = self._create_headers_with_auth(query_params['creds'][0]['user'].encode('utf-8'),
                                                         query_params['creds'][0]['pass'].encode('utf-8'))
            api = "http://%s:%s/query/service?%s" % ('http://10.112.183.101', port, params)
            print("%s" % api)
        else:
            params = {key: query}
            if 'creds' in query_params and query_params['creds']:
                headers = self._create_headers_with_auth(query_params['creds'][0]['user'].encode('utf-8'),
                                                         query_params['creds'][0]['pass'].encode('utf-8'))
                del query_params['creds']
            params.update(query_params)
            params = urllib.urlencode(params)
            if verbose:
                print('query params : {0}'.format(params))
            api = "http://%s:%s/query?%s" % ('10.112.183.101', port, params)

        status, content, header = self._http_request(api, 'POST', timeout=timeout, headers=headers)
        try:
            return json.loads(content)
        except ValueError:
            return content

    def run_cbq_query(self, query):
        query = query + ";"
        query = query.replace('default', "default_shadow")
        new_query = ""
        for word in query.split(" "):
            if word.find(",") != -1:
                for temp_word in word.split(","):
                    if temp_word.startswith("_"):
                        temp_word = "`" + temp_word + "`"
                    new_query = new_query + temp_word + ","
                    continue
                new_query = new_query[:-1]
            elif word.startswith("_"):
                word = "`" + word + "`"
                new_query = new_query + word + " "
            else:
                new_query = new_query + word + " "

        print('RUN CBAS QUERY %s' % new_query)
        api = 'http://10.112.183.101:8095' + "/analytics/service"
        headers = self._create_capi_headers()

        params = {'statement': new_query, 'mode': 'immediate', 'pretty': True,
                  'client_context_id': None, 'timeout': str(120) + 's'}

        scan_consistency = 'request_plus'
        if scan_consistency is not None:
            params['scan_consistency'] = scan_consistency

        scan_wait = '1m'
        if scan_wait is not None:
            params['scan_wait'] = scan_wait
            
        params = json.dumps(params)
        status, content, header = self._http_request(api, 'POST', headers=headers, params=params, timeout=70)
        if status:
            return json.loads(content)
        elif str(header['status']) == '503':
            print("Request Rejected")
            raise Exception("Request Rejected")
        elif str(header['status']) in ['500', '400']:
            json_content = json.loads(content)
            msg = json_content['errors'][0]['msg']
            if "Job requirement" in msg and "exceeds capacity" in msg:
                raise Exception("Capacity cannot meet job requirement")
            else:
                return content
        else:
            print("/analytics/service status:{0},content:{1}".format(status, content))
            raise Exception("Analytics Service API failed")

    def test_all(self):
        asc_desc = [' ', ' ASC ', ' DESC ']
        limit = [' ', ' LIMIT 10 ']
        offset = [' ', ' OFFSET 5 ']
        failed_window_function = []
        failed_aggregate_window_function = []
        failed_exceptional_window_function = []
        for k, v in self.window_functions.items():
            for op in asc_desc:
                for lim in limit:
                    for off in offset:
                        query = "select "+self._construct_window_function_call(v)+" as summa from default order by summa "+op+lim+off
                        result = self.run_cbq_query(query)
                        if "errors" in result:
                            failed_window_function.append(query)
                        else:
                            print("*******")
                            print(query)
                            print("*******")

        print('Running AGGREGATE functions')
        for k, v in self.aggregate_functions.items():
            for op in asc_desc:
                for lim in limit:
                    for off in offset:
                        query = "select "+self._construct_window_function_call(v)+" as summa from default order by summa "+op+lim+off
                        result = self.run_cbq_query(query)
                        if "errors" in result:
                            failed_aggregate_window_function.append(query)
                        else:
                            print("*******")
                            print(query)
                            print("*******")

        print('Verify query result')
        self.verify_results(failed_window_function, failed_aggregate_window_function)

    def test_tcpds(self):
        query = """SELECT *
                    FROM (
                    SELECT char_field,
                           SUM(decimal_field)sum_sales,
                           AVG(SUM(decimal_field))OVER (
                           PARTITION BY char_field) avg_quarterly_sales
                    FROM default
                    WHERE char_field IN ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z']
                    GROUP BY char_field) tmp1
                    WHERE CASE WHEN avg_quarterly_sales > 0 THEN ABS(sum_sales - avg_quarterly_sales) / avg_quarterly_sales ELSE NULL END > 0.1
                    ORDER BY avg_quarterly_sales,
                             sum_sales,
                             char_field LIMIT 100"""
        print(query)
        result = self.run_cbq_query(query)
        if result['status'] != "success":
            print("%%%%% %%%%% %%%%%% !!!!!!!!!!  FAILED ................................")

    def test_one(self):
        query = "select distinct(char_field), var_pop(decimal_field) over (partition by char_field) as agg from default"
        result = self.run_cbq_query(query)
        db_dict = {}
        for res in result['results']:
            char_key = str(res['char_field'])
            db_dict[char_key] = res['agg']
        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select var_pop(decimal_field) from default where char_field='{0}'".format(alpha)
            test_result = self.query_tool(test_query)
            test_dict[alpha] = test_result['results'][0]

        for alpha in self.alphabet:
            db_value = db_dict[alpha]
            test_value = test_dict[alpha]['$1']
            if None not in [db_value, test_value]:
                db_value = str(round(float(db_value), 4))
                test_value = str(round(float(test_value), 4))
            if test_value == 0:
                test_value = 0.0
            if db_value != test_value:
                print('Test failed:' + str(db_value) + " ---------- " + str(test_value) + alpha)
    
    def test_query(self):
        
        query = "select 1"
        result = self.run_cbq_query(query)
        print(result)


if __name__ == "__main__":
    obj = WindowFunctionsTest()
    #obj.test_all()
    #obj.test_one()
    #obj.test_tcpds()
    obj.test_query()
