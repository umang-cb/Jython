from cbas.cbas_base import CBASBaseTest
from couchbase_helper.documentgenerator import DocumentGenerator


class CBASInferBase(CBASBaseTest):

    def setUp(self):
        super(CBASInferBase, self).setUp()

    def remove_samples_from_response(self, infer_dict_list):
        if isinstance(infer_dict_list, dict):
            infer_dict_list = self.remove_sub_dict_prop(infer_dict_list)
        else:
            for index, tem_result in enumerate(infer_dict_list):
                if "properties" in tem_result:
                    for prop_key, _ in tem_result["properties"].items():
                        if "properties" in infer_dict_list[index]["properties"][prop_key]:
                            infer_dict_list[index] = self.remove_sub_dict_prop(infer_dict_list[index])

        return infer_dict_list

    def remove_sub_dict_prop(self, infer_dict):
        if "properties" in infer_dict:
            for prop_key, _ in infer_dict["properties"].items():
                infer_dict["properties"][prop_key].pop("samples", None)
                if "properties" in infer_dict["properties"][prop_key]:
                    infer_dict["properties"][prop_key] = self.remove_sub_dict_prop(infer_dict["properties"][prop_key])

        return infer_dict

    def generate_document_with_type_array_and_object(self):
        age = range(70)
        first = [["james", "last"], ["first", "sharon"], ["nathan"]]
        profession = [{"role": "doctor", "specialization": "heart"}, {"role": "doctor", "specialization": "neuro"},
                      {"role": "nurse"}]
        template = '{{ "number": {0}, "first_name": {1} , "profession":{2}, "mutated":0}}'
        gen_load = DocumentGenerator('test_docs_custom', template, age, first, profession, start=0, end=self.num_items)
        return gen_load


class CBASInferCompareJson:
    results = [
        {
            "id": "infer_schema_on_array",
            "json": [{u'#docs': 1, u'type': u'object', u'Flavor': u'', u'%docs': 25.0, u'properties': {
                u'a': {u'maxItems': 2, u'#docs': 1, u'type': u'array', u'minItems': 2, u'%docs': 100.0,
                u'samples': [[1, 2]], u'items': u'number'}}}, {u'#docs': 1, u'type': u'object', u'Flavor': u'', u'%docs': 25.0,
                                                u'properties': {u'a': {u'#docs': 1, u'type': u'object', u'%docs': 100.0,
                                                                       u'properties': {
                                                                           u'b': {u'#docs': 1, u'type': u'number',
                                                                                  u'%docs': 100.0},
                                                                           u'c': {u'#docs': 1, u'type': u'string',
                                                                                  u'%docs': 100.0},
                                                                           u'd': {u'#docs': 1, u'type': u'string',
                                                                                  u'%docs': 100.0},
                                                                           u'e': {u'#docs': 1, u'type': u'object',
                                                                                  u'%docs': 100.0, u'properties': {
                                                                                   u'f': {u'#docs': 1,
                                                                                          u'type': u'number',
                                                                                          u'%docs': 100.0}}}}}}},
                     {u'#docs': 1, u'type': u'object', u'Flavor': u'\'a\' = "aval"', u'%docs': 25.0,
                      u'properties': {u'a': {u'#docs': 1, u'type': u'string', u'samples': [u'aval'], u'%docs': 100.0}}},
                     {u'#docs': 1, u'type': u'object', u'Flavor': u"'a' = 1", u'%docs': 25.0,
                      u'properties': {u'a': {u'#docs': 1, u'type': u'number', u'samples': [1], u'%docs': 100.0}}}]
        },
        {
            "id": "analytics_supports_infer_schema",
            "json": {u'#docs': 10, u'type': u'object',
                     u'Flavor': u'\'first_name\' = "james", \'mutated\' = 0, \'profession\' = "doctor"', u'%docs': 100.0,
                     u'properties': {u'mutated': {u'#docs': 10, u'type': u'number', u'%docs': 100.0},
                                     u'profession': {u'#docs': 10, u'type': u'string', u'%docs': 100.0},
                                     u'number': {u'#docs': 10, u'type': u'number', u'%docs': 100.0},
                                     u'_id': {u'#docs': 10, u'type': u'string', u'%docs': 100.0},
                                     u'first_name': {u'#docs': 10, u'type': u'string', u'%docs': 100.0}}}
        }
    ]

    def __init__(self):
        pass

    @staticmethod
    def get_json(test_id):
        for result in CBASInferCompareJson.results:
            if result['id'] == test_id:
                return result['json']
        return None
