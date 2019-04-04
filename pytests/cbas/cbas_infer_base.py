from cbas.cbas_base import CBASBaseTest
from couchbase_helper.documentgenerator import DocumentGenerator


class CBASInferBase(CBASBaseTest):

    def setUp(self):
        super(CBASInferBase, self).setUp()

    def remove_samples_from_result(self, infer_dict_list):
        for index, tem_result in enumerate(infer_dict_list):
            if "properties" in tem_result:
                for prop_key, _ in tem_result["properties"].items():
                    infer_dict_list[index]["properties"][prop_key].pop("samples", None)
                    if "properties" in infer_dict_list[index]["properties"][prop_key]:
                        self.remove_sub_dict_prop(infer_dict_list[index]["properties"][prop_key])

    def remove_sub_dict_prop(self, infer_dict):
        if "properties" in infer_dict:
            for prop_key, _ in infer_dict["properties"].items():
                infer_dict["properties"][prop_key].pop("samples", None)
                if "properties" in infer_dict["properties"][prop_key]:
                    self.remove_sub_dict_prop(infer_dict["properties"][prop_key])

    def generate_document_with_type_array_and_object(self):
        age = range(70)
        first = [["james", "last"], ["first", "sharon"], ["nathan"]]
        profession = [{"role": "doctor", "specialization": "heart"}, {"role": "doctor", "specialization": "neuro"}, {"role": "nurse"}]
        template = '{{ "number": {0}, "first_name": {1} , "profession":{2}, "mutated":0}}'
        gen_load = DocumentGenerator('test_docs_custom', template, age, first, profession, start=0, end=self.num_items)
        return gen_load


class CBASInferCompareJson:
    results = [
        {
            "id": "infer_schema_on_array",
            "json": "Unauthorized user"
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
