import unittest
import unittest.mock as mock
import requests
from etl import Etl

TRANSFORMED_TEST_DATA = [{'GENERATION': 'aaaa11111', 'ID': 'e3867hf4-505b-18ec-aa21-acde48331122', 'NAME': 'user-e3867hf4-505b-18ec-aa21-acde48331122', 'AGE': '29', 'INSTITUTION': 'University Hospital', 'ACTIVITY': 'somthing surgery', 'COMMENT': 'To some\'s for making statues was lost in the kibana, on which deserts have. Someone, once general theories of relativity, he did not conquer. Democracy, in including physics video: physics "lightning" tour with justin morgan 52-part. Cat fancier publications?. Tense during or aviary; though generally, tame parrots should be a reaction to stress-causing. Indigenous, mostly military coup d\'état. an authoritarian military junta came to leipzig university, establishing. Castles that naister of antiquituh. Croatia (late. alaska\'s'}]
INSERTED_TEST_DATA = {'ID': 'e3867hf4-505b-18ec-aa21-acde48331122', 'NAME': 'user-e3867hf4-505b-18ec-aa21-acde48331122', 'HISTORY': [{'GENERATION_CODE': 'aaaa11111', 'AGE': '29', 'INSTITUTION': 'University Hospital', 'ACTIVITY': 'somthing surgery', 'COMMENT': 'To some\'s for making statues was lost in the kibana, on which deserts have. Someone, once general theories of relativity, he did not conquer. Democracy, in including physics video: physics "lightning" tour with justin morgan 52-part. Cat fancier publications?. Tense during or aviary; though generally, tame parrots should be a reaction to stress-causing. Indigenous, mostly military coup d\'état. an authoritarian military junta came to leipzig university, establishing. Castles that naister of antiquituh. Croatia (late. alaska\'s'}]}
TRANSFORMED_UPDATDING_TEST_DATA = [{'GENERATION': 'aaaa11112', 'ID': 'e3867hf4-505b-18ec-aa21-acde48331122', 'NAME': 'user-e3867hf4-505b-18ec-aa21-acde48331122', 'AGE': '30', 'INSTITUTION': 'University Hospital2', 'ACTIVITY': 'somthing surgery2', 'COMMENT': '22To some\'s for making statues was lost in the kibana, on which deserts have. Someone, once general theories of relativity, he did not conquer. Democracy, in including physics video: physics "lightning" tour with justin morgan 52-part. Cat fancier publications?. Tense during or aviary; though generally, tame parrots should be a reaction to stress-causing. Indigenous, mostly military coup d\'état. an authoritarian military junta came to leipzig university, establishing. Castles that naister of antiquituh. Croatia (late. alaska\'s'}]
UPDATED_TEST_DATA = {'GENERATION_CODE': 'aaaa11112', 'AGE': '30', 'INSTITUTION': 'University Hospital2', 'ACTIVITY': 'somthing surgery2', 'COMMENT': '22To some\'s for making statues was lost in the kibana, on which deserts have. Someone, once general theories of relativity, he did not conquer. Democracy, in including physics video: physics "lightning" tour with justin morgan 52-part. Cat fancier publications?. Tense during or aviary; though generally, tame parrots should be a reaction to stress-causing. Indigenous, mostly military coup d\'état. an authoritarian military junta came to leipzig university, establishing. Castles that naister of antiquituh. Croatia (late. alaska\'s'}

class TestDataPipeline(unittest.TestCase):

    def test_extract_from_api(self):
        mock_response = mock.MagicMock()
        type(mock_response).status_code = mock.PropertyMock(return_value=200)
        mock_response.json.return_value = {'nextPageToken': 'mocktoken1234', 'items':[{'id': 'filename.data', 'mediaLink': 'mockdownloadlink'}]}

        with unittest.mock.patch('requests.get', return_value=mock_response):
            result, _ = Etl.extract(self, "http://example.com/api", '')

        self.assertEqual(result, ['mockdownloadlink'])

    def test_transform_data(self):
        mock_response = mock.MagicMock()
        type(mock_response).status_code = mock.PropertyMock(return_value=200)
        mock_response.text = """ID: e3867hf4-505b-18ec-aa21-acde48331122\nNAME: mock user\nAGE: 29\nINSTITUTION: University Hospital\nACTIVITY: somthing surgery\nCOMMENT: To some's for making statues was lost in the kibana, on which deserts have. Someone, once general theories of relativity, he did not conquer. Democracy, in including physics video: physics "lightning" tour with justin morgan 52-part. Cat fancier publications?. Tense during or aviary; though generally, tame parrots should be a reaction to stress-causing. Indigenous, mostly military coup d'état. an authoritarian military junta came to leipzig university, establishing. Castles that naister of antiquituh. Croatia (late. alaska's united.nce."""

        etl = Etl()
        with unittest.mock.patch('requests.get', return_value=mock_response):
            result = etl.transform(["http://example.com/api?generation=aaaa11111&alt=media"])

        self.assertEqual(result, TRANSFORMED_TEST_DATA)

    @mock.patch("pymongo.MongoClient")
    def test_load_when_newly_insert(self, mock_mongo_client):
        mock_client = mock.MagicMock()
        mock_mongo_client.return_value = mock_client

        mock_db = mock.MagicMock()
        mock_collection = mock.MagicMock()

        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection

        mock_find_one = mock.MagicMock(return_value=0)
        mock_collection.find_one = mock_find_one

        Etl.load(self, mock_collection, TRANSFORMED_TEST_DATA)
        mock_collection.insert_one.assert_called_with(INSERTED_TEST_DATA)


    @mock.patch("pymongo.MongoClient")
    def test_load_when_update_existing_document(self, mock_mongo_client):
        mock_client = mock.MagicMock()
        mock_mongo_client.return_value = mock_client

        mock_db = mock.MagicMock()
        mock_collection = mock.MagicMock()

        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection

        mock_find_one = mock.MagicMock(return_value=INSERTED_TEST_DATA)
        mock_collection.find_one = mock_find_one

        Etl.load(self, mock_collection, TRANSFORMED_UPDATDING_TEST_DATA)
        mock_collection.update_one.assert_called_with({'ID': 'e3867hf4-505b-18ec-aa21-acde48331122'}, {'$push': {'HISTORY': UPDATED_TEST_DATA}})

if __name__ == '__main__':
    unittest.main()
