import json
from unittest import TestCase

from pyfiware import FiwareManager, FiException
from urllib3 import PoolManager

class DummyResponse:
    def __init__(self, status, data):
        self.data = data.encode("utf-8")
        self.status = status


class TestFiwareManagerQueries(TestCase):
    url = "http://127.0.0.1:1026"

    def __delete_fakes(self):
        response = self.PM.request(
            method='post', url=self.url + '/v2/op/update', headers={'Content-Type': 'application/json'},
            body=json.dumps({
                "actionType": "delete",
                "entities": [
                    {
                          "id": "Fake1",
                    }, {
                       "id": "Fake2",

                    }, {
                        "id": "Fake3",
                    }
                ]
            }))

        if response.status // 200 != 1 and response.status != 404:
            raise Exception(response.data)

    def tearDown(self):
        self.__delete_fakes()

    def setUp(self):
        self.fiware_manager = FiwareManager(self.url)
        self.PM = PoolManager()
        self.__delete_fakes()

        response = self.PM.request(
            method='POST', url=self.url+'/v2/entities', headers={'Content-Type': 'application/json'},
            body=json.dumps({
              "id": "Fake1",
              "type": "Fake",
              "temperature": {
                "value": 23,
                "type": "Float"
              },
              "pressure": {
                "value": 720,
                "type": "Integer"
              }
            }))
        if response.status // 200 != 1:
            raise Exception(response.data)
        response = self.PM.request(
            method='POST', url=self.url + '/v2/entities', headers={'Content-Type': 'application/json'},
            body=json.dumps({
               "id": "Fake2",
               "type": "Fake",
               "temperature": {
                   "value": 23,
                   "type": "Float"
               },
               "pressure": {
                   "value": 720,
                   "type": "Integer"
               }
            }))
        if response.status // 200 != 1:
            raise Exception(response.data)
        response = self.PM.request(
            method='POST', url=self.url + '/v2/entities', headers={'Content-Type': 'application/json'},
            body=json.dumps({
                "id": "Fake3",
                "type": "FakeType",
                "temperature": {
                    "value": 27,
                    "type": "Float"
                },
                "pressure": {
                    "value": 720,
                    "type": "Integer"
                }
            }))
        if response.status // 200 != 1:
            raise Exception(response.data)

    def test_get_by_id_empty(self):
        response = self.fiware_manager.get_by_id("wrongID")
        self.assertIsNone(response, "Not empty response")

    def test_get_by_id_found(self):
        correct_id = "Fake1"
        response = self.fiware_manager.get_by_id(correct_id)
        self.assertEqual(response["id"], correct_id, "Not correct element")

    def test_get_by_id_exception(self):
        with self.assertRaises(FiException):
            self.fiware_manager.get_by_id("NOSEN<...SE")


    def test_get_no_result(self):
        # No Results
        response = self.fiware_manager.get(entity_type="WrongType")
        self.assertEqual(response, [], "Not empty response")

    def test_get_single_result(self):
        response = self.fiware_manager.get(entity_type="FakeType")
        self.assertEqual(type(response), list, "Not a list")
        self.assertGreater(len(response), 0, "Empty")
        self.assertEqual(len(response), 1, "Not unique")

    def test_get_multiple_result(self):
        response = self.fiware_manager.get(entity_type="Fake")
        self.assertEqual(type(response), list, "Not a list")
        self.assertGreater(len(response), 1, "Unique results")

    def test_get_exception(self):
        # Raise exceptions
        with self.assertRaises(FiException):
            self.fiware_manager.get("><")

    def test_get_type(self):
        response = self.fiware_manager.get(entity_type="FakeType")
        self.assertEqual(type(response), list, "Not a list")
        self.assertEqual(len(response), 1, "Not unique")

    def test_get_id_pattern(self):
        response = self.fiware_manager.get(id_pattern="Fake.*")
        self.assertGreater(len(response), 1, "Unique results")

    def test_get_query(self):
        response = self.fiware_manager.get(query="temperature > 25")
        self.assertEqual(len(response), 1, "Unique results")


class TestFiwareManagerCreations(TestCase):
    url = "http://127.0.0.1:1026/"

    def __get_entity(self, e_id):
        response = self.PM.request(
            method='get', url=self.url + 'v2/entities/'+e_id, headers={'Accept': 'application/json'})

        if response.status // 200 != 1:
            raise Exception(response.data)
        return json.loads(response.data.decode('utf-8'))

    def __delete_fakes(self):
        response = self.PM.request(
            method='post', url=self.url + 'v2/op/update', headers={'Content-Type': 'application/json'},
            body=json.dumps({
                "actionType": "delete",
                "entities": [
                    {
                          "id": "Fake1",
                    }
                ]
            }))

        if response.status // 200 != 1 and response.status != 404:
            raise Exception(response.data)

    def tearDown(self):
        self.__delete_fakes()


    def setUp(self):
        self.fiware_manager = FiwareManager(self.url)
        self.PM = PoolManager()
        self.__delete_fakes()

    def test_create_blank(self):
        self.fiware_manager.create(element_id="Fake1", element_type="FakeType")
        element = self.__get_entity("Fake1")
        self.assertEqual(element["id"], "Fake1")
        self.assertEqual(element["type"], "FakeType")

    def test_create_no_id(self):
        with self.assertRaises(Exception):
            self.fiware_manager.create(entity_type="fake")

    def test_create_no_type(self):
        with self.assertRaises(Exception):
            self.fiware_manager.create(element_id="Fake1")

    def test_create_parameters(self):
        self.fiware_manager.create(element_id="Fake1", element_type="FakeType", weight=300, size="100l")
        element = self.__get_entity("Fake1")
        del element["weight"]["metadata"]
        del element["size"]["metadata"]
        self.assertEqual(element["id"], "Fake1")
        self.assertEqual(element["type"], "FakeType")
        self.assertEqual(element["weight"], {'value': 300, 'type': 'Integer'})
        self.assertEqual(element["size"], {'value': '100l', 'type': 'Text'})

    def test_create_dict(self):
        self.fiware_manager.create(element_id="Fake1", element_type="FakeType", **{'weight': 300, 'size': "100l"})
        element = self.__get_entity("Fake1")
        del element["weight"]["metadata"]
        del element["size"]["metadata"]
        self.assertEqual(element["id"], "Fake1")
        self.assertEqual(element["type"], "FakeType")
        self.assertEqual(element["weight"], {'value': 300, 'type': 'Integer'})
        self.assertEqual(element["size"], {'value': '100l', 'type': 'Text'})

    def test_create_raises(self):
        self.fiware_manager.create(element_id="Fake1", element_type="fake", **{'weight': 300, 'size': "100l"})
        with self.assertRaises(FiException):
            self.fiware_manager.create(element_id="Fake1", element_type="fake", **{'weight': 300, 'size': "100l"})


class TestFiwareManagerDeletions(TestCase):
    url = "http://127.0.0.1:1026"

    def _get_entity(self, e_id):
        response = self.PM.request(
            method='get', url=self.url + '/v2/entities/' + e_id, headers={'Accept': 'application/json'})

        if response.status // 200 != 1:
            raise Exception(response.status, response.data)
        return json.loads(response.data.decode('utf-8'))

    def _delete_fakes(self):
        response = self.PM.request(
            method='post', url=self.url + '/v2/op/update', headers={'Content-Type': 'application/json'},
            body=json.dumps({
                "actionType": "delete",
                "entities": [
                    {
                        "id": "Fake1",
                    }
                ]
            }))

        if response.status // 200 != 1 and response.status != 404:
            raise Exception(response.status, response.data)

    def tearDown(self):
        self._delete_fakes()

    def setUp(self):
        self.fiware_manager = FiwareManager(self.url)
        self.PM = PoolManager()
        self._delete_fakes()
        response = self.PM.request(
            method='POST', url=self.url + '/v2/entities', headers={'Content-Type': 'application/json'},
            body=json.dumps({
                "id": "Fake1",
                "type": "Fake",
                "temperature": {
                    "value": 23,
                    "type": "Float"
                },
                "pressure": {
                    "value": 720,
                    "type": "Integer"
                }
            }))
        if response.status // 200 != 1:
            raise Exception(response.data)

    def test_delete_fails(self):
        with self.assertRaises(FiException):
            self.fiware_manager.delete(element_id="WrongID")

    def test_delete_fails_silent(self):
        self.fiware_manager.delete(element_id="WrongID", silent=True)

    def test_delete_fails_no_silent(self):
        with self.assertRaises(FiException):
            self.fiware_manager.delete(element_id="ss<ss", silent=True)

    def test_delete_success(self):

        self.assertEqual(self._get_entity("Fake1")["id"], "Fake1", "Preexisting entity not found")
        self.fiware_manager.delete(element_id="Fake1")
        with self.assertRaises(Exception) as ex:
            self._get_entity("Fake1")
        self.assertEqual(ex.exception.args[0], 404, "Error message not a 404")


class TestFiwareManagerPatch(TestCase):
    url = "http://130.206.117.164"

    def _get_entity(self, e_id):
        response = self.PM.request(
            method='get', url=self.url + '/v2/entities/' + e_id, headers={'Accept': 'application/json'})

        if response.status // 200 != 1:
            raise Exception(response.status, response.data)
        return json.loads(response.data.decode('utf-8'))

    def _delete_fakes(self):
        response = self.PM.request(
            method='post', url=self.url + '/v2/op/update', headers={'Content-Type': 'application/json'},
            body=json.dumps({
                "actionType": "delete",
                "entities": [
                    {
                        "id": "Fake1",
                    }
                ]
            }))

        if response.status // 200 != 1 and response.status != 404:
            raise Exception(response.status, response.data)

    def tearDown(self):
        self._delete_fakes()

    def setUp(self):
        self.fiware_manager = FiwareManager(self.url)
        self.PM = PoolManager()
        self._delete_fakes()
        response = self.PM.request(
            method='POST', url=self.url + '/v2/entities', headers={'Content-Type': 'application/json'},
            body=json.dumps({
                "id": "Fake1",
                "type": "Fake",
                "temperature": {
                    "value": 23,
                    "type": "Float"
                },
                "pressure": {
                    "value": 720,
                    "type": "Integer"
                }
            }))
        if response.status // 200 != 1:
            raise Exception(response.data)



    def test_patch_fails(self):
        attributes = {
            "temperature": {
                "value": 26.5,
                "type": "Float"
            },
            "pressure": {
                "value": 763,
                "type": "Float"
            }
        }

        with self.assertRaises(FiException):
            self.fiware_manager.patch(element_id="WrongID", **attributes)

    def test_patch_fails_silent(self):
        attributes = {
            "temperature": {
                "value": 26.5,
                "type": "Float"
            },
            "pressure": {
                "value": 763,
                "type": "Float"
            }
        }

        self.fiware_manager.patch(element_id="WrongID", silent=True, **attributes)

    def test_patch_fails_no_silent(self):
        attributes = {
            "temperature": {
                "value": 26.5,
                "type": "Float"
            },
            "pressure": {
                "value": 763,
                "type": "Float"
            }
        }

        with self.assertRaises(FiException):
            self.fiware_manager.patch(element_id="WrongID>", silent=True, **attributes)

    def test_patch_success(self):
        attributes = {
            "temperature": {
                "value": 30,
                "type": "Float"
            },
            "pressure": {
                "value": 763,
                "type": "Float"
            }
        }

        self.fiware_manager.patch(element_id="Fake1", **attributes)
        e = self._get_entity("Fake1")

        self.assertEqual(e["temperature"]["value"], 30)
        self.assertEqual(e["pressure"]["value"], 763)

    def test_patch_success(self):
        attributes = {
            "temperature": {
                "value": 30,
                "type": "Float"
            },
            "pressure": {
                "value": 763,
                "type": "Float"
            }
        }

        self.fiware_manager.patch(element_id="Fake1", **attributes)
        e = self._get_entity("Fake1")

        self.assertEqual(e["temperature"]["value"], 30)
        self.assertEqual(e["pressure"]["value"], 763)