import json
from unittest import TestCase

from pyfiware import OrionConnector, FiException
from urllib3 import PoolManager


class FiwareTestCase(TestCase):
    url = "http://127.0.0.1:1026"
    service_paths = ["/#","/", "/test/testa"]

    def _clear_entities(self):

        for service_path in self.service_paths:
            response = self.fiware_manager._request(
                method='get', url=self.url + "/v2/entities", headers={'Accept': 'application/json'})
            if response.status // 200 != 1:
                raise Exception(response.data)
            entities = json.loads(response.data.decode('utf-8'))
            for entity in entities:
                response = self.fiware_manager._request(
                    method='DELETE', url=self.url + "/v2/entities/" + entity["id"],
                    headers={'Accept': 'application/json', "Fiware-ServicePath": service_path}, fields={'type': entity["type"]})

                if (response.status // 200 != 1) and (response.status != 404) :
                    raise Exception(response.data)

    def _get_entity(self, e_id):
        response = self.fiware_manager._request(
            method='get', url=self.url + '/v2/entities/' + e_id, headers={'Accept': 'application/json'})

        if response.status // 200 != 1:
            raise Exception(response.status, response.data)
        return json.loads(response.data.decode('utf-8'))

    def _create_entity(self, entity):

        response = self.fiware_manager._request(
            method='POST', url=self.url + '/v2/entities', headers={'Content-Type': 'application/json'},
            body=entity)
        if response.status // 200 != 1:
            raise Exception(response.data)

    def tearDown(self):
        self._clear_entities()

    def setUp(self):
        self.fiware_manager = OrionConnector(self.url)
        self._clear_entities()


class TestFiwareManagerQueries(FiwareTestCase):
    def setUp(self):
        super().setUp()
        self._create_entity({
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
        })
        self._create_entity({
           "id": "Fake2",
           "type": "FakeType",
           "temperature": {
               "value": 27,
               "type": "Float"
           },
           "pressure": {
               "value": 720,
               "type": "Integer"
           }
        })
        self._create_entity({
            "id": "Fake3",
            "type": "Fake",
            "temperature": {
                "value": 23,
                "type": "Float"
            },
            "pressure": {
                "value": 720,
                "type": "Integer"
            }
        })

    def test_get_by_id_empty(self):
        response = self.fiware_manager.get("wrongID")
        self.assertIsNone(response, "Not empty response")

    def test_get_by_id_found(self):
        correct_id = "Fake1"
        response = self.fiware_manager.get(correct_id)
        self.assertEqual(response["id"], correct_id, "Not correct element")

    def test_get_by_id_exception(self):
        with self.assertRaises(FiException):
            self.fiware_manager.get("NOSEN<...SE")

    def test_get_no_result(self):
        response = self.fiware_manager.search(entity_type="WrongType")
        self.assertEqual(response, [], "Not empty response")

    def test_get_single_result(self):
        response = self.fiware_manager.search(entity_type="FakeType")
        self.assertEqual(type(response), list, "Not a list")
        self.assertGreater(len(response), 0, "Empty")
        self.assertEqual(len(response), 1, "Not unique")

    def test_get_multiple_result(self):
        response = self.fiware_manager.search(entity_type="Fake")
        self.assertEqual(type(response), list, "Not a list")
        self.assertGreater(len(response), 1, "Unique results")

    def test_get_exception(self):
        # Raise exceptions
        with self.assertRaises(FiException):
            self.fiware_manager.search("><")

    def test_get_type(self):
        response = self.fiware_manager.search(entity_type="FakeType")
        self.assertEqual(type(response), list, "Not a list")
        self.assertEqual(len(response), 1, "Not unique")

    def test_get_id_pattern(self):
        response = self.fiware_manager.search(id_pattern="Fake.*")
        self.assertGreater(len(response), 1, "Unique results")

    def test_get_query(self):
        response = self.fiware_manager.search(query="temperature > 25")
        self.assertEqual(len(response), 1, "Unique results")


class TestFiwareManagerCreations(FiwareTestCase):

    def test_create_blank(self):
        self.fiware_manager.create(element_id="Fake1", element_type="FakeType")
        element = self._get_entity("Fake1")
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
        element = self._get_entity("Fake1")
        del element["weight"]["metadata"]
        del element["size"]["metadata"]
        self.assertEqual(element["id"], "Fake1")
        self.assertEqual(element["type"], "FakeType")
        self.assertEqual(element["weight"], {'value': 300, 'type': 'Integer'})
        self.assertEqual(element["size"], {'value': '100l', 'type': 'Text'})

    def test_create_dict(self):
        self.fiware_manager.create(element_id="Fake1", element_type="FakeType", **{'weight': 300, 'size': "100l"})
        element = self._get_entity("Fake1")
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


class TestFiwareManagerDeletions(FiwareTestCase):

    def setUp(self):
        super().setUp()
        self._create_entity({
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
            })

    def test_delete_fails(self):
        with self.assertRaises(FiException):
            self.fiware_manager.delete(entity_id="WrongID")

    def test_delete_fails_silent(self):
        self.fiware_manager.delete(entity_id="WrongID", silent=True)

    def test_delete_fails_no_silent(self):
        with self.assertRaises(FiException):
            self.fiware_manager.delete(entity_id="ss<ss", silent=True)

    def test_delete_success(self):

        self.assertEqual(self._get_entity("Fake1")["id"], "Fake1", "Preexisting entity not found")
        self.fiware_manager.delete(entity_id="Fake1")
        with self.assertRaises(Exception) as ex:
            self._get_entity("Fake1")
        self.assertEqual(ex.exception.args[0], 404, "Error message not a 404")


class TestFiwareManagerPatch(FiwareTestCase):

    def setUp(self):
        super().setUp()
        self._create_entity({
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
            })

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


class TestFiwareManagerScope(FiwareTestCase):

    def setUp(self):
        super().setUp()
        self._create_entity({
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
            })
        self.fiware_manager.scopes = "/test/testa"
        self._create_entity({
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
        })
        self.fiware_manager.scopes = None

    def test_get(self):
        self.assertIsNotNone(self.fiware_manager.get('Fake1'))
        self.assertIsNotNone(self.fiware_manager.get('Fake2'))
        self.fiware_manager.scopes = "/"

        self.assertIsNotNone(self.fiware_manager.get('Fake1'))
        self.assertIsNone(self.fiware_manager.get('Fake2'))
        self.fiware_manager.scopes = "/#"

        self.assertIsNotNone(self.fiware_manager.get('Fake1'))
        self.assertIsNotNone(self.fiware_manager.get('Fake2'))

        self.fiware_manager.scopes = "/test/#"
        self.assertIsNone(self.fiware_manager.get('Fake1'))
        self.assertIsNotNone(self.fiware_manager.get('Fake2'))

        self.fiware_manager.scopes = "/test/testb"
        self.assertIsNone(self.fiware_manager.get('Fake1'))
        self.assertIsNone(self.fiware_manager.get('Fake2'))

        self.fiware_manager.scopes = None
        self.assertIsNotNone(self.fiware_manager.get('Fake1'))
        self.assertIsNotNone(self.fiware_manager.get('Fake2'))