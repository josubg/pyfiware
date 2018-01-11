from unittest import TestCase
from unittest.mock import Mock, patch

from pyfiware import OrionManager, FiException


class DummyResponse:
    def __init__(self, status=None, data=None, headers=None):
        self.data = data.encode("utf-8")
        self.status = status
        self.headers = headers


class TestFiwareManagerQueries(TestCase):
    url = "http://127.0.0.1:1026"

    def setUp(self):
        self.fiware_manager = OrionManager(self.url)

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
            status=404,
            data='{"error":"NotFound","description":"The requested entity has not been found. Check type and id"}')))
    def test_get_by_id_empty(self):
        response = self.fiware_manager.get("wrongID")
        self.assertIsNone(response, "Not empty response")

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
            status=200,
            data='{"id":"CorrectID","type":"fake"}'
    )))
    def test_get_by_id_found(self):
        correct_id = "CorrectID"
        response = self.fiware_manager.get(correct_id)
        self.assertEqual(response["id"], correct_id, "Not correct element")

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=200,
        data='{"id":"CorrectID","type":"fake"}'
    )))
    def test_get_by_id_request(self):
        correct_id = "CorrectID"
        self.fiware_manager.get(correct_id)
        self.fiware_manager._request.assert_called_with(
            method='GET',
            url=self.url + '/v2/entities/' + correct_id,
            headers={
                'Accept': 'application/json'
            }
        )

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=500,
        data='{"error":"Everything Blew up"}'
    )))
    def test_get_by_id_error(self):
        with self.assertRaises(FiException):
            self.fiware_manager.get("NOSENSE")

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=404,
        data='{"error":"NotFound","description":"The requested entity has not been found. Check type and id"}'
    )))
    def test_get_no_result(self):
        # No Results

        response = self.fiware_manager.search(entity_type="WrongType")
        self.assertEqual(response, [], "Not empty response")

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=200,
        data='[{"id":"CorrectID","type":"fake"}]'
    )))
    def test_get_single_result(self):
        response = self.fiware_manager.search(entity_type="fake")
        self.assertEqual(type(response), list, "Not a list")
        self.assertEqual(len(response), 1, "Not unique")

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
            status=200,
            data='[{"id":"1","type":"fake"},' +
            '{"id":"2","type":"fake"}]'
    )))
    def test_get_multiple_result(self):
        response = self.fiware_manager.search()
        self.assertEqual(type(response), list, "Not a list")
        self.assertGreater(len(response), 1, "Unique results")

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=500,
        data='{"error":"Everything Blew up"}'
    )))
    def test_get_exception(self):
        # Raise exceptions

        with self.assertRaises(FiException):
            self.fiware_manager.search()

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=200,
        data='[{"id":"CorrectID","type":"fake"}]'
    )))
    def test_get_queries_id_pattern(self):
        self.fiware_manager.search(id_pattern="id*/")
        self.fiware_manager._request.assert_called_with(
            method='GET',
            url=self.url + '/v2/entities',
            headers={
                'Accept': 'application/json'
            },
            fields={
                'idPattern': "id*/"
            }
        )

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
            status=200,
            data='[{"id":"CorrectID","type":"fake"}]'
        )))
    def test_get_queries_type(self):
        self.fiware_manager.search(query="something > 500")
        self.fiware_manager._request.assert_called_with(
            method='GET',
            url=self.url + '/v2/entities',
            headers={
                'Accept': 'application/json'
            },
            fields={
                "q": "something > 500"
            }
        )


class TestFiwareManagerCreations(TestCase):
    url = "http://127.0.0.1:1026"

    def setUp(self):
        self.fiware_manager = OrionManager(self.url)

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
            status=201,
            data=''
        )))
    def test_create_blank(self):
        self.fiware_manager.create(element_id="1", element_type="fake")
        self.fiware_manager._request.assert_called_with(
            method='POST',
            url=self.url + '/v2/entities',
            headers={
                'Accept': 'application/json',
                "Content-Type": "application/json"
            },
            body={
                'id': '1',
                'type': 'fake'
            }
        )

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
            status=201,
            data=''
        )))
    def test_create_parameters(self):
        self.fiware_manager.create(element_id="1", element_type="fake", weight=300, size="100l")
        self.fiware_manager._request.assert_called_with(
            method='POST',
            url=self.url + '/v2/entities',
            headers={
                'Accept': 'application/json',
                "Content-Type": "application/json"
            },
            body={
                'id': '1',
                'type': 'fake',
                'weight': {'value': 300,
                           'type': 'Integer'
                           },
                'size': {'value': "100l",
                         'type': "Text"}
            }
        )

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=201,
        data=''
    )))
    def test_create_dict(self):
        self.fiware_manager.create(element_id="1", element_type="fake", **{'weight': 300, 'size': "100l"})
        self.fiware_manager._request.assert_called_with(
            method='POST',
            url=self.url + '/v2/entities',
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            },
            body={
                'id': '1',
                'type': 'fake',
                'weight': {'value': 300,
                           'type': 'Integer'
                           },
                'size': {'value': "100l",
                         'type': "Text"}
            }
        )

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=403,
        data=''
    )))
    def test_create_raises(self):
        with self.assertRaises(FiException):
            self.fiware_manager.create(element_id="1", element_type="fake", **{'weight': 300, 'size': "100l"})

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=201,
        data=''
    )))
    def test_create_no_id(self):
        with self.assertRaises(TypeError):
            self.fiware_manager.create(entity_type="fake")

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=201,
        data=''
    )))
    def test_create_no_type(self):
        with self.assertRaises(TypeError):
            self.fiware_manager.create(element_id="1")


class TestFiwareManagerDeletions(TestCase):
    url = "http://127.0.0.1:1026"

    def setUp(self):
        self.fiware_manager = OrionManager(self.url)

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=404,
        data=''
    )))
    def test_delete_fails(self):
        with self.assertRaises(FiException):
            self.fiware_manager.delete(entity_id="1")

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=404,
        data=''
    )))
    def test_delete_fails_silent(self):
        self.fiware_manager.delete(entity_id="1", silent=True)

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=500,
        data=''
    )))
    def test_delete_fails_no_silent(self):
        with self.assertRaises(FiException):
            self.fiware_manager.delete(entity_id="1", silent=True)

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=200,
        data=''
    )))
    def test_delete_success(self):
        self.fiware_manager.delete(entity_id="1")
        self.fiware_manager._request.assert_called_with(
            method='DELETE',
            url=self.url + '/v2/entities/' + "1",
            headers={
                'Accept': 'application/json'
            },

        )


class TestFiwareManagerPatch(TestCase):
    url = "http://127.0.0.1:1026"

    def setUp(self):
        self.fiware_manager = OrionManager(self.url)

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=204,
        data=''
    )))
    def test_patch_success(self):
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

        self.fiware_manager.patch(element_id="1", **attributes)
        self.fiware_manager._request.assert_called_with(
            method='PATCH',
            url=self.url + '/v2/entities/' + "1" + "/attrs",
            body={
                 "pressure": {
                     "value": 763,
                     "type": "Float"
                 },
                 "temperature": {
                     "value": 26.5,
                     "type": "Float"
                 }
                 },
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },

        )

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=404,
        data=''
    )))
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
            self.fiware_manager.patch(element_id="1", **attributes)

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=404,
        data=''
    )))
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

        self.fiware_manager.patch(element_id="1", silent=True, **attributes)

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=500,
        data=''
    )))
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
            self.fiware_manager.patch(element_id="1", silent=True, **attributes)


# class TestFiwareManagerScope(TestCase):
#     url = "http://127.0.0.1:1026"
#
#     def setUp(self):
#         self.fiware_manager = OrionManager(self.url)
#
#     @patch.object(OrionManager._pool_manager, "_request", Mock(return_value=DummyResponse(
#         status=201,
#         data='{}'
#     )))
#     def test_get(self):
#         self.fiware_manager.get('Fake1')
#         self.fiware_manager._request.assert_called_with(
#             method='GET',
#             url=self.url + '/v2/entities/' + "Fake1",
#             headers={
#                 'Accept': 'application/json',
#             },
#
#         )
#         self.fiware_manager.scopes = "/test"
#         self.fiware_manager.get('Fake2')
#         self.fiware_manager._request.assert_called_with(
#             method='GET',
#             url=self.url + '/v2/entities/' + "Fake1",
#             headers={
#                 'Accept': 'application/json',
#                 'Fiware-ServicePath': '/test'
#             },
#
#         )
#
#         self.fiware_manager.scopes = None
#         self.fiware_manager.get('Fake1')
#         self.fiware_manager._request.assert_called_with(
#             method='GET',
#             url=self.url + '/v2/entities/' + "Fake1",
#             headers={
#                 'Accept': 'application/json',
#             },
#         )
