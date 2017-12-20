from unittest import TestCase
from unittest.mock import Mock, patch

from pyfiware import FiwareManager, FiException


class DummyResponse:
    def __init__(self, status, data):
        self.data = data.encode("utf-8")
        self.status = status


class TestFiwareManagerQueries(TestCase):
    url = "http://130.206.117.164/v2/"

    def setUp(self):
        self.fiware_manager = FiwareManager(self.url)

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
            status=404,
            data='{"error":"NotFound","description":"The requested entity has not been found. Check type and id"}')))
    def test_get_by_id_empty(self):
        response = self.fiware_manager.get_by_id("wrongID")
        self.assertIsNone(response, "Not empty response")

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
            status=200,
            data='{"id":"CorrectID","type":"fake"}'
    )))
    def test_get_by_id_found(self):
        correct_id = "CorrectID"
        response = self.fiware_manager.get_by_id(correct_id)
        self.assertEqual(response["id"], correct_id, "Not correct element")

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
        status=200,
        data='{"id":"CorrectID","type":"fake"}'
    )))
    def test_get_by_id_request(self):
        correct_id = "CorrectID"
        self.fiware_manager.get_by_id(correct_id)
        self.fiware_manager.request.assert_called_with(
            method='GET',
            url=self.url + 'entities/' + correct_id,
            headers={
                'Accept': 'application/json'
            }
        )

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
        status=500,
        data='{"error":"Everything Blew up"}'
    )))
    def test_get_by_id_error(self):
        with self.assertRaises(FiException):
            self.fiware_manager.get_by_id("NOSENSE")

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
        status=404,
        data='{"error":"NotFound","description":"The requested entity has not been found. Check type and id"}'
    )))
    def test_get_no_result(self):
        # No Results

        response = self.fiware_manager.get(entity_type="WrongType")
        self.assertEqual(response, [], "Not empty response")

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
        status=200,
        data='[{"id":"CorrectID","type":"fake"}]'
    )))
    def test_get_single_result(self):
        response = self.fiware_manager.get(entity_type="fake")
        self.assertEqual(type(response), list, "Not a list")
        self.assertEqual(len(response), 1, "Not unique")

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
            status=200,
            data='[{"id":"1","type":"fake"},' +
            '{"id":"2","type":"fake"}]'
    )))
    def test_get_multiple_result(self):
        response = self.fiware_manager.get()
        self.assertEqual(type(response), list, "Not a list")
        self.assertGreater(len(response), 1, "Unique results")

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
        status=500,
        data='{"error":"Everything Blew up"}'
    )))
    def test_get_exception(self):
        # Raise exceptions

        with self.assertRaises(FiException):
            self.fiware_manager.get()

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
        status=200,
        data='[{"id":"CorrectID","type":"fake"}]'
    )))
    def test_get_queries_id_pattern(self):
        self.fiware_manager.get(id_pattern="id*/")
        self.fiware_manager.request.assert_called_with(
            method='GET',
            url=self.url + 'entities',
            headers={
                'Accept': 'application/json'
            },
            fields={
                'idPattern': "id*/"
            }
        )

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
            status=200,
            data='[{"id":"CorrectID","type":"fake"}]'
        )))
    def test_get_queries_type(self):
        self.fiware_manager.get(query="something > 500")
        self.fiware_manager.request.assert_called_with(
            method='GET',
            url=self.url + 'entities',
            headers={
                'Accept': 'application/json'
            },
            fields={
                "q": "something > 500"
            }
        )


class TestFiwareManagerCreations(TestCase):
    url = "http://130.206.117.164/v2/"

    def setUp(self):
        self.fiware_manager = FiwareManager(self.url)

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
            status=201,
            data=''
        )))
    def test_create_blank(self):
        self.fiware_manager.create(element_id="1", element_type="fake")
        self.fiware_manager.request.assert_called_with(
            method='POST',
            url=self.url + 'entities',
            headers={
                'Accept': 'application/json',
                "Content-Type": "application/json"
            },
            body={
                'id': '1',
                'type': 'fake'
            }
        )

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
            status=201,
            data=''
        )))
    def test_create_parameters(self):
        self.fiware_manager.create(element_id="1", element_type="fake", weight=300, size="100l")
        self.fiware_manager.request.assert_called_with(
            method='POST',
            url=self.url + 'entities',
            headers={
                'Accept': 'application/json',
                "Content-Type": "application/json"
            },
            body={
                'id': '1',
                'type': 'fake',
                'weight': 300,
                'size': "100l"
            }
        )

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
        status=201,
        data=''
    )))
    def test_create_dict(self):
        self.fiware_manager.create(element_id="1", element_type="fake", **{'weight': 300, 'size': "100l"})
        self.fiware_manager.request.assert_called_with(
            method='POST',
            url=self.url + 'entities',
            headers={
                'Accept': 'application/json'
            },
            body={
                'id': '1',
                'type': 'fake',
                'weight': 300,
                'size': "100l"
            }
        )

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
        status=403,
        data=''
    )))
    def test_create_raises(self):
        with self.assertRaises(FiException):
            self.fiware_manager.create(element_id="1", element_type="fake", **{'weight': 300, 'size': "100l"})

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
        status=201,
        data=''
    )))
    def test_create_no_id(self):
        with self.assertRaises(Exception):
            self.fiware_manager.create(entity_type="fake")

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
        status=201,
        data=''
    )))
    def test_create_no_type(self):
        with self.assertRaises(Exception):
            self.fiware_manager.create(element_id="1")


class TestFiwareManagerDeletions(TestCase):
    url = "http://130.206.117.164/v2/"

    def setUp(self):
        self.fiware_manager = FiwareManager(self.url)

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
        status=404,
        data=''
    )))
    def test_delete_fails(self):
        with self.assertRaises(FiException):
            self.fiware_manager.delete(element_id="1")

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
        status=404,
        data=''
    )))
    def test_delete_fails_silent(self):
        self.fiware_manager.delete(element_id="1", silent=True)

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
        status=500,
        data=''
    )))
    def test_delete_fails_no_silent(self):
        with self.assertRaises(FiException):
            self.fiware_manager.delete(element_id="1", silent=True)

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
        status=200,
        data=''
    )))
    def test_delete_success(self):
        self.fiware_manager.delete(element_id="1")
        self.fiware_manager.request.assert_called_with(
            method='DELETE',
            url=self.url + 'entities/' + "1",
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'

            },

        )


class TestFiwareManagerPatch(TestCase):
    url = "http://130.206.117.164/v2/"

    def setUp(self):
        self.fiware_manager = FiwareManager(self.url)

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
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
        self.fiware_manager.request.assert_called_with(
            method='PATCH',
            url=self.url + 'entities/' + "1"+ "/attrs",
            body='{'
                 '"pressure": {'
                 '"value": 763, '
                 '"type": "Float"'
                 '}, '
                 '"temperature": {'
                 '"value": 26.5, '
                 '"type": "Float"'
                 '}'
                 '}',
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },

        )

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
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

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
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

    @patch.object(FiwareManager, "request", Mock(return_value=DummyResponse(
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
            self.fiware_manager.patch(element_id="1", silent=True,**attributes)
