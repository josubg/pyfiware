from unittest import TestCase
from unittest.mock import patch, Mock

from pyfiware import OrionManager, FiException
from test.mock.test_fiware_entities import DummyResponse


class TestFiwareManagerSubscriptions(TestCase):
    url = "http://127.0.0.1:1026"

    def setUp(self):
        self.fiware_manager = OrionManager(self.url)

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=201,
        data='',
        headers={'location': 'URL'}
    )))
    def test_subscribe(self):
        self.fiware_manager.subscribe(
            description="One subscription to rule them all",
            entities=[{"idPattern": ".*", "type": "Room"}],
            condition_expression={"q": "temperature>40"},
            condition_attributes=["temperature"],
            http="http://localhost:1234",
            notification_attrs=["temperature", "humidity"],
            expires="2016-04-05T14:00:00.00Z",
            throttling=5
            )

        self.fiware_manager._request.assert_called_with(
            method='POST',
            url=self.url + '/v2/subscriptions',
            body={
                "description": "One subscription to rule them all",
                "subject": {
                  "entities": [
                    {
                      "idPattern": ".*",
                      "type": "Room"
                    }
                  ],
                  "condition": {
                    "attrs": [
                      "temperature"
                    ],
                    "expression": {
                      "q": "temperature>40"
                    }
                  }
                },
                "notification": {
                  "http": {
                    "url": "http://localhost:1234"
                  },
                  "attrs": [
                    "temperature",
                    "humidity"
                  ]
                },
                "expires": "2016-04-05T14:00:00.00Z",
                "throttling": 5
              },
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },

        )

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=403,
        data='Something goes wrong',
    )))
    def test_subscribe_fail(self):
        with self.assertRaises(FiException):
            self.fiware_manager.subscribe(
                description="One subscription to rule them all",
                entities=[{"idPattern": ".*", "type": "Room"}],
                condition_expression={"q": "temperature>40"},
                condition_attributes=["temperature"],
                http="http://localhost:1234",
                notification_attrs=["temperature", "humidity"],
                expires="2016-04-05T14:00:00.00Z",
                throttling=5
                )

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=201,
        data='',
        headers={'location': 'URL'}
    )))
    def test_unsubscribe_url(self):
        self.fiware_manager.unsubscribe(url="v2/subscriptions/fakeURL")

        self.fiware_manager._request.assert_called_with(
            method='DELETE',
            url=self.url + '/v2/subscriptions/' + "fakeURL",

        )

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=201,
        data='',
        headers={'location': 'URL'}
    )))
    def test_unsubscribe_id(self):
        self.fiware_manager.unsubscribe(subscription_id="fakeID")

        self.fiware_manager._request.assert_called_with(
            method='DELETE',
            url=self.url + '/v2/subscriptions/'+"fakeID",
        )

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=401,
        data='Something goes wrong',
    )))
    def test_unsubscribe_fails(self):
        with self.assertRaises(FiException):
            self.fiware_manager.unsubscribe(url="v2/subscriptions/fakeURL")

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=201,
        data="""{
            "description": "One subscription to rule them all",
            "subject": {
              "entities": [
                {
                  "idPattern": ".*",
                  "type": "Room"
                }
              ],
              "condition": {
                "attrs": [
                  "temperature"
                ],
                "expression": {
                  "q": "temperature>40"
                }
              }
            },
            "notification": {
              "http": {
                "url": "http://localhost:1234"
              },
              "attrs": [
                "temperature",
                "humidity"
              ]
            },
            "expires": "2016-04-05T14:00:00.00Z",
            "throttling": 5
          }"""
    )))
    def test_subscription(self):
        self.fiware_manager.subscriptions(subscription_id="fakeURL")

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=403,
        data='Something goes wrong',
    )))
    def test_subscription_fails(self):
        with self.assertRaises(FiException):
            self.fiware_manager.subscriptions(subscription_id="fakeURL")

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=201,
        data=''
    )))
    def test_subscription_update(self):
        ID = "FAKE"

        self.fiware_manager.subscription_update(subscription_id=ID, status="active")

        self.fiware_manager._request.assert_called_with(
            method='PATCH',
            url=self.url + '/v2/subscriptions/' + ID,
            body={
                "status": "active"
            },
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'}
        )

    @patch.object(OrionManager, "_request", Mock(return_value=DummyResponse(
        status=401,
        data=''
    )))
    def test_subscription_update_fail(self):
        ID = "WRONG"
        with self.assertRaises(FiException):
            self.fiware_manager.subscription_update(subscription_id=ID, status="active")
