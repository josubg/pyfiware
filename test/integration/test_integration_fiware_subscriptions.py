import json
from unittest import TestCase
from pyfiware import OrionManager, FiException


class TestCaseFiwareSubscriptions(TestCase):
    url = "http://127.0.0.1:1026"

    def setUp(self):
        self.fiware_manager = OrionManager(self.url)

    def tearDown(self):
        self._clear_subscriptions()

    def _clear_subscriptions(self):
        response = self.fiware_manager._request(
            method='get', url=self.url + "/v2/subscriptions", headers={'Accept': 'application/json'})
        if response.status // 200 != 1:
            raise Exception(response.data)
        for entity in json.loads(response.data.decode('utf-8')):
            response = self.fiware_manager._request(
                method='delete', url=self.url + "/v2/subscriptions/" + entity["id"],
                headers={'Accept': 'application/json'})

            if response.status // 200 != 1:
                raise Exception(response.data)

    def _get_subscription(self, url):
        response = self.fiware_manager._request(
            method='get', url=self.url + url, headers={'Accept': 'application/json'})

        if response.status // 200 != 1:
            raise Exception(response.data)
        return json.loads(response.data.decode('utf-8'))

    def _delete_subscription(self, url):
        response = self.fiware_manager._request(
            method='delete', url=self.url + "/" + url, headers={'Accept': 'application/json'})

        if response.status // 200 != 1:
            raise Exception(response.data)

    def _create_subscription(self, data):
        response = self.fiware_manager._request(
            method='post', url=self.url + "/v2/subscriptions", body=data,
            headers={'Accept': 'application/json', "Content-Type": "application/json"
                     })

        if response.status // 200 != 1:
            raise Exception(response.data)
        return response.headers["location"].split('/')[-1], response.headers["location"]


class TestFiwareManagerSubscriptions(TestCaseFiwareSubscriptions):

    def test_subscribe(self):
        sid, url = self.fiware_manager.subscribe(
            description="One subscription to rule them all",
            entities=[{"idPattern": ".*", "type": "Room"}],
            condition_expression={"q": "temperature>40"},
            condition_attributes=["temperature"],
            http="http://localhost:1234",
            notification_attrs=["temperature", "humidity"],
            expires="2016-04-05T14:00:00.00Z",
            throttling=5
            )
        expected_sub = {
            'id': sid,
            'description': "One subscription to rule them all",
            'expires': "2016-04-05T14:00:00.00Z",
            'throttling': 5,
            'status': 'expired',
            'subject': {
                'condition': {
                    'attrs': ['temperature'],
                    'expression': {'q': "temperature>40"}
                },
                'entities': [{"idPattern": ".*", "type": "Room"}]
            },
            'notification': {
                'attrs': ["temperature", "humidity"],
                'attrsFormat': 'normalized',
                'http': {'url': 'http://localhost:1234'}
            }


        }
        subscription = self._get_subscription(url)
        self._delete_subscription(url)
        self.assertEqual(expected_sub, subscription)

    def test_subscribe_fail(self):
        with self.assertRaises(FiException):
            self.fiware_manager.subscribe(
                description="One subscription to rule them all",
                entities=[{"type": "Room"}],
                condition_expression={"q": "temperature>40"},
                condition_attributes=["temperature"],
                http="http://localhost:1234",
                notification_attrs=["temperature", "humidity"],
                expires="2016-04-05T14:00:00.00Z",
                throttling=5
                )

    def test_unsubscribe_url(self):
        sid, url = self._create_subscription({
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
              })
        try:
            self.fiware_manager.unsubscribe(url=url)
        except Exception as ex:
            self._delete_subscription(url)
            self.fail("Exception while unsubscribe" + str(ex))

    def test_unsubscribe_id(self):
        sid, url = self._create_subscription({
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
        })
        try:
            self.fiware_manager.unsubscribe(subscription_id=sid)
        except Exception as ex:
            self._delete_subscription(url)
            self.fail("Exception while unsubscribe" + str(ex))

    def test_unsubscribe_fails(self):
        with self.assertRaises(FiException):
            self.fiware_manager.unsubscribe(url="v2/subscriptions/fakeURL")

    def test_subscription_id(self):
        sid, url = self._create_subscription({
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
        })
        expected_sub = {
            'id': sid,
            'description': "One subscription to rule them all",
            'expires': "2016-04-05T14:00:00.00Z",
            'throttling': 5,
            'status': 'expired',
            'subject': {
                'condition': {
                    'attrs': ['temperature'],
                    'expression': {'q': "temperature>40"}
                },
                'entities': [{"idPattern": ".*", "type": "Room"}]
            },
            'notification': {
                'attrs': ["temperature", "humidity"],
                'attrsFormat': 'normalized',
                'http': {'url': 'http://localhost:1234'}
            }

        }
        subscription = self.fiware_manager.subscriptions(subscription_id=sid)
        self._delete_subscription(url)
        self.assertEqual(expected_sub, subscription)

    def test_subscription_list(self):
        sid1, url1 = self._create_subscription({
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
        })

        sid2, url2 = self._create_subscription({
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
        })

        sid3, url3 = self._create_subscription({
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
        })
        expected_sub = {
            'id': sid2,
            'description': "One subscription to rule them all",
            'expires': "2016-04-05T14:00:00.00Z",
            'throttling': 5,
            'status': 'expired',
            'subject': {
                'condition': {
                    'attrs': ['temperature'],
                    'expression': {'q': "temperature>40"}
                },
                'entities': [{"idPattern": ".*", "type": "Room"}]
            },
            'notification': {
                'attrs': ["temperature", "humidity"],
                'attrsFormat': 'normalized',
                'http': {'url': 'http://localhost:1234'}
            }

        }

        self.assertGreater(len(self.fiware_manager.subscriptions()), 2, "Not all retrieved")
        self.assertEqual(len(self.fiware_manager.subscriptions(limit=2)), 2, "Not limited")
        self.assertEqual(type(self.fiware_manager.subscriptions(limit=1)), dict, "singleton listed")
        self.assertEqual(self.fiware_manager.subscriptions(limit=1, offset=1), expected_sub, "Not offset")

        self._delete_subscription(url1)
        self._delete_subscription(url2)
        self._delete_subscription(url3)

    def test_subscription_fails(self):
        with self.assertRaises(FiException):
            self.fiware_manager.subscriptions(subscription_id="fakeURL")

    def test_subscription_update(self):
        sid, url = self._create_subscription({
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
        })
        expected_sub = {
            'id': sid,
            'description': "One subscription to rule them all",
            'expires': "2016-04-05T14:00:00.00Z",
            'throttling': 5,
            'status': 'expired',
            'subject': {
                'condition': {
                    'attrs': ['temperature'],
                    'expression': {'q': "temperature>40"}
                },
                'entities': [{"idPattern": ".*", "type": "Room"}]
            },
            'notification': {
                'attrs': ["temperature", "humidity"],
                'attrsFormat': 'normalized',
                'http': {'url': 'http://localhost:1234'}
            }

        }
        subscription = self._get_subscription(url)

        self.assertEquals(expected_sub, subscription)
        expected_sub["throttling"] = 7
        self.fiware_manager.subscription_update(subscription_id=sid, throttling=7)
        subscription = self._get_subscription(url)

        self.assertEquals(expected_sub, subscription)
        self._delete_subscription(url)

    def test_subscription_update_fail(self):
        subscription_id = "WRONG"
        with self.assertRaises(FiException):
            self.fiware_manager.subscription_update(subscription_id=subscription_id, status="active")
