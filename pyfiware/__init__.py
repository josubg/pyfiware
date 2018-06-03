from urllib3 import PoolManager
import json


class FiException(Exception):
    """Exception produced by a context broker response"""
    pass


class OrionManager:
    """ Manages entities in the Orion context broker through its REST API.

    """
    version = "v2"
    header_no_payload = {
        "Accept": "application/json"
    }
    header_payload = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    @property
    def scopes(self):
        return self._scopes

    @scopes.setter
    def scopes(self, value):
        if value is None:
            self._scopes = None
        elif type(value) == list:
            self._scopes = ", ".join(value)
        elif type(value) == str:
            self._scopes = value
        else:
            raise Exception("scopes must be list or string")

    def __init__(self, host, codec="utf-8", scopes=None):
        """ Initialize the Manager.

        :param host: The url of the NGSI API  (Ending  '/' will be removed )
        :param codec: The codec used  decoding responses.
        """
        if host[-1] == "/":
            self.host = host[:-1]
        else:
            self.host = host
        self.base_url = self.host + "/" + self.version

        self.url_entities = self.base_url + "/entities"
        self.url_types = self.base_url + "/type"
        self.url_subscriptions = self.base_url + "/subscriptions"
        self.batch = self.base_url

        self.codec = codec
        self._scopes = None
        self.scopes = scopes

        self._pool_manager = PoolManager()

    def _request(self, body=None, **kwargs):
        """Send a request to the Context Broker"""
        if body:
            body = json.dumps(body)
        if self.scopes:
            kwargs["headers"] = kwargs.get("headers", {}).copy()
            kwargs["headers"]["Fiware-ServicePath"] = self.scopes

        return self._pool_manager.request(body=body, **kwargs)

    def get(self, entity_id, entity_type=None):
        """ Get an entity form the context by its ID . If orion responses not found a None is returned.

        :param entity_id: The ID of the entity tha is retrieved

        :return: The entity or None
        """
        get_url = self.url_entities + '/' + entity_id
        if entity_type:
            get_url += "?type=" + entity_type
        response = self._request(
                method="GET", url=get_url, headers=self.header_no_payload)
        if response.status // 200 != 1:
            if response.status == 404:
                return None
            raise FiException("Error%s: %s", response.status, response.data.decode(self.codec))

        return json.loads(response.data.decode(self.codec))

    def search(self, entity_type=None, id_pattern=None, query=None):
        """ Get the list of the entities that match the provided entity class, id pattern and/or query.

        :param entity_type: The entity type that the entities must match .
        :param id_pattern: The entity id pattern that the entities must match.
        :param query: The query that the entities must match.
        :return: A list of entities or None
        """

        fields = {}
        if entity_type:
            fields["type"] = entity_type
        if id_pattern:
            fields["idPattern"] = id_pattern
        if query:
            fields["q"] = query

        response = self._request(
                method="GET",  url=self.url_entities, headers=self.header_no_payload, fields=fields)
        if response.status // 200 != 1:
            if response.status == 404:
                return []
            raise FiException("Error%s: %s", response.status, response.data.decode(self.codec))

        return json.loads(response.data.decode(self.codec))

    def delete(self, entity_id, silent=False):
        """Delete a entity  from the Context broker.

        :param entity_id: Id of the entity to erase.
        :param silent: Not produce error if the entity is not found.
        :return: Nothing
        """
        get_url = self.url_entities + '/' + entity_id

        response = self._request(
                method="DELETE", url=get_url, headers=self.header_no_payload)
        if response.status // 200 != 1:
            if response.status != 404 or not silent:
                raise FiException("Error%s: %s", response.status, response.data.decode(self.codec))

    def create(self, element_id, element_type, **attributes):
        """ Create a Entity in the context broker. The entities can be passed as parametters or as a dictionary with **
        or attributes.

        Examples:

            fiware_manager.create(element_id="1", element_type="fake", **{'weight': 300, 'size': "100l"})
            fiware_manager.create(element_id="1", element_type="fake", attributes= {'weight': 300, 'size': "100l"})
            fiware_manager.create(element_id="1", element_type="fake", weight=300, size="100l")

        :param element_id: The ID of the entity
        :param element_type: The Type on the entity
        :param attributes:  The attributes for the entity.
        :return:
        """

        body = {'id': element_id, "type": element_type}

        for key in attributes:
            type_name = type(attributes[key]).__name__.capitalize()
            if type_name == "Str":
                type_name = "Text"
            if type_name == "Int":
                type_name = "Integer"
            body[key] = {'value': attributes[key], "type": type_name}

        response = self._request(
            method="POST", url=self.url_entities, body=body, headers=self.header_payload)
        if response.status // 200 != 1:
            raise FiException("Error%s: %s", response.status, response.data.decode(self.codec))

    def patch(self, element_id, silent=False, **attributes):

        url = self.url_entities + "/" + element_id + "/attrs"

        response = self._request(
                method="PATCH", url=url, body=attributes, headers=self.header_payload)
        if response.status // 200 != 1:
            if response.status != 404 or not silent:
                raise FiException("Error%s: %s", response.status, response.data.decode(self.codec))

    def unsubscribe(self, url=None, subscription_id=None):
        if (url is None) == (subscription_id is None):
            raise FiException()
        subscription_url = ""
        if url:
            subscription_url = "/".join((self.host, url))
        elif subscription_id:
            subscription_url = "/".join((self.url_subscriptions, subscription_id))

        response = self._request(
            method="DELETE", url=subscription_url)
        if response.status // 200 != 1:
            raise FiException("Error%s: %s", response.status, response.data.decode(self.codec))

    def subscribe(self, description,
                  entities, condition_attributes=None, condition_expression=None,
                  notification_attrs=None, notification_attrs_blacklist=None,
                  http=None, http_custom=None,
                  attrs_format=None, metadata=None,
                  expires=None, throttling=None):

        subscription = {"description": description}

        # General
        if expires:
            subscription["expires"] = expires
        if throttling:
            subscription["throttling"] = throttling

        # subject
        condition = {}
        if condition_attributes:
            condition["attrs"] = condition_attributes
        if condition_expression:
            condition["expression"] = condition_expression

        subject = {"entities": entities, "condition": condition}
        subscription["subject"] = subject

        # Notification
        notification = {}

        # Check if one and only one is defined
        if (notification_attrs is None) == (notification_attrs_blacklist is None):
            raise Exception("One and only one of nottification_attrs and nottification_attrs_blackist can be set")
        if notification_attrs:
            notification["attrs"] = notification_attrs
        if notification_attrs_blacklist:
            notification["exceptAttrs"] = notification_attrs_blacklist

        # Check if one and only one is defined
        if (http is None) == (http_custom is None):
            raise Exception("One and only one of http and http_custom can be set")
        if http:
            notification["http"] = {"url": http}

        if http_custom:
            notification["httpCustom"] = http_custom

        if attrs_format:
            notification["attrsFormat"] = attrs_format
        if metadata:
            notification["metadata"] = metadata

        subscription["notification"] = notification

        response = self._request(
                method="POST", url=self.url_subscriptions, body=subscription, headers=self.header_payload)
        if response.status // 200 != 1:
            raise FiException("Error%s: %s", response.status, response.data.decode(self.codec))

        return response.headers["location"].split('/')[-1], response.headers["location"]

    def subscriptions(self, subscription_id=None, limit=None, offset=None, count=False):
        fields = {}
        url = self.url_subscriptions

        if subscription_id:
            url += '/' + subscription_id
        if limit:
            fields["limit"] = limit
        if offset:
            fields["offset"] = offset
        if count:
            fields["options"] = '{"count": True}'

        response = self._request(
            method="GET", url=url, fields=fields, headers=self.header_no_payload)
        if response.status // 200 != 1:
            raise FiException("Error%s: %s", response.status, response.data.decode(self.codec))
        data = json.loads(response.data.decode(self.codec))
        if type(data) is list and len(data) == 1:
            return data[0]

        return data

    def subscription_update(self,
                            subscription_id, status=None, description=None,
                            entities=None, condition_attributes=None, condition_expression=None,
                            notification_attrs=None, notification_attrs_blacklist=None,
                            http=None, http_custom=None,
                            attrs_format=None, metadata=None,
                            expires=None, throttling=None):

        subscription = {}
        if status:
            subscription["status"] = status
        if description:
            subscription["description"] = description
        # General
        if expires:
            subscription["expires"] = expires
        if throttling:
            subscription["throttling"] = throttling

        # subject
        condition = {}
        if condition_attributes:
            condition["attrs"] = condition_attributes
        if condition_expression:
            condition["expression"] = condition_expression

        subject = {}
        if entities:
            subject["entities"] = entities
        if condition:
            subject["condition"] = condition

        if subject:
            subscription["subject"] = subject

        # Notification
        notification = {}

        if notification_attrs:
            notification["attrs"] = notification_attrs
        if notification_attrs_blacklist:
            notification["exceptAttrs"] = notification_attrs_blacklist

        # Check if one and only one is defined
        if http:
            notification["http"] = {"url": http}

        if http_custom:
            notification["httpCustom"] = http_custom

        if attrs_format:
            notification["attrsFormat"] = attrs_format
        if metadata:
            notification["metadata"] = metadata

        if notification:
            subscription["notification"] = notification

        response = self._request(
            method="PATCH", url=self.url_subscriptions + "/" + subscription_id,
            body=subscription, headers=self.header_payload)
        if response.status // 200 != 1:
            raise FiException("Error%s: %s", response.status, response.data.decode(self.codec))
