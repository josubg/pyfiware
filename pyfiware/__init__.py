from urllib3 import PoolManager
import json
from logging import getLogger

logger = getLogger(__name__)


class FiException(Exception):
    """Exception produced by a context broker response"""
    def __init__(self, status, message, *args, **kwargs):
        super().__init__(status, message, *args, **kwargs)
        self.status = status
        self.message = message


class OrionConnector:
    """ Connects to the Orion context broker and provide easy use for its REST API.

    """
    version = "v2"
    header_no_payload = {
        "Accept": "application/json"
    }
    header_payload = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    _pool_manager = PoolManager()

    @property
    def service_path(self):
        """ Service path as string"""
        return self._service_path

    @service_path.setter
    def service_path(self, value):
        """ Service path to include in commands

        :param value: service paths as list
        :return:
        """
        if value is None:
            self._service_path = None
        elif type(value) == list:
            self._service_path = ", ".join(value)
        elif type(value) == str:
            self._service_path = value
        else:
            raise Exception("service_path must be list or string")

    def __init__(self, host, codec="utf-8", service=None, service_path=None, oauth_connector=None):
        """ Initialize the connector.

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
        self.service = service
        self._service_path = None
        self.service_path = service_path

        # OAUTH
        self.oauth = oauth_connector

    def _request(self, body=None, **kwargs):
        """Send a request to the Context Broker"""
        if body:
            body = json.dumps(body)
        headers = kwargs.pop("headers", {}).copy()
        if self.service:
            headers["Fiware-Service"] = self.service
        if self.service_path:
            headers["Fiware-ServicePath"] = self.service_path
        if self.oauth:
            headers["X-Auth-Token"] = self.oauth.token
        logger.debug("URL %s\nHEADERS %s\nBODY %s\n", kwargs['url'], headers, body)
        return self._pool_manager.request(body=body, headers=headers, **kwargs)

    def get(self, entity_id, entity_type=None):
        """ Get an entity form the context by its ID . If orion responses not found a None is returned.

        :param entity_id: The ID of the entity tha is retrieved
        :param entity_type: The entity type that the entities must match .

        :return: The entity or None
        """
        get_url = self.url_entities + '/' + entity_id
        if entity_type:
            get_url += "?type=" + entity_type
        response = self._request(
                method="GET", url=get_url, headers=self.header_no_payload)
        if response.status // 200 != 1:
            if response.status == 404:
                logger.debug("Not found: %s", get_url)
                return None
            raise FiException(response.status, "Error{}: {}".format(response.status, response.data.decode(self.codec)))

        return json.loads(response.data.decode(self.codec))

    def search(self, entity_type=None, id_pattern=None, query=None,
               georel=None, geometry=None, coords=None, limit=0, offset=0):
        """ Get the list of the entities that match the provided entity class, id pattern and/or query.

        :param entity_type: The entity type that the entities must match .
        :param id_pattern: The entity id pattern that the entities must match.
        :param query: The query that the entities must match.
        :param limit: The limit of returned entities. Zero, the default value, means unlimited
        :param offset: The offset of returned entities, for paginated search.


        :param georel

        :return: A list of entities or None
        """
        fields = {"options": "count",
                  "limit": limit if limit and limit <= 1000 else 1000}
        if offset:
            fields["offset"] = offset
        if entity_type:
            fields["type"] = entity_type
        if id_pattern:
            fields["idPattern"] = id_pattern
        if query:
            fields["q"] = query

        if georel and geometry and coords:
            if not (georel in ["coveredBy", "intersects", "equals", "disjoint"] or georel.startswith("near")):
                raise FiException("(%s) is not a valid spatial relationship(georel).", georel)
            if geometry not in ["point", "line", "polygon", "box"]:
                raise FiException("(%s) is not a valid geometry.", geometry)
            fields["georel"] = georel
            fields["geometry"] = geometry
            fields["coords"] = coords

        elif georel and geometry and coords:
            raise FiException(
                "Geographical Queries requires  georel, geometry and coords  attributes.%s %s %s",
                "georel not set!" if georel is None else ""
                "geometry not set!" if geometry is None else "",
                "coords not set!" if coords is None else ""
            )

        logger.debug("REQUEST to %s\n %s ", self.url_entities, fields)
        response = self._request(
                method="GET",  url=self.url_entities, headers=self.header_no_payload, fields=fields)
        if response.status // 200 != 1:
            if response.status == 404:
                logger.info("Not found: %s, \nfields: %s", self.url_entities, fields)
                return []
            raise FiException(response.status, "Error{}: {}".format(response.status, response.data.decode(self.codec)))
        results = json.loads(response.data.decode(self.codec))
        total_count = int(response.headers["fiware-total-count"])
        count = len(results)
        if not limit:
            limit = total_count
        if total_count - offset >= limit > count:
            results.extend(self.search(entity_type, id_pattern, query,  limit=limit-count, offset=offset + count))
        return results

    def delete(self, entity_id, silent=False, entity_type=None):
        """Delete a entity  from the Context broker.

        :param entity_type: Restrict the search to specific type.
        :param entity_id: Id of the entity to erase.
        :param silent: Not produce error if the entity is not found.

        :return: Nothing
        """
        get_url = self.url_entities + '/' + entity_id
        if entity_type:
            get_url += "?type=" + entity_type

        response = self._request(
                method="DELETE", url=get_url, headers=self.header_no_payload)
        if response.status // 200 != 1:
            if response.status != 404 or not silent:
                logger.debug("Not found: %s", get_url)
                raise FiException(response.status,
                                  "Error{}: {}".format(response.status, response.data.decode(self.codec)))

    def create(self, element_id, element_type, **attributes):
        body = {'id': element_id, "type": element_type}

        for key in attributes:
            type_name = type(attributes[key]).__name__.capitalize()
            if type_name == "Str":
                type_name = "String"
            if type_name == "Int":
                type_name = "Integer"
            if type_name == "Dict":
                type_name = "StructuredValue"
            body[key] = {'value': attributes[key], "type": type_name}

        self.create_raw(element_id, element_type, **body)

    def create_raw(self, element_id, element_type, **attributes):
        """ Create a Entity in the context broker. The entities can be passed as parameters or as a dictionary with **
        or attributes.

        Examples:

            fiware_manager.create(element_id="1", element_type="fake", **{'weight': 300, 'size': "100l"})
            fiware_manager.create(element_id="1", element_type="fake", attributes= {'weight': 300, 'size': "100l"})
            fiware_manager.create(element_id="1", element_type="fake", weight=300, size="100l")

        :param element_id: The ID of the entity
        :param element_type: The Type on the entity
        :param attributes:  The attributes for the entity.

        :return: Nothing
        """
        if element_id:
            attributes["id"] = element_id
        if element_type:
            attributes["type"] = element_type

        response = self._request(
            method="POST", url=self.url_entities, body=attributes, headers=self.header_payload)
        if response.status // 200 != 1:
            raise FiException(response.status, "Error{}: {}".format(response.status, response.data.decode(self.codec)))

    def patch(self, element_id, element_type, **attributes):

        url = self.url_entities + "/" + element_id + "/attrs?type=" + element_type

        response = self._request(
                method="PATCH", url=url, body=attributes, headers=self.header_payload)
        if response.status // 200 != 1:
            if response.status != 404:
                logger.debug("Not found: %s", url)
                raise FiException(response.status,
                                  "Error{}: {}".format(response.status, response.data.decode(self.codec)))

    def unsubscribe(self, url=None, subscription_id=None):
        if (url is None) == (subscription_id is None):
            raise FiException(None, "Set URL or subscription_id")

        subscription_url = ""
        if url:
            subscription_url = "/".join((self.host, url))
        elif subscription_id:
            subscription_url = "/".join((self.url_subscriptions, subscription_id))

        response = self._request(
            method="DELETE", url=subscription_url)
        if response.status // 200 != 1:
            raise FiException(response.status,
                              "Error{}: {}".format(response.status, response.data.decode(self.codec)))

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
            raise Exception("One and only one of notification_attrs and notification_attrs_blacklist can be set")
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
            raise FiException(response.status,
                              "Error{}: {}".format(response.status, response.data.decode(self.codec)))

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
            raise FiException(response.status,
                              "Error{}: {}".format(response.status, response.data.decode(self.codec)))
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
            raise FiException(response.status,
                              "Error{}: {}".format(response.status, response.data.decode(self.codec)))
