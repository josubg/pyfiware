import json
from logging import getLogger

from urllib3 import PoolManager

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

        # removing trailing slashes is needed to modify the
        # service path for hierarchical search.
        # For Orion it makes no differences if '/foo' or '/foo///'
        # is used.
        if value is None:
            self._service_path = None
        elif type(value) == list:
            self._service_path = ", ".join([sp.rstrip("/") for sp in value])
        elif type(value) == str:
            if value != "/":
                self._service_path = value.rstrip("/")
            else:
                self._service_path = value
        else:
            raise Exception("service_path must be list or string")

    def __init__(self, host, codec="utf-8", service=None, service_path=None, oauth_connector=None, authorization_header_name="X-Auth-Token"):
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
        self.url_batch_update = self.base_url + "/op/update"
        self.batch = self.base_url

        self.codec = codec
        self.service = service
        self._service_path = None
        self.service_path = service_path

        # OAUTH
        self.oauth = oauth_connector
        self.authorization_header_name = authorization_header_name

    def _request(self, body=None, **kwargs):
        """Send a request to the Context Broker"""
        if body:
            body = json.dumps(body)
        headers = kwargs.pop("headers", {}).copy()
        if self.service:
            headers["Fiware-Service"] = self.service
        if self.service_path and "Fiware-ServicePath" not in headers:
            headers["Fiware-ServicePath"] = self.service_path
        if self.oauth:
            headers[self.authorization_header_name] = self.oauth.token
        logger.debug("URL %s\nHEADERS %s\nBODY %s\n", kwargs['url'], headers, body)
        return self._pool_manager.request(body=body, headers=headers, **kwargs)

    def get(self, entity_id, entity_type=None, key_values=False):
        """ Get an entity from the context by its ID. If Orion responses not found a None is returned.

        :param entity_id: The ID of the entity that is retrieved.
        :param entity_type: The entity type that the entities must match.
        :param key_values: Wether a full NGSIv2 entity should be returned or only a keyValues model

        :return: The entity or None
        """
        get_url = self.url_entities + '/' + entity_id

        fields = {}

        if entity_type:
            fields["type"] = entity_type
        if key_values:
            fields["options"] = "keyValues"
        response = self._request(
                method="GET", url=get_url, headers=self.header_no_payload, fields=fields)
        if response.status // 200 != 1:
            if response.status == 404:
                logger.debug("Not found: %s", get_url)
                return None
            raise FiException(response.status, "Error{}: {}".format(response.status, response.data.decode(self.codec)))

        return json.loads(response.data.decode(self.codec))

    def count(self, entity_type=None, id_pattern=None, query=None,
              georel=None, geometry=None, coords=None, hierarchical_search=False):
        """ Get the  total amount of entities that match the provided entity class, id pattern and/or query.

        :param entity_type: The entity type that the entities must match .
        :param id_pattern: The entity id pattern that the entities must match.
        :param query: The query that the entities must match.
        :param geometry: Geometry form used to spacial limit the query: "point", "line", "polygon", "box"
        :param georel: Relation between the geometry an  the entities: "coveredBy", "intersects", "equals", "disjoint"
        :param coords: Semicolon separated list of coordinates(coma separated) Ex: "45.7878,3.455454;41.7878,5.455454"
        :param hierarchical_search: Search only in this servicePath or in all sub servicePaths as well

        :return: The amount of entities
        """
        fields = {"options": "count",
                  "limit": 1
                  }
        if entity_type:
            fields["type"] = entity_type
        if id_pattern:
            fields["idPattern"] = id_pattern
        if query:
            fields["q"] = query

        headers = self.header_no_payload.copy()
        if hierarchical_search:
            if not self._service_path:
                raise FiException(None, "Hierarchical search does not work without service path.")
            elif ", " in self._service_path:
                headers["Fiware-ServicePath"] = ", ".join([sp + "/#" for sp in self.service_path.split(", ")])
            else:
                headers["Fiware-ServicePath"] = self.service_path + "/#"

        if georel and geometry and coords:
            if not (georel in ["coveredBy", "intersects", "equals", "disjoint"] or georel.startswith("near")):
                raise FiException(None, f"({georel}) is not a valid spatial relationship(georel).")
            if geometry not in ["point", "line", "polygon", "box"]:
                raise FiException(None, f"({geometry}) is not a valid geometry.")
            fields["georel"] = georel
            fields["geometry"] = geometry
            fields["coords"] = coords

        elif georel or geometry or coords:
            raise FiException(None,
                f"Geographical Queries requires  georel, geometry and coords  attributes. \
                    {'georel not set!' if georel is None else ''} \
                    {'geometry not set!' if geometry is None else ''} \
                    {'coords not set!' if coords is None else ''}"
            )

        logger.debug("REQUEST to %s\n %s ", self.url_entities, fields)
        response = self._request(
                method="GET",  url=self.url_entities, headers=headers, fields=fields)
        if response.status // 200 != 1:
            if response.status == 404:
                logger.info("Not found: %s, \nfields: %s", self.url_entities, fields)
                return []
            raise FiException(response.status, "Error{}: {}".format(response.status, response.data.decode(self.codec)))
        return int(response.headers["fiware-total-count"])

    def search(self, entity_type=None, id_pattern=None, query=None,
               georel=None, geometry=None, coords=None, limit=0, offset=0, key_values=False, hierarchical_search=False):
        """ Get the list of the entities that match the provided entity class, id pattern and/or query.

        :param entity_type: The entity type that the entities must match .
        :param id_pattern: The entity id pattern that the entities must match.
        :param query: The query that the entities must match.
        :param limit: The limit of returned entities. Zero, the default value, means 1000 (max limit of Orion)
        :param offset: The offset of returned entities, for paginated search.
        :param geometry: Geometry form used to spacial limit the query: "point", "line", "polygon", "box"
        :param georel: Relation between the geometry an  the entities: "coveredBy", "intersects", "equals", "disjoint"
        :param coords: Semicolon separated list of coordinates(coma separated) Ex: "45.7878,3.455454;41.7878,5.455454"
        :param key_values: Wether a full NGSIv2 entity should be returned or only a keyValues model
        :param hierarchical_search: Search only in this servicePath or in all sub servicePaths as well

        :return: A list of entities or None
        """
        options = "count"
        if key_values:
            options = options + ",keyValues"
        fields = {"options": options,
                  "limit": limit if limit and limit <= 1000 else 1000}
        if offset:
            fields["offset"] = offset
        if entity_type:
            fields["type"] = entity_type
        if id_pattern:
            fields["idPattern"] = id_pattern
        if query:
            fields["q"] = query

        headers = self.header_no_payload.copy()
        if hierarchical_search:
            if not self._service_path:
                raise FiException(None, "Hierarchical search does not work without service path.")
            elif ", " in self._service_path:
                headers["Fiware-ServicePath"] = ", ".join([sp + "/#" for sp in self.service_path.split(", ")])
            else:
                headers["Fiware-ServicePath"] = self.service_path + "/#"

        if georel and geometry and coords:
            if not (georel in ["coveredBy", "intersects", "equals", "disjoint"] or georel.startswith("near")):
                raise FiException(None, f"({georel}) is not a valid spatial relationship(georel).")
            if geometry not in ["point", "line", "polygon", "box"]:
                raise FiException(None, f"({geometry}) is not a valid geometry.")
            fields["georel"] = georel
            fields["geometry"] = geometry
            fields["coords"] = coords

        elif georel or geometry or coords:
            raise FiException(None,
                f"Geographical Queries requires  georel, geometry and coords  attributes. \
                    {'georel not set!' if georel is None else ''} \
                    {'geometry not set!' if geometry is None else ''} \
                    {'coords not set!' if coords is None else ''}"
            )

        logger.debug("REQUEST to %s\n %s ", self.url_entities, fields)
        response = self._request(
                method="GET",  url=self.url_entities, headers=headers, fields=fields)
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
            results.extend(self.search(entity_type=entity_type, id_pattern=id_pattern, query=query,
               georel=georel, geometry=geometry, coords=coords, limit=limit-count, offset=offset + count, key_values=key_values, hierarchical_search=hierarchical_search))

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
            if response.status == 404:
                logger.debug("Not found: %s", url)
            raise FiException(response.status,
                                "Error{}: {}".format(response.status, response.data.decode(self.codec)))

    def update(self, element_id, element_type, **attributes):
        url = self.url_entities + "/" + element_id + "/attrs?type=" + element_type

        response = self._request(
                method="POST", url=url, body=attributes, headers=self.header_payload)
        if response.status // 200 != 1:
            if response.status == 404:
                logger.debug("Not found: %s", url)
            raise FiException(response.status,
                                "Error{}: {}".format(response.status, response.data.decode(self.codec)))

    def delete_attribute(self, element_id, element_type, attribute_name):
        url = self.url_entities + "/" + element_id + "/attrs/" + attribute_name + "?type=" + element_type

        response = self._request(
                method="DELETE", url=url)
        if response.status // 200 != 1:
            if response.status == 404:
                logger.debug("Not found: %s", url)
            raise FiException(response.status,
                                "Error{}: {}".format(response.status, response.data.decode(self.codec)))

    def batch_update(self, action_type, entities):
        """ Create/Modify/Delete multiple entities at once in the context broker.

        Examples:

            fiware_manager.batch_update(action_type="append", entities=[{"type": "Room", "id": "Room3", "temperate": {"value": 29.9, "type": "Float"}}])

        :param action_type: Can be one of "append", "appendStrict", "update", "delete" or "replace"
        :param entities: A list of entities

        :return: Nothing
        """

        body = {
                "actionType": action_type,
                "entities": entities
               }

        response = self._request(
            method="POST", url=self.url_batch_update, body=body, headers=self.header_payload)
        if response.status // 200 != 1:
            raise FiException(response.status, "Error{}: {}".format(response.status, response.data.decode(self.codec)))

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

        subscription["subject"] = {"entities": entities, "condition": condition}

        # Notification
        notification = {}

        # Check if one and only one is defined
        if notification_attrs and notification_attrs_blacklist:
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

    def subscription(self, subscription_id=None):
        fields = {}
        url = self.url_subscriptions

        if subscription_id:
            url += '/' + subscription_id
        response = self._request(
            method="GET", url=url, fields=fields, headers=self.header_no_payload)
        if response.status // 200 != 1:
            raise FiException(response.status,
                              "Error{}: {}".format(response.status, response.data.decode(self.codec)))
        data = json.loads(response.data.decode(self.codec))
        return data

    def subscriptions(self, limit=None, offset=None, count=False):
        fields = {}
        url = self.url_subscriptions
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
        if type(data) == list and len(data) == 1:
            data = data[0]

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
        # Check if only one is defined
        if notification_attrs and notification_attrs_blacklist:
            raise Exception("One and only one of notification_attrs and notification_attrs_blacklist can be set")
        if notification_attrs:
            notification["attrs"] = notification_attrs
        if notification_attrs_blacklist:
            notification["exceptAttrs"] = notification_attrs_blacklist

        # Check if only one is defined
        if http and http_custom:
            raise Exception("One and only one of http and http_custom must be set: http or http_custom")
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
