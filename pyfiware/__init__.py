from urllib3 import PoolManager
import json


class FiException(Exception):
    pass


class FiwareManager:

    def __init__(self, host, version="v2", codec="utf-8"):
        if host[-1] == "/":
            self.host = host[:-1]
        else:
            self.host = host
        self.base_url = self.host + "/" + version

        self.url_entities = self.base_url + "/entities"
        self.url_types = self.base_url + "/type"
        self.batch = self.base_url

        self.codec = codec
        self._pool_manager = PoolManager()
        self.header_no_payload = {
            "Accept": "application/json"
        }
        self.header_payload = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def request(self, **kwargs):
        return self._pool_manager.request(**kwargs)

    def get_by_id(self, element_id, scopes=None):
        get_url = self.url_entities + '/' + element_id
        headers = self.header_no_payload

        if scopes:
            if type(scopes) == list:
                headers["headers"] = ", ".join(scopes)
            elif type(scopes) == list:
                headers["headers"] = scopes
            else:
                raise Exception("scopes must be list or string")

        response = self.request(
                method="GET", url=get_url, headers=headers)
        if response.status // 200 != 1:
            if response.status == 404:
                return None
            raise FiException("Error%s: %s", response.status, response.data.decode(self.codec))
        data = json.loads(response.data.decode(self.codec))

        return data

    def get(self, entity_type=None, id_pattern=None, query=None):

        headers = self.header_no_payload
        fields = {}
        if entity_type:
            fields["type"] = entity_type
        if id_pattern:
            fields["idPattern"] = id_pattern
        if query:
            fields["q"] = query

        response = self.request(
                method="GET",  url=self.url_entities, headers=headers, fields=fields)
        if response.status // 200 != 1:
            if response.status == 404:
                return []
            raise FiException("Error%s: %s", response.status, response.data.decode(self.codec))

        return json.loads(response.data.decode(self.codec))

    def delete(self, element_id, silent=False):
        get_url = self.url_entities + '/' + element_id
        headers = self.header_no_payload

        response = self.request(
                method="DELETE", url=get_url, headers=headers)
        if response.status // 200 != 1:
            if response.status != 404 or not silent:
                raise FiException("Error%s: %s", response.status, response.data.decode(self.codec))

    def create(self, element_id, element_type, **attributes):

        headers = self.header_payload
        body = {'id': element_id, "type": element_type}

        for key in attributes:
            type_name = type(attributes[key]).__name__.capitalize()
            if type_name == "Str":
                type_name = "Text"
            if type_name == "Int":
                type_name = "Integer"
            body[key] = {'value': attributes[key], "type": type_name}

        response = self.request(
                method="POST", url=self.url_entities, body=json.dumps(body), headers=headers)
        if response.status // 200 != 1:
            raise FiException("Error%s: %s", response.status, response.data.decode(self.codec))

    def patch(self, element_id, silent=False, **attributes):

        headers = self.header_payload
        url = self.url_entities + "/" + element_id + "/attrs"

        response = self.request(
                method="PATCH", url=url, body=json.dumps(attributes, ), headers=headers)
        if response.status // 200 != 1:
            if response.status != 404 or not silent:
                raise FiException("Error%s: %s", response.status, response.data.decode(self.codec))
