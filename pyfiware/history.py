from urllib3 import PoolManager
import json
from datetime import datetime, timezone
from logging import getLogger


logger = getLogger(__name__)


class HistoryException(Exception):
    def __init__(self, status, message, *args, **kwargs):
        super().__init__(status, message, *args, **kwargs)
        self.status = status
        self.message = message


class HistoryConnector:

    _pool_manager = PoolManager()

    def __init__(self, host, token, codec="utf-8", version="api"):
        self.host = host + "/" + version
        self.codec = codec
        self.token = token
        self.header_payload = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Access-Token": self.token,
        }

    def scenario_create(self, scenario_id):
        response = self._pool_manager.request(method="POST", url="{0}/scenario/{1}".format(
            self.host, scenario_id))

        if response.status // 200 != 1:
            raise HistoryException(response.status,
                                   "Error{}: {}".format(response.status, response.data.decode(self.codec)))

    def scenario_socket_connect(self, scenario_id):
        response = self._pool_manager.request(method="POST", url="{0}/scenario/{1}/socket".format(
            self.host, scenario_id))

        if response.status // 200 != 1:
            raise HistoryException(response.status,
                                   "Error{}: {}".format(response.status, response.data.decode(self.codec)))

    def scenario_delete(self, scenario_id):
        response = self._pool_manager.request(method="DELETE", url="{0}/scenario/{1}".format(
            self.host, scenario_id))

        if response.status // 200 != 1:
            raise HistoryException(response.status,
                                   "Error{}: {}".format(response.status, response.data.decode(self.codec)))

    def scenario_socket_close(self, scenario_id):
        response = self._pool_manager.request(method="DELETE", url="{0}/scenario/{1}/socket".format(
            self.host, scenario_id))

        if response.status // 200 != 1:
            raise HistoryException(response.status,
                                   "Error{}: {}".format(response.status, response.data.decode(self.codec)))

    def scenario_list(self, user_id=None):
        fields = {}
        if user_id:
            fields["user_id"] = user_id

        response = self._pool_manager.request(method="GET", url="{0}/scenarios".format(self.host), fields=fields)

        if response.status // 200 != 1:
            raise HistoryException(response.status,
                                   "Error{}: {}".format(response.status, response.data.decode(self.codec)))

        return json.loads(response.data.decode(self.codec))

    def scenario_get(self, scenario_id):
        url = "{0}/scenario/{1}".format(self.host, scenario_id)
        response = self._pool_manager.request(method="GET", url=url)

        if response.status // 200 != 1:
            if response.status == 404:
                logger.debug("Not found: %s", url)
                return None
            raise HistoryException(
                response.status, "Error{}: {}".format(response.status,
                                                      response.data.decode(self.codec)))

        return json.loads(response.data.decode(self.codec))

    def entity_list(self, scenario_id, since=None, until=None, limit=9999, offset=0):
        fields = {
            "limit": limit,
            "offset": offset
        }
        if since:
            fields['time>'] = '{0}'.format(since.replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')) \
                if since.__class__ is datetime else since

        if until:
            fields['time<'] = '{0}'.format(until.replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')) \
                if until.__class__ is datetime else until

        response = self._pool_manager.request(
            method="GET", url="{0}/scenario/{1}/entities".format(self.host, scenario_id), headers=self.header_payload,
            fields=fields
        )

        if response.status // 200 != 1:
            raise HistoryException(response.status,
                                   "Error{}: {}".format(response.status, response.data.decode(self.codec)))

        return json.loads(response.data.decode(self.codec))

    def entity_get(self, scenario_id, entity_type, entity_id, since=None, until=None, limit=9999, offset=0, attributes=None, query=None):
        fields = {
            "attributes": "*",
            "limit": limit,
            "offset": offset,
        }
        if attributes is not None:
            fields['attributes'] = ','.join(attributes)

        if query:
            fields.update(query)

        if since:
            fields['time>'] = '{0}'.format(since.replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')) \
                if since.__class__ is datetime else since

        if until:
            fields['time<'] = '{0}'.format(until.replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')) \
                if until.__class__ is datetime else until

        response = self._pool_manager.request(
            method="GET", url="{0}/scenario/{1}/entity/{2}/{3}".format(self.host, scenario_id, entity_type, entity_id),
            headers=self.header_payload, fields=fields
        )

        if response.status // 200 != 1:
            raise HistoryException(response.status,
                                   "Error{}: {}".format(response.status, response.data.decode(self.codec)))

        return json.loads(response.data.decode(self.codec))


    def entities_get(self, scenario_id, entity_type, since=None, until=None, limit=9999, offset=0, attributes=None, query=None):
        fields = {
            "attributes": "*",
            "limit": limit,
            "offset": offset,
        }

        if attributes is not None:
            fields['attributes'] = ','.join(attributes)

        if query:
            fields.update(query)

        if since:
            fields['time>'] = '{0}'.format(since.replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')) \
                if since.__class__ is datetime else since

        if until:
            fields['time<'] = '{0}'.format(until.replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')) \
                if until.__class__ is datetime else until

        response = self._pool_manager.request(
            method="GET", url="{0}/scenario/{1}/entities/{2}d".format(self.host, scenario_id, entity_type),
            headers=self.header_payload, fields=fields
        )

        if response.status // 200 != 1:
            raise HistoryException(response.status,
                                   "Error{}: {}".format(response.status, response.data.decode(self.codec)))

        return json.loads(response.data.decode(self.codec))


    def entity_list_by_type(self, scenario_id, entity_type, since=None, until=None, limit=9999, offset=0, attrs=None):
        fields = {
            "attributes": "*",
            "limit": limit,
            "offset": offset
        }
        if since:
            fields['time>'] = '{0}'.format(since.replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')) \
                if since.__class__ is datetime else since

        if until:
            fields['time<'] = '{0}'.format(until.replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')) \
                if until.__class__ is datetime else until

        response = self._pool_manager.request(
            method="GET", url="{0}/scenario/{1}/entities/{2}".format(self.host, scenario_id, entity_type),
            headers=self.header_payload, fields=fields)

        if response.status // 200 != 1:
            raise HistoryException(response.status,
                                   "Error{}: {}".format(response.status, response.data.decode(self.codec)))
        return json.loads(response.data.decode(self.codec))

    def entity_type_fist_time(self, scenario_id, entity_type):
        response = self._pool_manager.request(method="GET", url="{0}/scenario/{1}/entities/{2}/min_time".format(
            self.host, scenario_id, entity_type))

        if response.status // 200 != 1:
            raise HistoryException(response.status,
                                   "Error{}: {}".format(response.status, response.data.decode(self.codec)))
        return json.loads(response.data.decode(self.codec))

    def entity_type_last_time(self, scenario_id, entity_type):
        response = self._pool_manager.request(method="GET", url="{0}/scenario/{1}/entities/{2}/min_time".format(
            self.host, scenario_id, entity_type))

        if response.status // 200 != 1:
            raise HistoryException(response.status,
                                   "Error{}: {}".format(response.status, response.data.decode(self.codec)))
        return json.loads(response.data.decode(self.codec))

    def entity_create(self, scenario_id, **data):
        response = self._pool_manager.request(
            method="POST", url="{0}/scenario/{1}/entity".format(self.host, scenario_id), body=json.dumps(data),
            headers=self.header_payload)

        if response.status // 200 != 1:
            raise HistoryException(response.status,
                                   "Error{}: {}".format(response.status, response.data.decode(self.codec)))
        return response.data

    def entity_update(self, scenario_id, entity_type, entity_id, **data):
        response = self._pool_manager.request(
            method="PATCH", url="{0}/scenario/{1}/entity/{2}/{3}".format(self.host, scenario_id, entity_type, entity_id),
            body=json.dumps(data), headers=self.header_payload)

        if response.status // 200 != 1:
            raise HistoryException(response.status,
                                   "Error{}: {}".format(response.status, response.data.decode(self.codec)))
        return response.data
