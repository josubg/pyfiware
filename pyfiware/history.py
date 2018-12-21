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

    def __init__(self, host, codec="utf-8", version="api"):
        self.host = host + "/" + version
        self.codec = codec

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

    def scenario_list(self):
        response = self._pool_manager.request(method="GET", url="{0}/scenarios".format(self.host))

        if response.status // 200 != 1:
            raise HistoryException(response.status,
                                   "Error{}: {}".format(response.status, response.data.decode(self.codec)))

        return json.loads(response.data.decode(self.codec))

    def scenario_get(self, scenario_id):
        url = "{}/scenario/{}".format(self.host, scenario_id)
        response = self._pool_manager.request(method="GET", url=url)

        if response.status // 200 != 1:
            if response.status == 404:
                logger.debug("Not found: %s", url)
                return None
            raise HistoryException(
                response.status, "Error{}: {}".format(response.status,
                                                      response.data.decode(self.codec)))

        return json.loads(response.data.decode(self.codec))

    def entity_list(self, scenario_id, since=None, until="now", limit=9999, offset=0):
        fields = {
            "max_time": until,
            "limmit": limit,
            "offset": offset
        }
        if since:
            fields["since"] = since

        response = self._pool_manager.request(
            method="GET", url="{0}/scenario/{1}/entities".format(self.host, scenario_id),
            fields=fields
        )

        if response.status // 200 != 1:
            raise HistoryException(response.status,
                                   "Error{}: {}".format(response.status, response.data.decode(self.codec)))

        return json.loads(response.data.decode(self.codec))

    def entity_get(self, scenario_id, entity_type, entity_id, since=None, until=None, limit=9999, offset=0):

        fields = {
            "max_time": until.replace(tzinfo=timezone.utc).timestamp()*1000\
                if until.__class__ is datetime else until \
                if until else datetime.now(tz=timezone.utc).timestamp()*1000,
            "limit": limit,
            "offset": offset
        }
        if since:
            fields["since"] = since

        response = self._pool_manager.request(
            method="GET", url="{0}/scenario/{1}/entity/{2}/{3}".format(self.host, scenario_id, entity_type, entity_id),
            fields=fields
        )

        if response.status // 200 != 1:
            raise HistoryException(response.status,
                                   "Error{}: {}".format(response.status, response.data.decode(self.codec)))

        return json.loads(response.data.decode(self.codec))

    def entity_list_by_type(self, scenario_id, entity_type, since=None, until="now", limit=9999, offset=0):
        fields = {
            "max_time": until,
            "limmit": limit,
            "offset": offset
        }
        if since:
            fields["since"] = since
        response = self._pool_manager.request(
            method="GET", url="{0}/scenario/{1}/entities/{2}".format(self.host, scenario_id, entity_type),
            fields=fields)

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

    def entity_update(self, scenario_id, entity_type, entity_id, data):
        response = self._pool_manager.request(
            method="POST", url="{0}/scenario/{1}/entities/{2}/{3}".format(
                self.host, scenario_id, entity_type, entity_id),
            body=data)

        if response.status // 200 != 1:
            raise HistoryException(response.status,
                                   "Error{}: {}".format(response.status, response.data.decode(self.codec)))
        return json.loads(response.data.decode(self.codec))
