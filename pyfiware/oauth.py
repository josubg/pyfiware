from base64 import b64encode
from json import loads
from time import time
from urllib3 import PoolManager
from logging import getLogger

logger = getLogger(__name__)


class OAuthManager:

    def __init__(self,  oauth_server_url=None, client_id=None, client_secret=None,
                 user=None, password=None, codec="utf-8", token=None,
                 refresh_token=None, secure_lapse=10, scopes=None):
        self.codec = codec
        self.oauth_server = oauth_server_url
        self.user = user
        self.password = password
        self._client_id = client_id
        self._client_secret = client_secret
        self._encode()
        self.PM = PoolManager()
        self._bearer = None
        self._token = token
        self._refresh_token = refresh_token
        self._expiration = None
        self.secure_lapse = secure_lapse
        self.scopes = scopes

    def _encode(self, ):
        self.packed_Auth = b64encode(bytes("{0}:{1}".format(
            self._client_id, self._client_secret), self.codec)).decode(self.codec)

    @property
    def token(self):
        if self._token:
            if self.expired:
                try:
                    self._refresh_orion()
                except:
                    self._login()
        else:
            self._login()
        if self._bearer.lower() == "bearer":
            return f"Bearer {self._token}"
        return self._token

    @property
    def expired(self):
        if self._expiration is not None:
            return time() >= self._expiration - self.secure_lapse
        return False

    @property
    def client_id(self):
        return self._client_id

    @client_id.setter
    def client_id(self, value):
        self._client_id = value
        self._encode()

    @property
    def client_secret(self):
        return self._client_secret

    @client_secret.setter
    def client_secret(self, value):
        self.client_secret = value
        self._encode()

    def _login(self):

        url = self.oauth_server + "/token"
        headers = {"Authorization": "BASIC " + self.packed_Auth,
                   "Content-Type": "application/x-www-form-urlencoded"
                   }
        body = "grant_type=password&username=" + self.user + "&password=" + self.password
        if self.scopes:
            body = body + "&scope=" + " ".join(self.scopes)

        logger.debug("URL %s\nHEADERS %s\nBODY %s\n", url, headers, body)
        try :
            r = self.PM.request(method='POST', url=url, headers=headers, body=body)
        except Exception as ex:
            logger.warning("Unable to get Auth token: %s", ex)
            return None
        if r.status // 100 != 2:
            raise Exception("Orion Failed:%s ", r.status)

        response = loads(r.data.decode(self.codec))
        logger.debug(response)
        self._bearer = response["token_type"]
        self._token = response["access_token"]
        self._refresh_token = response["refresh_token"]
        self._expiration = time() + response["expires_in"]

    def _refresh_orion(self):

        url = self.oauth_server + "/token"
        headers = {"Authorization": "BASIC " + self.packed_Auth,
                   "Content-Type": "application/x-www-form-urlencoded"
                   }
        body = "grant_type=refresh_token&refresh_token=" + self._refresh_token

        logger.debug("URL %s\nHEADERS %s\nBODY %s\n", url, headers, body)

        r = self.PM.request(method='POST', url=url, headers=headers, body=body)
        if r.status // 100 != 2:
            raise Exception("Orion Failed:%s ", r.status)

        response = loads(r.data.decode(self.codec))
        logger.debug(response)
        self._bearer = response["token_type"]
        self._token = response["access_token"]
        self._refresh_token = response["refresh_token"]
        self._expiration = time() + response["expires_in"]
