"""
The AppFigures API client
"""

import sys
from typing import Dict
import requests

import singer

from tap_appfigures.utils import RequestError


LOGGER = singer.get_logger()


class AppFiguresClient:
    """
    The client
    """
    BASE_URI = "https://api.appfigures.com/v2/"

    def __init__(self, config):
        self.start_date = config.get('start_date')
        self.auth_type = config.get("auth_type")
        self.stream_name = config.get("stream_name")
        if self.auth_type == "oauth":
            self.client_key = config.get("client_key")
            self.client_secret = config.get("client_secret")
            self.access_token = config.get("access_token")
            self.access_secret = config.get("access_secret")
        else:
            self.api_key = config.get('api_key')
            self.password = config.get('password')
            self.username = config.get('username')


    def make_request(self, uri):
        """
        Make a request to BASE_URI/uri
        and handle any errors
        """
        headers: Dict[str,str] = {}
        LOGGER.info("Making get request to {}".format(uri))
        try:
            if self.auth_type == "oauth":
                params = dict(
                    oauth_signature_method="PLAINTEXT",
                    oauth_consumer_key=self.client_key,
                    oauth_token=self.access_token,
                    oauth_signature=f"{self.client_secret}&{self.access_secret}"
                )
                headers = {"Authorization": f"OAuth {','.join(f'{k}={v}' for k, v in params.items())}"}
                response = requests.get(
                    self.BASE_URI + uri.lstrip("/"),
                    headers=headers,
                    timeout=600
                )

            else:
                headers = {"X-Client-Key": self.api_key}
                auth = (self.username, self.password)
                response = requests.get(
                    self.BASE_URI + uri.lstrip("/"),
                    auth=auth,
                    headers=headers,
                    timeout=600
                )
        except Exception as e:
            LOGGER.error('Error [{}], request {} failed'.format(e, uri))
            raise RequestError

        if response.status_code == 420:
            LOGGER.critical('Daily rate limit reached, after request for {}'.format(uri))
            sys.exit(1)

        response.raise_for_status()
        return response
