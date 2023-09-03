from pathlib import Path
from typing import Any, Dict, Optional, Iterable, cast

import requests
from requests.auth import HTTPDigestAuth
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class AmazonAssociatesStream(RESTStream):
    @property
    def url_base(self) -> str:
        return self.config["api_url"]

    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {"accept-encoding": "gzip"}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = response.headers.get("X-Next-Page", None)

        return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def prepare_request(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> requests.PreparedRequest:
        http_method = self.rest_method
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context, next_page_token)
        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers

        request = cast(
            requests.PreparedRequest,
            self.requests_session.prepare_request(
                requests.Request(
                    method=http_method,
                    url=url,
                    params=params,
                    headers=headers,
                    json=request_data,
                    auth=HTTPDigestAuth(  # This is the only change from the base class method
                        self.config.get("username"), self.config.get("password")
                    ),
                ),
            ),
        )
        return request

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())
