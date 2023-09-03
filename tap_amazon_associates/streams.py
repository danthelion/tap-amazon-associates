import re
import zlib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Iterable, Callable, Generator

import backoff
import requests
from requests.auth import HTTPDigestAuth
from singer_sdk import typing as th
from singer_sdk.exceptions import RetriableAPIError

from tap_amazon_associates.client import AmazonAssociatesStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class ReportListStream(AmazonAssociatesStream):
    name = "ReportList"
    path = "/datafeed/listReports"
    primary_keys = ["filename"]
    replication_key = "last_modified"
    schema = th.PropertiesList(
        th.Property(
            "filename",
            th.StringType,
            description="Filename of the report",
        ),
        th.Property(
            "last_modified",
            th.DateTimeType,
            description="Last modified date of the report",
        ),
        th.Property(
            "download",
            th.StringType,
            description="URL slug to download the report",
        ),
        th.Property(
            "report_type",
            th.StringType,
            description="Type of report e.g earnings, orders, tracking, etc",
        ),
    ).to_dict()

    def extract_report_type(self, filename) -> str:
        try:
            report_type = (
                re.match(r"\w+-\d{2}-(.+)-\d{8}\.tsv\.gz", filename)
                .group(1)
                .replace("-", " ")
                .title()
                .replace(" ", "")
            )
        except AttributeError:
            self.logger.error(
                f"Could not extract report type from filename: {filename}"
            )
        # the Strategist UK account has subtags in their earnings and orders reports
        if "uk-21" in filename and report_type in ["EarningsReport", "OrdersReport"]:
            report_type += "Subtags"
        return report_type

    def get_child_context(self, record: Dict, context: Optional[Dict]) -> Dict:
        return {
            "filename": record["filename"],
            "last_modified": record["last_modified"],
            "report_type": record["report_type"],
        }

    def _sync_children(self, child_context: Dict) -> None:
        for child_stream in self.child_streams:
            if child_stream.selected or child_stream.has_selected_descendents:
                current_max_update_date = None
                if self.stream_state.get("starting_replication_value"):
                    try:
                        current_max_update_date = datetime.strptime(
                            self.stream_state.get("starting_replication_value"),
                            "%Y-%m-%d %H:%M:%S %Z",
                        )
                    except ValueError:
                        current_max_update_date = datetime.strptime(
                            self.stream_state.get("starting_replication_value"),
                            "%Y-%m-%dT%H:%M:%SZ",
                        )
                last_modified_datetime = datetime.strptime(
                    child_context["last_modified"], "%Y-%m-%d %H:%M:%S %Z"
                )
                if (
                    child_stream.name == child_context["report_type"]
                    and current_max_update_date
                    and last_modified_datetime > current_max_update_date
                ):
                    child_stream.sync(context=child_context)

    def parse_response(self, response: requests.Response) -> Iterable[Dict]:
        page_content = response.text
        match_list = re.findall(
            r"<TR><TD>(.*?)</TD><TD>(.*?)</TD>.*?<a href=(.*?)>",
            page_content,
            re.DOTALL,
        )
        file_list = []
        for row in match_list:
            if "tsv" in row[0] and "bounty" not in row[0]:
                new_time = (
                    datetime.strptime(row[1], "%a %b %d %H:%M:%S %Z %Y").strftime(
                        "%Y-%m-%d %H:%M:%S %Z"
                    )
                    + "UTC"
                )
                new_row = {
                    "filename": row[0],
                    "last_modified": new_time,
                    "download": row[2][1:-1],
                }
                file_list.append(new_row)
        yield from file_list

    def post_process(self, row: Dict, context: Optional[Dict]) -> Dict:
        """As needed, append or transform raw data to match expected structure."""
        report_type = self.extract_report_type(row["filename"])
        row["report_type"] = report_type
        return row


class ReportStream(AmazonAssociatesStream):
    name = "Report"
    path = "/datafeed/getReport?filename="
    schema_filepath = SCHEMAS_DIR / "reports.json"

    def get_url(self, context: Optional[Dict] = None) -> str:
        if context is None:
            return self.url_base + self.path
        else:
            return self.url_base + self.path + context["filename"]

    def decompress_stream(self, stream):
        o = zlib.decompressobj(16 + zlib.MAX_WBITS)

        for c in stream:
            yield o.decompress(c)

        yield o.flush()

    def iter_decompressed_lines(self, decompressed_stream):
        pending = None
        for c in decompressed_stream:
            if pending is not None:
                c = pending + c

            lines = c.splitlines()

            if lines and lines[-1] and c and lines[-1][-1] == c[-1]:
                pending = lines.pop()
            else:
                pending = None

            yield from lines

        if pending is not None:
            yield pending

    def backoff_wait_generator(self) -> Callable[..., Generator[int, Any, None]]:
        return backoff.constant(interval=3)

    def backoff_max_tries(self) -> int:
        return 8

    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures.

        Uses a wait generator defined in `backoff_wait_generator` to
        determine backoff behaviour. Try limit is defined in
        `backoff_max_tries`, and will trigger the event defined in
        `backoff_handler` before retrying. Developers may override one or
        all of these methods to provide custom backoff or retry handling.

        Args:
            func: Function to decorate.

        Returns:
            A decorated method.
        """
        decorator: Callable = backoff.on_exception(
            self.backoff_wait_generator,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,  # this is the only diff from the base
            ),
            max_tries=self.backoff_max_tries,
            on_backoff=self.backoff_handler,
        )(func)
        return decorator

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.

        Raises:
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.
        """
        url: str = self.get_url(context)
        headers = self.http_headers
        resp = requests.get(
            url=url,
            headers=headers,
            auth=HTTPDigestAuth(
                self.config.get("username"), self.config.get("password")
            ),
            stream=True,
        )
        yield from self.parse_response(resp)

    def parse_response(self, response: requests.Response) -> Iterable[Dict]:
        for i, c in enumerate(
            self.iter_decompressed_lines(self.decompress_stream(response))
        ):
            if c:
                if i == 1:
                    headers = c.decode().split("\t")
                elif i > 1:
                    c_arr = c.decode().split("\t")
                    yield {k: v.replace('""', "") for k, v in zip(headers, c_arr)}

    def format_key(self, key: str) -> str:
        """Format key for stream."""
        return key.replace(" ", "_").lower()

    def post_process(self, row: Dict, context: Optional[Dict]) -> Dict:
        """As needed, append or transform raw data to match expected structure."""
        proccesed_row = {
            **{self.format_key(k): v for k, v in row.items()},
            **{
                "filename": context["filename"],
                "report_type": context["report_type"],
                "last_modified": context["last_modified"],
            },
        }
        return proccesed_row


class EarningsReportStream(ReportStream):
    name = "EarningsReport"
    parent_stream_type = ReportListStream
    schema_filepath = SCHEMAS_DIR / "earnings-report.json"


class EarningsSubtagReportStream(ReportStream):
    name = "EarningsReportSubtags"
    parent_stream_type = ReportListStream
    schema_filepath = SCHEMAS_DIR / "earnings-subtag-report.json"

    def format_key(self, key: str) -> str:
        """Format key for stream."""
        return key.replace("Sub Tag", "subtag").replace(" ", "_").lower()


class OrdersReportStream(ReportStream):
    name = "OrdersReport"
    parent_stream_type = ReportListStream
    schema_filepath = SCHEMAS_DIR / "orders-report.json"


class OrdersSubtagReportStream(ReportStream):
    name = "OrdersReportSubtags"
    parent_stream_type = ReportListStream
    schema_filepath = SCHEMAS_DIR / "orders-subtag-report.json"

    def format_key(self, key: str) -> str:
        """Format key for stream."""
        return key.replace("Sub Tag", "subtag").replace(" ", "_").lower()


class TrackingReportStream(ReportStream):
    name = "TrackingReport"
    parent_stream_type = ReportListStream
    schema_filepath = SCHEMAS_DIR / "tracking-report.json"


class UtmSourceReportStream(ReportStream):
    name = "Cp_Utm_Source_ReportReport"
    parent_stream_type = ReportListStream
    schema_filepath = SCHEMAS_DIR / "utm-source-report.json"
