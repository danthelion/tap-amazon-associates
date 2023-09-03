from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th
from tap_amazon_associates.streams import (
    ReportListStream,
    EarningsReportStream,
    OrdersReportStream,
    TrackingReportStream,
    EarningsSubtagReportStream,
    OrdersSubtagReportStream,
    UtmSourceReportStream,
)

STREAM_TYPES = [
    ReportListStream,
    EarningsReportStream,
    OrdersReportStream,
    TrackingReportStream,
    EarningsSubtagReportStream,
    OrdersSubtagReportStream,
    UtmSourceReportStream,
]


class TapAmazonAssociates(Tap):
    name = "tap-amazon-associates"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "username",
            th.StringType,
            required=True,
            description="Amazon Associates username",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="Amazon Associates password",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://assoc-datafeeds-na.amazon.com",
            description="The url for the API service",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
