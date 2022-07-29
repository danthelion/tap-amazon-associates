"""amazon_associates tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
# TODO: Import your custom stream types here:
from tap_amazon_associates.streams import (
    ReportListStream,
    EarningsReportStream,
    OrdersReportStream,
    TrackingReportStream,
    EarningsSubtagReportStream,
    OrdersSubtagReportStream,
    UtmSourceReportStream
)
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    ReportListStream,
    EarningsReportStream,
    OrdersReportStream,
    TrackingReportStream,
    EarningsSubtagReportStream,
    OrdersSubtagReportStream,
    UtmSourceReportStream
]


class TapAmazonAssociates(Tap):
    """amazon_associates tap class."""
    name = "tap-amazon-associates"

    # TODO: Update this section with the actual config values you expect:
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
            description="The earliest record date to sync"
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://assoc-datafeeds-na.amazon.com",
            description="The url for the API service"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
