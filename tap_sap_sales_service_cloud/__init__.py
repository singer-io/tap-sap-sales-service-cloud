"""tap_sap_sales_service_cloud — Singer tap for SAP Sales and Service Cloud.

Entry point and CLI dispatch.
"""

import json
import sys

import singer

from tap_sap_sales_service_cloud.client import SAPSalesServiceCloudClient
from tap_sap_sales_service_cloud.discover import discover
from tap_sap_sales_service_cloud.sync import sync

LOGGER = singer.get_logger()

# Minimum required configuration keys.
REQUIRED_CONFIG_KEYS = ["api_server", "start_date",
                        "username", "password"]


def do_discover(client: SAPSalesServiceCloudClient) -> None:
    """Run discovery mode and emit the catalog to stdout."""
    LOGGER.info("Starting discover")
    catalog = discover(client)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info("Finished discover")


@singer.utils.handle_top_exception(LOGGER)
def main() -> None:
    """Tap entry point: parse args then dispatch to discover or sync."""
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = parsed_args.config

    with SAPSalesServiceCloudClient(config) as client:
        state = {}
        if parsed_args.state:
            state = parsed_args.state

        if parsed_args.discover:
            do_discover(client)
        elif parsed_args.catalog:
            sync(
                client=client,
                config=config,
                catalog=parsed_args.catalog,
                state=state,
            )


if __name__ == "__main__":
    main()
