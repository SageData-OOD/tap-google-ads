import os
import json
import concurrent.futures
import threading

import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.transform import transform
from datetime import datetime, timedelta

from .schemas import *

WORKER_THREADS = 10
LOCK = threading.Lock()
LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date", "end_date", "customer_ids", "client_id", "client_secret", "developer_token",
                        "refresh_token"]


def get_key_properties(stream_name, selected_fields=[]):
    """
    Returns list of primary key columns as per report.
    It will dynamically generate list of keys based on given selected field segments.
    """

    default_key_properties = {
        "ad_group_ad": ["segments__date", "customer__id", "campaign__id", "ad_group__id", "ad_group_ad__ad__id"],
        "ad_group": ["segments__date", "customer__id", "campaign__id", "ad_group__id"],
        "campaign": ["segments__date", "customer__id", "campaign__id"],
        "click_view": ["segments__date", "customer__id", "campaign__id", "ad_group__id", "click_view__gclid"],
        "geographic_view": ["segments__date", "customer__id", "campaign__id", "ad_group__id",
                            "geographic_view__country_criterion_id"],
        "search_term_view": ["segments__date", "customer__id", "campaign__id", "ad_group__id",
                             "search_term_view__search_term"],
        "video": ["segments__date", "customer__id", "campaign__id", "ad_group__id",
                  "ad_group_ad__ad__id", "video__id", "video__channel_id"],
        "keyword_view": ["segments__date", "customer__id", "campaign__id", "ad_group__id",
                         "ad_group_criterion__criterion_id", "keyword_view__resource_name"]
    }
    key_field_prefixes = KEY_FIELD_PREFIXES[stream_name]
    dynamic_key_fields = [k for k in selected_fields if k.split("__")[0] in key_field_prefixes]
    return list(set(default_key_properties.get(stream_name, []) + dynamic_key_fields))


def create_metadata_for_report(schema, tap_stream_id, incompatible_fields):
    key_properties = get_key_properties(tap_stream_id)
    mdata = [{"breadcrumb": [], "metadata": {"inclusion": "available",
                                             "forced-replication-method": "INCREMENTAL",
                                             "valid-replication-keys": "segments__date",
                                             "table-key-properties": key_properties}}]

    # Fields inside fieldExclusions of the Fields which is having automatic inclusion, should always be unselected
    # as automatic inclusion fields are always selected, hence skipping those Exclusion fields.
    fields_to_skip = [skip for k in key_properties for skip in incompatible_fields.get(k, [])]
    for key in schema.properties:
        if key in fields_to_skip:
            continue

        if "object" in schema.properties.get(key).type:
            for prop in schema.properties.get(key).properties:
                inclusion = "automatic" if prop in key_properties else "available"
                mdata.extend([{
                    "breadcrumb": ["properties", key, "properties", prop],
                    "metadata": {"inclusion": inclusion}
                }])
        else:
            inclusion = "automatic" if key in key_properties else "available"
            mdata.append({"breadcrumb": ["properties", key],
                          "metadata": {"inclusion": inclusion,
                                       "fieldExclusions": [["properties", i] for i in incompatible_fields.get(key, [])]}
                          })

    return mdata


def discover(config):
    ga_ads_service = get_google_ads_field_service(config)
    incompatible_fields = get_incompatible_fields(ga_ads_service)
    raw_schemas = generate_schemas(ga_ads_service)
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = create_metadata_for_report(schema, stream_id, incompatible_fields)
        key_properties = get_key_properties(stream_id)
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata
            )
        )
    return Catalog(streams)


def json_value_from_dotted_path(path, json_obj):
    """
    e.x. json_obj={ "key1": { "key2": { "key3": value_1 } } }
         path = key1.key2.key3
          ->  return value_1, 0

         if key not found then
          ->  return None, 1
    """

    keys = path.split('.')
    rv = json_obj
    for key in keys:
        key = "type_" if key == "type" else key
        try:
            rv = rv[key]
        except KeyError:
            return None, 1
    return rv, 0


def build_query(stream_id, fields, date_to_poll):
    """
    Generate query as per Google_Ads API V10
    google_ads query builder UI: https://developers.google.cn/google-ads/api/fields/v10/ad_group_ad_query_builder
    """

    date = "{:%Y-%m-%d}".format(date_to_poll)
    query = f'SELECT {",".join(fields)} FROM {stream_id} WHERE segments.date '

    # For "click_view" report query, `WHERE` clause specifying a single day within the last 90 days.
    query += f'BETWEEN "{date}" AND "{date}"' if stream_id != "click_view" else f'= "{date}"'

    return query


def flatten_records(record_list, fields, headers):
    """
    It will flatten the records as per schema fields
    Ex. {"abc": {"xyz": {"mnp": 1}}}  ->  {"abc__xyz__mnp": 1}
    """

    transformed_records = []
    for rec in record_list:
        dic = {}
        for idx, f in enumerate(fields):
            value, err = json_value_from_dotted_path(f, rec)
            if not err:
                dic[headers[idx]] = value
        transformed_records.append(dic)
    return transformed_records


def query_report(date_to_poll,
                 stream_id,
                 fields,
                 adwords_account_id,
                 googleads_client,
                 records):
    dotted_path_fields = [f.replace("__", ".") for f in fields]
    query = build_query(stream_id, dotted_path_fields, date_to_poll)
    ga_service = googleads_client.get_service("GoogleAdsService", version="v10")
    streams = ga_service.search_stream(
        customer_id=adwords_account_id, query=query
    )

    LOGGER.info("Download %s for account: %s, date: %s", stream_id,
                adwords_account_id,
                date_to_poll)
    LOGGER.info("Query -----> %s", query)

    record_list = []
    for batch in streams:
        for row in batch.results:
            row_dict = MessageToDict(row._pb, preserving_proto_field_name=True)
            record_list.append(row_dict)

    flattened_records = flatten_records(record_list, dotted_path_fields, fields)

    with LOCK:
        records += flattened_records


def get_selected_attrs(stream):
    """
    List down user selected fields
    requires [ "selected": true ] inside property metadata
    """

    list_attrs = list()
    for md in stream.metadata:
        if md["breadcrumb"]:
            if md["metadata"].get("selected", False) or md["metadata"].get("inclusion") == "automatic":
                list_attrs.append(md["breadcrumb"][-1])

    return list_attrs


def get_verified_date_to_poll(stream_id, date_to_poll):
    """
    For "click_view" report query, `WHERE` clause specifying a single day within the last 90 days of current date.
    so, if date_to_poll is less than current date, it will be changed to the least possible date value (today - 90 days)
    """
    
    utcnow = datetime.utcnow()

    if stream_id == "click_view":
        minimal_date_to_poll = utcnow - timedelta(days=90)
        if date_to_poll < minimal_date_to_poll:
            date_to_poll = minimal_date_to_poll
    
    # fix for data freshness
    # e.g. Sunday's data is available at 3 AM UTC on Monday
    # If integration is set to sync at 1AM then a problem occurs

    if date_to_poll >= utcnow - timedelta(days=3):
        date_to_poll = utcnow - timedelta(days=4)
    
    return date_to_poll


def sync(config, state, catalog):
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        bookmark_column = "segments__date"
        mdata = metadata.to_map(stream.metadata)
        schema = stream.schema.to_dict()

        fields = get_selected_attrs(stream)
        dynamically_generated_key_fields = get_key_properties(stream.tap_stream_id, fields)
        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=schema,
            key_properties=dynamically_generated_key_fields
        )

        max_worker_threads = min(WORKER_THREADS, len(
            config["customer_ids"]))
        LOGGER.info("Download reports with max workers: %d", max_worker_threads)

        bookmark = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column) \
            if state.get("bookmarks", {}).get(stream.tap_stream_id) else config["start_date"]

        date_to_poll = get_verified_date_to_poll(stream.tap_stream_id, datetime.strptime(bookmark, "%Y-%m-%d"))

        end_date = datetime.strptime(config["end_date"], "%Y-%m-%d")
        config["use_proto_plus"] = True

        while date_to_poll <= end_date:
            # These must be here to reset records/futures from previous date
            futures = []
            records = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_worker_threads) as executor:
                for adwords_account_id in config["customer_ids"]:
                    if "login_customer_id" in config:
                        config.pop("login_customer_id")

                    # if account is in the format xxx:yyy it means its accessed via MCC account xxx
                    # otherwise it's access directly
                    account_parts = adwords_account_id.split(':')
                    if len(account_parts) > 1:
                        config["login_customer_id"] = account_parts[0]

                    # google-ads client per account, since we download stuff in parallel
                    googleads_client = GoogleAdsClient.load_from_dict(config)

                    futures.append(executor.submit(query_report,
                                                   date_to_poll,
                                                   stream.tap_stream_id,
                                                   fields,
                                                   account_parts[-1],
                                                   googleads_client,
                                                   records))

            for future in concurrent.futures.as_completed(futures):
                future.result()

            with singer.metrics.record_counter(stream.tap_stream_id) as counter:
                for row in records:
                    transformed_data = transform(row, schema, metadata=mdata)

                    # write one or more rows to the stream:
                    singer.write_records(stream.tap_stream_id, [transformed_data])
                    counter.increment()
                    bookmark = max([bookmark, transformed_data[bookmark_column]])

            # DP: Google Ads produces tons of data and while this check is the correct
            # thing to do in the general case I will disable it

            # if records:

            state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, bookmark)
            singer.write_state(state)

            date_to_poll += timedelta(days=1)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover(args.config)
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover(args.config)
        state = args.state or {}
        sync(args.config, state, catalog)


if __name__ == "__main__":
    main()
