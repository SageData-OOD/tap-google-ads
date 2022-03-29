#!/usr/bin/env python3
import os
import json
import concurrent.futures
import threading
import pandas as pd

import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.transform import transform
from datetime import datetime, timedelta

from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.json_format import MessageToDict


WORKER_THREADS = 10
LOCK = threading.Lock()
LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date", "end_date", "customer_ids", "client_id", "client_secret", "developer_token",
                        "refresh_token"]

REPORT_MAPPINGS = {
    "ad_performance_report": "ad_group_ad",
    # Also, it can be: user_location_view
    "geo_performance_report": "geographic_view",
    "campaign_performance_report": "campaign",
    "video_performance_report": "video",
    "adgroup_performance_report": "ad_group",
    "search_query_performance_report": "search_term_view"
}


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def get_key_properties(stream_name):
    key_properties = {
        "ad_performance_report": ["account_id", "created_ts", "campaign__id", "ad_group__id", "ad_group_ad__ad__id"],
        "adgroup_performance_report": ["account_id", "created_ts", "campaign__id", "ad_group__id"],
        "campaign_performance_report": ["account_id", "created_ts", "campaign__id"],
        "geo_performance_report": ["account_id", "created_ts", "campaign__id", "ad_group__id",
                                   "geographic_view__country_criterion_id", "segments__device"],
        "search_query_performance_report": ["account_id", "created_ts", "campaign__id", "ad_group__id",
                                            "search_term_view__search_term"],
        "video_performance_report": ["account_id", "created_ts", "campaign__id", "ad_group__id",
                                     "ad_group_ad__ad__id", "video__id"]
    }
    return key_properties.get(stream_name, [])


def create_metadata_for_report(schema, tap_stream_id):
    key_properties = get_key_properties(tap_stream_id)

    mdata = [{"breadcrumb": [], "metadata": {"inclusion": "available",
                                             "forced-replication-method": "INCREMENTAL",
                                             "valid-replication-keys": "created_ts",
                                             "table-key-properties": key_properties}}]

    for key in schema.properties:
        if "object" in schema.properties.get(key).type:
            for prop in schema.properties.get(key).properties:
                inclusion = "automatic" if prop in key_properties else "available"
                mdata.extend([{
                    "breadcrumb": ["properties", key, "properties", prop],
                    "metadata": {"inclusion": inclusion}
                }])
        else:
            inclusion = "automatic" if key in key_properties else "available"
            mdata.append({"breadcrumb": ["properties", key], "metadata": {"inclusion": inclusion}})

    return mdata


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = create_metadata_for_report(schema, stream_id)
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


def build_query(stream_id, schema, date_to_poll):
    report = REPORT_MAPPINGS[stream_id]
    fields, _ = get_report_fields_and_headers(schema)
    date = "{:%Y-%m-%d}".format(date_to_poll)
    query = f'SELECT {",".join(fields)} FROM {report} WHERE segments.date BETWEEN "{date}" AND "{date}"'
    return query


def transform_records(record_list, schema):
    transformed_records = []
    for rec in record_list:
        dic = {}
        fields, headers = get_report_fields_and_headers(schema)
        for i in range(len(fields)):
            value, err = json_value_from_dotted_path(fields[i], rec)
            if not err:
                dic[headers[i]] = value
        transformed_records.append(dic)
    return transformed_records


def query_report(date_to_poll,
                 stream_id,
                 schema,
                 adwords_account_id,
                 googleads_client,
                 records):
    query = build_query(stream_id, schema, date_to_poll)
    ga_service = googleads_client.get_service("GoogleAdsService", version="v9")
    streams = ga_service.search_stream(
        customer_id=adwords_account_id, query=query
    )

    LOGGER.info("Download %s for account: %s, date: %s", stream_id,
                adwords_account_id,
                date_to_poll)

    record_list = []
    for batch in streams:
        for row in batch.results:
            row_dict = MessageToDict(row._pb, preserving_proto_field_name=True)
            record_list.append(row_dict)

    # Records transformation as per DES, for 1:1 mapping
    transformed_records = transform_records(record_list, schema)

    df = pd.DataFrame(transformed_records)
    # shitty legacy transforms
    if "cost" in df:
        df["cost"] = df["cost"].apply(lambda x: int(x) / 1000000)

    df["created_ts"] = str(date_to_poll)
    df["account_id"] = adwords_account_id

    # Google ads to google adwords compat transforms
    for column in df.columns:
        if column.find("rate") != -1:
            df[column] = pd.to_numeric(df[column], errors='coerce')
        elif column in ("target_cpa", "cpc_bid") or column.find("search_") == 0:
            df[column] = df[column].astype(str)

    df = df.applymap(lambda x: None if str(x).strip() in ["--", "nan", "NAN"] else x)

    with LOCK:
        records += df.to_dict(orient="records")


def get_report_fields_and_headers(schema):
    headers = [prop for prop in schema["properties"]]

    # Excluding created_ts & account_id from fields as they are not part of query fields.
    fields_to_exclude = ["created_ts", "account_id"]
    for e in fields_to_exclude:
        if e in headers:
            headers.remove(e)

    fields = [f.replace("__", ".") for f in headers]
    return fields, headers


def sync(config, state, catalog):
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        bookmark_column = "date_to_poll"
        mdata = metadata.to_map(stream.metadata)
        schema = stream.schema.to_dict()

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=schema,
            key_properties=stream.key_properties,
        )
        futures = []
        records = []

        max_worker_threads = min(WORKER_THREADS, len(
            config["customer_ids"]))
        LOGGER.info("Download reports with max workers: %d", max_worker_threads)

        start_date = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column) \
            if state.get("bookmarks", {}).get(stream.tap_stream_id) else config["start_date"]

        date_to_poll = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(config["end_date"], "%Y-%m-%d")
        config["use_proto_plus"] = True

        while date_to_poll <= end_date:
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
                                                   schema,
                                                   account_parts[-1],
                                                   googleads_client,
                                                   records))

            for future in concurrent.futures.as_completed(futures):
                future.result()

            with singer.metrics.record_counter(stream.tap_stream_id) as counter:
                for row in records:
                    # Type Conversation and Transformation
                    transformed_data = transform(row, schema, metadata=mdata)

                    # write one or more rows to the stream:
                    singer.write_records(stream.tap_stream_id, [transformed_data])
                    counter.increment()
            state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, str(date_to_poll.date()))
            singer.write_state(state)
            date_to_poll += timedelta(days=1)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        state = args.state or {}
        sync(args.config, state, catalog)


if __name__ == "__main__":
    main()
