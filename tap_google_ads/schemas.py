from singer.schema import Schema
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.json_format import MessageToDict

# Name of reports supported by this tap
REPORTS = [
    "ad_group",
    "ad_group_ad",
    "campaign",
    "geographic_view",
    "keyword_view",
    "search_term_view",
    "video"
]

# List of commonly used attribute_resources(along with metrics and segments) supported by available reports
# TODO: if you add support of new report, all attribute_resources respected to report should be included in the list.
#     : Here you can check supported resources by reports
#     : https://developers.google.com/google-ads/api/fields/v10/overview_query_builder
LIST_ATTRIBUTE_RESOURCES = [
    "accessible_bidding_strategy",
    "ad_group",
    "ad_group_ad",
    "ad_group_criterion",
    "bidding_strategy",
    "campaign",
    "campaign_budget",
    "customer",
    "metrics",
    "segments"
]

# Google Ads Fields types to Singer Schema Property types mapping
FIELDS_TYPES_MAPPING = {
    "BOOLEAN": {"type": ["null", "boolean"]},
    "DATE": {"type": ["null", "string"], "format": "datetime"},
    "DOUBLE": {"type": ["null", "number"]},
    "ENUM": {"type": ["null", "string"]},
    "INT64": {"type": ["null", "integer"]},
    "RESOURCE_NAME": {"type": ["null", "string"]},
    "STRING": {"type": ["null", "string"]}
}

# This prefix helps to identify primary key fields(segments) in order to generate dynamic PK.
KEY_FIELD_PREFIXES = {
    "ad_group": ['segments'],
    "ad_group_ad": ['segments'],
    "campaign": ['segments'],
    "geographic_view": ['segments'],
    "keyword_view": ['segments'],
    "search_term_view": ['segments'],
    "video": ['segments']
}


def get_property_type(prop):
    """
    Replace Google Ads datatype with schema supported type
    """

    return FIELDS_TYPES_MAPPING.get(prop, {"type": ["null", "string"]})


def query_ads_field_service(ga_ads_service, resource):
    """
    Query to Google Ads Filed Service to get resource's metadata
    #searchgoogleadsfields
    ref: https://developers.google.com/google-ads/api/reference/rpc/v10/GoogleAdsFieldService
    """

    query = "SELECT name, data_type, selectable_with, metrics, segments, enum_values, attribute_resources" \
            f" WHERE name LIKE '{resource}'"

    streams = ga_ads_service.search_google_ads_fields(query=query)
    record_list = []
    for row in streams:
        row_dict = MessageToDict(row._pb, preserving_proto_field_name=True)
        record_list.append(row_dict)
    return record_list


def fetch_resource_fields(ga_ads_service, resource_name):
    """
    Fetch and refactor fields as per expected schema
    """

    list_fields = query_ads_field_service(ga_ads_service, resource_name + ".%")
    list_fields = {
        f["name"].replace(".", "__"): get_property_type(f["data_type"])
        for f in list_fields
    }
    return list_fields


def get_google_ads_field_service(config):
    """
    Return object of GoogleAdsFieldService
    """
    config["use_proto_plus"] = True
    googleads_client = GoogleAdsClient.load_from_dict(config)
    ga_ads_service = googleads_client.get_service(
        "GoogleAdsFieldService", version="v10")
    return ga_ads_service


def get_incompatible_fields(ga_ads_service):
    """
    Return list of incompatible fields for the metrics and segments
    here - we can't directly get incompatible fields, we received list of selectable fields for each fields.
         - Hence, to get incompatible fields, we remove selectable from all_fields.
    """

    attribute_resource_fields = {}
    for resource in ["metrics", "segments"]:
        list_fields = query_ads_field_service(ga_ads_service, resource + ".%")
        list_fields = {
            f["name"].replace(".", "__"): [selectable.replace(".", "__") for selectable in f.get("selectable_with", [])]
            for f in list_fields
        }
        attribute_resource_fields[resource] = list_fields
    all_metrics = [f for f in attribute_resource_fields["metrics"]]
    all_segments = [f for f in attribute_resource_fields["segments"]]

    # For Metrics, [ all_segments - selectable ], as metrics selectable does not have incompatible metrics
    incompatible_fields = {field: list(set(all_segments) - set(selectable) - {field})
                           for field, selectable
                           in attribute_resource_fields["metrics"].items()}

    # For Segments, [ ( all_segments + all_metrics ) - selectable ], as segments selectable can contains
    # both metrics and segments as incompatible metrics
    incompatible_fields.update({field: list((set(all_metrics + all_segments)) - set(selectable) - {field})
                                for field, selectable
                                in attribute_resource_fields["segments"].items()})
    return incompatible_fields


def create_report_definition(ga_ads_service,
                             report,
                             attribute_resource_fields,
                             schema):
    report_schema = {}
    for report_metadata in query_ads_field_service(ga_ads_service, report):
        for name, meta in report_metadata.items():
            if name in ["metrics", "segments", "attribute_resources"]:
                for m in meta:
                    m = m.replace(".", "__")
                    # if field name starts with "metrics" or "segments"
                    # E.x. metrics.clicks, segments.device
                    if m.find(name) == 0:
                        report_schema[m] = attribute_resource_fields[name][m]

                    # Along with outer scope attribute_resources, we can have some attribute_resources as a part
                    # of "metrics" or "segments", Hence updating all of them from attribute_resource_fields.
                    # E.x. ad_group, ad_group_ad, campaign
                    elif m.find(name) != 0:
                        report_schema.update(attribute_resource_fields[m])
    report_schema.update(fetch_resource_fields(ga_ads_service, report))
    schema[report] = Schema.from_dict({"type": ["null", "object"],
                                       "properties": report_schema})


def generate_schemas(ga_ads_service):
    """
    Dynamically generate schemas for available resources(reports)
    """

    # Fetch common attribute resources fields and refactor the datatype
    attribute_resource_fields = {}
    for resource in LIST_ATTRIBUTE_RESOURCES:
        attribute_resource_fields[resource] = fetch_resource_fields(
            ga_ads_service, resource)

    import concurrent.futures

    futures = []
    schema = {}
    for report in REPORTS:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures.append(executor.submit(create_report_definition,
                                            ga_ads_service,
                                            report,
                                            attribute_resource_fields,
                                            schema))

        for future in concurrent.futures.as_completed(futures):
            future.result()

    return schema
