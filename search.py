# Copyright 2024 Vitus Haberzettl
# Copyright 2024, 2025 Lorenz Haberzettl
#
#
# This file is part of Search Analytics Node.
#
# Search Analytics Node is free software: you can redistribute it and/or modify it under the terms
# of the GNU General Public License as published by the Free Software Foundation, either version 3
# of the License, or (at your option) any later version.
#
# Search Analytics Node is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# Search Analytics Node. If not, see <https://www.gnu.org/licenses/>.


import logging
import pickle
import copy
import pandas
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
import itertools

import knime.extension as knext
from googleapiclient.discovery import build

import lib.api_request_delay
import lib.credentials
import lib.key_management
import lib.property_parameter


KNIME_NODE_KEYWORDS = [
    "Google Search Console",
    "GSC"
]


logger = logging.getLogger(__name__)


category = knext.category(
    path="/community",
    level_id="san_ext",
    name="Search Analytics Node",
    description="Nodes to query your Search Console data",
    icon="icons/query.png"
)


class SearchAuthPortSpec(knext.PortObjectSpec):
    def serialize(self):
        pass

    @classmethod
    def deserialize(cls, data: dict):
        pass



class SearchAuthPortObject(knext.PortObject):
    def __init__(self, spec: SearchAuthPortSpec, credentials, is_pro) -> None:
        super().__init__(spec)
        self._credentials = credentials
        self._is_pro = is_pro


    def serialize(self) -> bytes:
        payload = {
            "port_version": 2,
            "credentials": self._credentials,
            "is_pro": self._is_pro
        }
        return pickle.dumps(payload)


    @classmethod
    def deserialize(cls, spec: SearchAuthPortSpec, storage: bytes) -> "SearchAuthPortObject":
        payload = pickle.loads(storage)
        if isinstance(payload, dict):
            version = payload["port_version"]
        else:
            version = 1
        
        if version == 2:
            # Version 2 payload structure:
            # {
            #   "port_version": 2,
            #   "credentials": <str>,
            #   "is_pro": <bool>
            # }
            credentials = payload["credentials"]
            is_pro = payload["is_pro"]
        elif version == 1:
            # Version 1 (deprecated) payload structure:
            # Only contains the credentials <str> without metadata.
            credentials = payload
            is_pro = False
        else:
            raise RuntimeError(
                "Unknown Auth Port version! Reset and execute the Authenticator node again."
            )

        return cls(spec, credentials, is_pro)


    def get_credentials(self):
        return self._credentials


    def get_is_pro(self):
        return self._is_pro


search_auth_port_type = knext.port_type(
    name="Search Authentication Port",
    spec_class=SearchAuthPortSpec,
    object_class=SearchAuthPortObject
)


@knext.node(name="Search Analytics - Authenticator", node_type=knext.NodeType.OTHER, icon_path="icons/authenticator.png", category=category, keywords=KNIME_NODE_KEYWORDS)
@knext.output_port(name="Search Analytics Auth Port", description="Emits authentication credentials for downstream nodes.", port_type=search_auth_port_type)
class SearchAuthenticator:
    """Connects your Google account with your workflow.
    Connects your Google account with your workflow.

    When executed, this node opens a browser window where you **sign in and grant access**.

    The node outputs authentication credentials that other Search Analytics nodes use to fetch data.
    """


    class ExpirationOptions(knext.EnumParameterOptions):
        one_hour = ("One Hour", "Authentication will expire after one hour.")
        never = ("Never", "Authentication will not expire.")

    expiration = knext.EnumParameter(
        label="Authentication Expiration Time",
        description="Specifies when your Google account authentication expires. After expiration, you must re-authenticate by executing the node again.\n\n**Heads up! Your authentication details are saved in your workflow. If you share a workflow with an executed Authenticator node, anyone who has access to the workflow can use it to run queries on your Google Search Console properties.**",
        default_value=ExpirationOptions.one_hour.name,
        enum=ExpirationOptions,
        style=knext.EnumParameter.Style.DROPDOWN
    )


    key = knext.StringParameter(
        label="License Key (Optional)",
        description="Upgrade with a license key and supercharge your workflow:\n\n🔥 **Query node**: Remove the 100,000 row cap and fetch all available data\n\n🔥 **URL Inspection node**: Up to 10x faster execution thanks to parallel processing",
        default_value="",
        since_version="1.7.0"
    )


    def configure(self, config_context):
        return SearchAuthPortSpec()


    def set_available_props(self, exec_context, credentials):
        service = build(
            serviceName="searchconsole",
            version="v1",
            credentials=credentials
        )
        
        sites_list_result = service.sites().list().execute()
        
        service.close()

        site_entries = sites_list_result.get("siteEntry", [])

        if len(site_entries) == 0:
            exec_context.set_warning("The selected Google account does not have any Search Console properties.")

        available_props = []
        for e in site_entries:
            if "unverified".casefold() in e["permissionLevel"].casefold():
                continue
            available_props.append(e["siteUrl"])
        
        if len(site_entries) > 0 and len(available_props) == 0:
            exec_context.set_warning("The selected Google Account does not have any verified Search Console properties. Verify your property and then execute again.")

        exec_context.flow_variables["available_props"] = available_props


    def execute(self, exec_context):
        is_pro = False
        if len(self.key.strip()) != 0:
            is_pro = lib.key_management.verify_key(key=self.key.strip())

        credentials = lib.credentials.create_new(exec_context=exec_context)

        self.set_available_props(exec_context=exec_context, credentials=credentials)

        strip_keys = []
        if self.expiration != self.ExpirationOptions.never.name:
            strip_keys.append("refresh_token")

        return SearchAuthPortObject(
            spec=SearchAuthPortSpec(),
            credentials=credentials.to_json(strip=strip_keys),
            is_pro=is_pro
        )


@knext.parameter_group(label="Property and Type")
class PropertyTypeParameterGroup:
    property = lib.property_parameter.create()


    class TypeOptions(knext.EnumParameterOptions):
        web = ("Web", "Only *(standard) Google Search \"All\" tab* traffic.")
        discover = ("Discover", "Only *Google Discover* traffic.")
        googleNews = ("GoogleNews", "Only *news.google.com and Google News app (Android and iOS)* traffic, excluding Google Search \"News\" tab.")
        news = ("News", "Only *Google Search \"News\"* tab traffic.")
        image = ("Image", "Only *Google Search \"Images\"* tab traffic.")
        video = ("Video", "Only *Google Search \"Videos\"* tab traffic.")
    
    type = knext.EnumParameter(
        label="Search Type",
        description="Limit results to the specified type.",
        default_value=TypeOptions.web.name,
        enum=TypeOptions,
        style=knext.EnumParameter.Style.DROPDOWN
    )


@knext.parameter_group(label="Date Range")
class DateRangeParameterGroup:
    class IntervalOptions(knext.EnumParameterOptions):
        d7 = ("Last 7 Days", "Query for the last 7 days.")
        d28 = ("Last 28 Days", "Query for the last 28 days.")
        d90 = ("Last 90 Days", "Query for the last 90 days.")
        d180 = ("Last 180 Days", "Query for the last 180 days.")
        d365 = ("Last 365 Days", "Query for the last 365 days.")
        custom = ("Custom Interval", "Select a custom interval by manually specifying a start (inclusive) and end date (inclusive).")

    interval = knext.EnumParameter(
        label="Interval",
        description="Specify the date range to limit results. **Except for the custom interval option, all the interval options end three days ago to ensure the results are fixed and will not change anymore.**",
        default_value=IntervalOptions.d7.name,
        enum=IntervalOptions,
        since_version="1.1.0",
        style=knext.EnumParameter.Style.DROPDOWN
    )
    
    # If the show_time parameter is set to False, DateTimeParameter will return a date object;
    # otherwise, a datetime object is returned.
    custom_start_date = knext.DateTimeParameter(
        label="Start",
        description="Limit results to the specified interval start date (inclusive).",
        default_value=datetime(year=datetime.now(tz=timezone.utc).year, month=1, day=1),
        show_time=False
    ).rule(
        condition=knext.OneOf(subject=interval, values=[IntervalOptions.custom.name]),
        effect=knext.Effect.SHOW
    )
    
    custom_end_date = knext.DateTimeParameter(
        label="End",
        description="Limit results to the specified interval end date (inclusive).",
        default_value=datetime.now(tz=timezone.utc) - timedelta(days=3),
        show_time=False
    ).rule(
        condition=knext.OneOf(subject=interval, values=[IntervalOptions.custom.name]),
        effect=knext.Effect.SHOW
    )


@knext.parameter_group(label="Group By Dimension")
class DimensionParameterGroup:
    date = knext.BoolParameter(label="Date", description="Break down the results by date.", default_value=False)
    country = knext.BoolParameter(label="Country", description="Break down the results by country.", default_value=False)
    device = knext.BoolParameter(label="Device", description="Break down the results by device.", default_value=False)
    page = knext.BoolParameter(label="Page", description="Break down the results by page.", default_value=False)
    query = knext.BoolParameter(label="Query", description="Break down the results by query.", default_value=False)
    search_appearance = knext.BoolParameter(label="Search Appearance", description="Break down the results by search appearance.", default_value=False)


@knext.parameter_group(label="Advanced", is_advanced=True)
class AdvancedParameterGroup:
    class DataStateOptions(knext.EnumParameterOptions):
        final = ("Final", "Only include the final data. The data may be delayed by a few days but will not change anymore.")
        all = ("All", "Include fresh data. The results will contain more recent data, which has yet to be finalized and is, thus, subject to change.")
    
    data_state = knext.EnumParameter(
        label="Data State",
        description="Specify the data state of the results.",
        default_value=DataStateOptions.final.name,
        enum=DataStateOptions,
        style=knext.EnumParameter.Style.DROPDOWN
    )


    class AggregationOptions(knext.EnumParameterOptions):
        auto = ("Auto", "Google automatically selects the most appropriate aggregation type.")
        byPage = ("Page", "Aggregate data by page.")
        byProperty = ("Property", "Aggregate data by property.")
        byNewsShowcasePanel = ("NewsShowcasePanel", "Aggregate data by News Showcase Panel.")
    
    aggregation = knext.EnumParameter(
        label="Aggregation Type",
        description="Select the aggregation type. For more information, refer to [Google's documentation](https://support.google.com/webmasters/answer/6155685#urlorsite).",
        default_value=AggregationOptions.auto.name,
        enum=AggregationOptions,
        style=knext.EnumParameter.Style.DROPDOWN
    )


    row_limit = knext.IntParameter(
        label="Row Limit",
        description="Set the maximum number of rows to return. **Use 0 to remove the limit** and fetch all available data.\n\n**Note:** Google may still impose restrictions based on query complexity, meaning very large or complex requests might take a long time or could result in a server error. Simplifying the query (e.g., shorter date ranges or fewer dimensions) can help improve speed and reliability.",
        default_value=0,
        min_value=0,
        max_value=100000000000000
    )


@knext.node(name="Search Analytics - Query", node_type=knext.NodeType.SOURCE, icon_path="icons/query.png", category=category, keywords=KNIME_NODE_KEYWORDS)
@knext.input_port(name="Search Analytics Auth Port", description="Receives authentication credentials from a *Search Analytics - Authenticator* node.", port_type=search_auth_port_type)
@knext.output_table(name="Result Table", description="Output table containing search performance metrics based on your selected options.")
class SearchQuery:
    """Fetches search performance data from the Google Search Console API.
    Fetches **search performance data** from the Google Search Console API.

    Use this node to retrieve metrics such as:

    - Impressions
    - Clicks
    - Position
    - Search queries
    - Pages
    - Countries
    - Devices, and more

    ⚠️ This node requires a connected and executed **Search Analytics - Authenticator** node.
    """


    property_type = PropertyTypeParameterGroup()
    date_range = DateRangeParameterGroup()
    dimension = DimensionParameterGroup()
    advanced = AdvancedParameterGroup()

    
    def get_date_range(self):
        if self.date_range.interval == DateRangeParameterGroup.IntervalOptions.custom.name:
            return self.date_range.custom_start_date, self.date_range.custom_end_date
        
        today = datetime.now(tz=timezone.utc).date()
        end_date = today - timedelta(days=3)

        date_delta = 365 - 1
        match (self.date_range.interval):
            case DateRangeParameterGroup.IntervalOptions.d7.name:
                date_delta = 7 - 1
            case DateRangeParameterGroup.IntervalOptions.d28.name:
                date_delta = 28 - 1
            case DateRangeParameterGroup.IntervalOptions.d90.name:
                date_delta = 90 - 1
            case DateRangeParameterGroup.IntervalOptions.d180.name:
                date_delta = 180 - 1
        start_date = end_date - timedelta(days=date_delta)
        return start_date, end_date
    

    def get_selected_dimensions(self):
        selected = []

        if self.dimension.date:
            selected.append("date")
        if self.dimension.country:
            selected.append("country")
        if self.dimension.device:
            selected.append("device")
        if self.dimension.page:
            selected.append("page")
        if self.dimension.query:
            selected.append("query")
        if self.dimension.search_appearance:
            selected.append("searchAppearance")

        return selected


    def get_request_body(self, row_limit, start_row):
        body = {}

        start_date, end_date = self.get_date_range()

        body["type"] = self.property_type.type
        body["startDate"] = start_date.isoformat()
        body["endDate"] = end_date.isoformat()
        body["dimensions"] = self.get_selected_dimensions()
        body["rowLimit"] = row_limit
        body["startRow"] = start_row
        body["dataState"] = self.advanced.data_state
        body["aggregationType"] = self.advanced.aggregation

        return body


    def parse_response(self, api_response):
        dimensions = self.get_selected_dimensions()
        new_rows = []

        if "rows" not in api_response:
            return new_rows

        for row in api_response["rows"]:
            new_row = {}

            for i, dim in enumerate(dimensions):
                new_row[dim] = row["keys"][i]

            new_row.update(copy.deepcopy(row))
            if "keys" in new_row:
                del new_row["keys"]
            
            new_rows.append(new_row)

        return new_rows


    def configure(self, config_context, auth_port_spec):
        pass


    def execute(self, exec_context, auth_port_object):
        if self.property_type.property is None:
            raise ValueError("No value for 'Property' parameter selected!")
        
        service = build(
            serviceName="searchconsole",
            version="v1",
            credentials=lib.credentials.parse_json(auth_port_object.get_credentials())
        )

        api_row_limit = 25000
        user_row_limit = self.advanced.row_limit
        rows = []

        if auth_port_object.get_is_pro() != True and (user_row_limit == 0 or user_row_limit > 100000):
            user_row_limit = 100000

        i = 0
        while True:
            start_row = i * api_row_limit

            exec_context.set_progress(
                (start_row / user_row_limit) if user_row_limit != 0 else 0.5,
                "Fetched " + str(start_row) + " rows and counting ...",
            )

            api_response = service.searchanalytics().query(
                siteUrl=self.property_type.property,
                body=self.get_request_body(row_limit=api_row_limit, start_row=start_row)
            ).execute()

            new_rows = self.parse_response(api_response)
            rows += new_rows

            if user_row_limit != 0 and user_row_limit <= len(rows):
                rows = rows[:user_row_limit]
                break

            if len(new_rows) < api_row_limit:
                break

            i += 1
            time.sleep(lib.api_request_delay.get(i))

        service.close()

        if (
            auth_port_object.get_is_pro() != True
            and len(rows) == 100000
            and (self.advanced.row_limit == 0 or self.advanced.row_limit > 100000)
        ):
            exec_context.set_warning(
                "You've reached the 100,000 row limit. Want more? Enter a valid license key in the Authenticator node to unlock the complete dataset."
            )

        return knext.Table.from_pandas(
            data=pandas.DataFrame(data=rows),
            row_ids="auto"
        )


@knext.parameter_group(label="Property and URL Table Column")
class UrlInspectionPropertyInspectionUrlColumnParameterGroup:
    property = lib.property_parameter.create()
    inspection_url_column = knext.ColumnParameter(
        label="URL Table Column",
        description="Select the column which contains the URLs you would like to inspect. You can only inspect the URLs of your verified properties.",
        port_index=1
    )


@knext.parameter_group(label="Modules")
class UrlInspectionModulesParameterGroup:
    index_status = knext.BoolParameter(label="Index Status", description="Include the **Index Status (IS)** values in the output: Coverage State, Crawled As, Google Canonical, Indexing State, Last Crawl Time, Page Fetch State, Referring URLs, robots.txt State, Sitemaps, User Canonical, and Verdict.", default_value=True)
    mobile_usability = knext.BoolParameter(label="Mobile Usability", description="Include the **Mobile Usability (MU)** values in the output: Issues and Verdict.", default_value=False)
    accelerated_mobile_pages = knext.BoolParameter(label="Accelerated Mobile Pages", description="Include the **Accelerated Mobile Pages (AMP)** values in the output: Index Status Verdict, URL, Indexing State, Issues, Last Crawl Time, Page Fetch State, robots.txt State, and Verdict.", default_value=False)
    rich_results = knext.BoolParameter(label="Rich Results", description="Include the **Rich Results (RR)** values in the output. Since the RR data is nested, it is always provided in JSON format. The 'Output Results as JSON' setting does not affect this behavior.", default_value=False)


@knext.parameter_group(label="Advanced", is_advanced=True)
class UrlInspectionAdvancedParameterGroup:
    add_web_link = knext.BoolParameter(label="Add Link to Google Search Console Website", description="Include a link to the Google Search Console Website in the output to view the results directly in your browser.", default_value=False)
    json = knext.BoolParameter(label="Output Results as JSON", description="Output the results as JSON instead of splitting them into separate table columns. Rich Result values are always returned as JSON, regardless of this setting.", default_value=False)


@knext.node(name="Search Analytics - URL Inspection", node_type=knext.NodeType.SOURCE, icon_path="icons/url-inspection.png", category=category, keywords=KNIME_NODE_KEYWORDS)
@knext.input_port(name="Search Analytics Auth Port", description="Receives authentication credentials from a *Search Analytics - Authenticator* node.", port_type=search_auth_port_type)
@knext.input_table(name="URL Table", description="Input table with URLs to inspect.")
@knext.output_table(name="Result Table", description="Output table with inspection results for each URL.")
class UrlInspection:
    """Fetches URL-level diagnostics using the Google Search Console URL Inspection API.
    Fetches **URL-level diagnostics** using the Google Search Console URL Inspection API.

    For each URL, this node returns details such as:

    - Index Status
    - Mobile Usability
    - Accelerated Mobile Pages
    - Rich Results

    Google allows up to **2,000 URL inspections per property each day**. If you exceed this limit, subsequent requests will fail with a *quota-exceeded* error. The quota resets automatically every 24 hours.

    ⚠️ This node requires a connected and executed **Search Analytics - Authenticator** node.
    """


    property_inspection_url_column = UrlInspectionPropertyInspectionUrlColumnParameterGroup()
    modules = UrlInspectionModulesParameterGroup()
    advanced = UrlInspectionAdvancedParameterGroup()
    

    def configure(self, config_context, auth_port_spec, inspection_url_port_spec):
        pass


    def build_row(self, url, api_response):
        row = {
            "URL": url
        }

        if self.advanced.add_web_link == True:
            row["Web Link"] = api_response.get("inspectionResult", {}).get("inspectionResultLink", "")

        if self.modules.index_status == True:
            row.update(self.get_index_status_columns(api_response))

        if self.modules.mobile_usability == True:
            row.update(self.get_mobile_usability_columns(api_response))

        if self.modules.accelerated_mobile_pages == True:
            row.update(self.get_accelerated_mobile_pages_columns(api_response))

        if self.modules.rich_results == True:
            row.update(self.get_rich_results_columns(api_response))
        
        return row
    

    # Google's API only includes those parameters in the response for which it returns values.
    # We add defaults to ensure the JSON data contains these keys, even if they have no value set.
    def ensure_keys(self, d: dict, none_keys=None, list_keys=None):
        if none_keys is None:
            none_keys = []

        if list_keys is None:
            list_keys = []

        for k in none_keys:
            d[k] = d.get(k, None)

        for k in list_keys:
            d[k] = d.get(k, [])

        return d
    

    def get_index_status_columns(self, api_response):
        isr = api_response.get("inspectionResult", {}).get("indexStatusResult", {})
        isr = self.ensure_keys(d=isr, none_keys=["coverageState", "crawledAs", "googleCanonical",
            "indexingState", "lastCrawlTime", "pageFetchState", "robotsTxtState", "userCanonical",
            "verdict"], list_keys=["referringUrls", "sitemap"])

        if self.advanced.json == True:
            return {
                "IS: JSON": isr
            }
        
        # Some columns get special handling in terms of formatting
        referring_urls_column = None
        if 0 < len(isr["referringUrls"]):
            referring_urls_column = "\n".join(isr["referringUrls"])
        sitemap_column = None
        if 0 < len(isr["sitemap"]):
            sitemap_column = "\n".join(isr["sitemap"])

        return {
            "IS: Coverage State": isr["coverageState"],
            "IS: Crawled As": isr["crawledAs"],
            "IS: Google Canonical": isr["googleCanonical"],
            "IS: Indexing State": isr["indexingState"],
            "IS: Last Crawl Time": isr["lastCrawlTime"],
            "IS: Page Fetch State": isr["pageFetchState"],
            "IS: Referring URLs": referring_urls_column,
            "IS: robots.txt State": isr["robotsTxtState"],
            "IS: Sitemaps": sitemap_column,
            "IS: User Canonical": isr["userCanonical"],
            "IS: Verdict": isr["verdict"]
        }
    

    def get_mobile_usability_columns(self, api_response):
        mur = api_response.get("inspectionResult", {}).get("mobileUsabilityResult", {})
        mur = self.ensure_keys(d=mur, none_keys=["verdict"], list_keys=["issues"])

        if self.advanced.json == True:
            return {
                "MU: JSON": mur
            }

        # Some columns get special handling in terms of formatting
        issues_column = None
        if 0 < len(mur["issues"]):
            issues = []
            for i in mur["issues"]:
                issues.append(
                    i.get("severity", "MISSING_SEVERITY") + " " +
                    i.get("issueType", "MISSING_TYPE") + " " +
                    i.get("message", "MISSING_MESSAGE")
                )
            issues_column = "\n".join(issues)

        return {
            "MU: Issues": issues_column,
            "MU: Verdict": mur["verdict"]
        }
    

    def get_accelerated_mobile_pages_columns(self, api_response):
        ampr = api_response.get("inspectionResult", {}).get("ampResult", {})
        ampr = self.ensure_keys(d=ampr, none_keys=["ampIndexStatusVerdict", "ampUrl",
                                "indexingState", "lastCrawlTime", "pageFetchState",
                                "robotsTxtState", "verdict"], list_keys=["issues"])

        if self.advanced.json == True:
            return {
                "AMP: JSON": ampr
            }

        # Some columns get special handling in terms of formatting
        issues_column = None
        if 0 < len(ampr["issues"]):
            issues = []
            for i in ampr["issues"]:
                issues.append(
                    i.get("severity", "MISSING_SEVERITY") + " " +
                    i.get("issueMessage", "MISSING_MESSAGE"))
            issues_column = "\n".join(issues)

        return {
            "AMP: Index Status Verdict": ampr["ampIndexStatusVerdict"],
            "AMP: URL": ampr["ampUrl"],
            "AMP: Indexing State": ampr["indexingState"],
            "AMP: Issues": issues_column,
            "AMP: Last Crawl Time": ampr["lastCrawlTime"],
            "AMP: Page Fetch State": ampr["pageFetchState"],
            "AMP: robots.txt State": ampr["robotsTxtState"],
            "AMP: Verdict": ampr["verdict"]
        }
    

    def get_rich_results_columns(self, api_response):
        rrr = api_response.get("inspectionResult", {}).get("richResultsResult", {})
        rrr = self.ensure_keys(d=rrr, none_keys=["verdict"], list_keys=["detectedItems"])

        return {
            "RR: JSON": rrr
        }

    def inspect_one(self, url, property, credentials):
        service = build(
            serviceName="searchconsole",
            version="v1",
            credentials=credentials
        )

        api_response = service.urlInspection().index().inspect(
                body={
                    "siteUrl": property,
                    "inspectionUrl": url
                }
            ).execute()
        
        service.close()
        
        return api_response


    def execute(self, exec_context, auth_port_object, inspection_url_port_object):
        if self.property_inspection_url_column.property is None:
            raise ValueError("No value for 'Property' parameter selected!")

        inspection_url_column = self.property_inspection_url_column.inspection_url_column
        if inspection_url_column is None:
            raise ValueError("No value for 'URL Table Column' parameter selected!")
        if inspection_url_column not in inspection_url_port_object.column_names:
            raise ValueError(
                "The column specified in the 'URL Table Column' parameter ('"
                + inspection_url_column
                + "') does not exist in the table connected to the 'URL Table' input port!"
            )
        
        max_workers = 10
        if auth_port_object.get_is_pro() != True:
            max_workers = 1

        credentials = lib.credentials.parse_json(auth_port_object.get_credentials())

        inspection_url_series = inspection_url_port_object.to_pandas()[inspection_url_column]
        if inspection_url_series.isna().any() == True:
            raise ValueError("The selected column of the input table contains missing values!")
        if inspection_url_series.eq("").any() == True:
            raise ValueError("The selected column of the input table contains empty string values!")
        if len(inspection_url_series) > 2000:
            raise ValueError(
                "Your input table contains too many URLs to inspect! Google allows inspecting up to 2,000 URLs per property each day, but your request included "
                + str(len(inspection_url_series))
                + ". Please reduce the number of rows and try again."
            )

        rows = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for api_response in executor.map(
                self.inspect_one,
                inspection_url_series,
                itertools.repeat(self.property_inspection_url_column.property),
                itertools.repeat(credentials),
            ):
                if exec_context.is_canceled() == True:
                    executor.shutdown(wait=True, cancel_futures=True)
                    raise RuntimeError("Execution was canceled.")
                
                rows.append(
                    self.build_row(
                        url=inspection_url_series[len(rows)], api_response=api_response
                    )
                )

                exec_context.set_progress(
                    len(rows) / len(inspection_url_series),
                    str(len(rows)) + " of " + str(len(inspection_url_series)) + " URLs processed",
                )

        return knext.Table.from_pandas(
            data=pandas.DataFrame(data=rows),
            row_ids="auto"
        )


@knext.parameter_group(label="Filter")
class FilterParameterGroup:
    class TypeFilterOptions(knext.EnumParameterOptions):
        all = ("Any", "No filtering by property type.")
        urlprefix = ("URL-Prefix Only", "Show only URL-Prefix properties.")
        domain = ("Domain Only", "Show only Domain properties.")

    type_filter = knext.EnumParameter(
        label="Property Type",
        description="Limit results by property type.",
        default_value=TypeFilterOptions.all.name,
        enum=TypeFilterOptions,
        style=knext.EnumParameter.Style.DROPDOWN,
    )


    class VerificationFilterOptions(knext.EnumParameterOptions):
        all = ("Any", "No filtering by verification status.")
        verified = ("Verified Only", "Show only verified properties.")
        unverified = ("Unverified Only", "Show only unverified properties.")

    verification_filter = knext.EnumParameter(
        label="Verification Status",
        description="Limit results by verification status.",
        default_value=VerificationFilterOptions.all.name,
        enum=VerificationFilterOptions,
        style=knext.EnumParameter.Style.DROPDOWN,
    )


@knext.node(name="Search Analytics - Property Details", node_type=knext.NodeType.SOURCE, icon_path="icons/url-inspection.png", category=category, keywords=KNIME_NODE_KEYWORDS)
@knext.input_port(name="Search Analytics Auth Port", description="Receives authentication credentials from a *Search Analytics - Authenticator* node.", port_type=search_auth_port_type)
@knext.output_table(name="Result Table", description="Output table with your Google Search Console properties.")
class PropertyDetails:
    """Fetches a list of your Google Search Console properties.
    Fetches a list of your **Google Search Console properties** and lets you filter them by property type and verification status. Use it to quickly spot which sites are verified and which still need attention.

    **This is a helper node — optional and not required by the other nodes.**

    ⚠️ This node requires a connected and executed **Search Analytics - Authenticator** node.
    """


    filters = FilterParameterGroup()

    output_df_schema = {
        "Site URL": "string",
        "Property Type": "string",
        "Permission Level": "string",
        "Verified": "boolean",
    }


    def configure(self, config_context, auth_port_spec):
        pass


    def execute(self, exec_context, auth_port_object):
        credentials = lib.credentials.parse_json(auth_port_object.get_credentials())

        service = build(
            serviceName="searchconsole", version="v1", credentials=credentials
        )

        sites_list_result = service.sites().list().execute()

        service.close()

        rows = pandas.DataFrame(
            {
                col: pandas.Series(dtype=dtype)
                for col, dtype in self.output_df_schema.items()
            }
        )

        if "siteEntry" not in sites_list_result:
            exec_context.set_warning(
                "The selected Google account does not have any Search Console properties."
            )
        else:
            for e in sites_list_result["siteEntry"]:
                site_url = e["siteUrl"]
                permission_level = e["permissionLevel"]
                is_domain_property = site_url.startswith("sc-domain:")
                is_verified = "unverified".casefold() not in permission_level.casefold()

                if (
                    self.filters.type_filter
                    == self.filters.TypeFilterOptions.urlprefix.name
                    and is_domain_property
                ) or (
                    self.filters.type_filter
                    == self.filters.TypeFilterOptions.domain.name
                    and not is_domain_property
                ):
                    continue

                if (
                    self.filters.verification_filter
                    == self.filters.VerificationFilterOptions.verified.name
                    and not is_verified
                ) or (
                    self.filters.verification_filter
                    == self.filters.VerificationFilterOptions.unverified.name
                    and is_verified
                ):
                    continue

                rows.loc[len(rows)] = {
                    "Site URL": site_url,
                    "Property Type": "Domain" if is_domain_property else "URL-Prefix",
                    "Permission Level": permission_level,
                    "Verified": is_verified,
                }

        return knext.Table.from_pandas(
            data=rows.astype(self.output_df_schema), row_ids="auto"
        )
