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
        return pickle.dumps((self._credentials, self._is_pro))


    @classmethod
    def deserialize(cls, spec: SearchAuthPortSpec, storage: bytes) -> "SearchAuthPortObject":
        credentials, is_pro = pickle.loads(storage)
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
@knext.output_port(name="Search Analytics Auth Port", description="", port_type=search_auth_port_type)
class SearchAuthenticator:
    """This node allows you to authenticate yourself with Google.
    Authenticate yourself with Google by executing this node. A browser window will open to guide you through the process.
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
        description="Upgrade with a license key and supercharge your workflow:\n\n🔥 **Query node**: Remove the 100,000 row cap and access the complete dataset\n\n🔥 **URL Inspection node**: Up to 10x faster execution thanks to parallel processing",
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

        if "siteEntry" not in sites_list_result:
            raise RuntimeError("The selected Google account does not have any Search Console properties.")

        available_props = []
        for e in sites_list_result["siteEntry"]:
            if "unverified" in e["permissionLevel"].lower():
                continue
            available_props.append(e["siteUrl"])
        
        if len(available_props) == 0:
            raise RuntimeError("The selected Google Account does not have any verified Search Console properties. Verify your property and then execute again.")

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
        web = ("Web", "description")
        discover = ("Discover", "description")
        googleNews = ("GoogleNews", "description")
        news = ("News", "description")
        image = ("Image", "description")
        video = ("Video", "description")
    
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
        auto = ("Auto", "description")
        byPage = ("Page", "description")
        byProperty = ("Property", "description")
        byNewsShowcasePanel = ("NewsShowcasePanel", "description")
    
    aggregation = knext.EnumParameter(
        label="Aggregation Type",
        description="",
        default_value=AggregationOptions.auto.name,
        enum=AggregationOptions,
        style=knext.EnumParameter.Style.DROPDOWN
    )


    row_limit = knext.IntParameter(
        label="Row Limit",
        description="Set the maximum number of rows to return. **Use 0 to remove the limit** and fetch all available data.",
        default_value=0,
        min_value=0,
        max_value=100000000000000
    )


@knext.node(name="Search Analytics - Query", node_type=knext.NodeType.SOURCE, icon_path="icons/query.png", category=category, keywords=KNIME_NODE_KEYWORDS)
@knext.input_port(name="Search Analytics Auth Port", description="", port_type=search_auth_port_type)
@knext.output_table(name="Result Table", description="")
class SearchQuery:
    """Retrieve detailed search performance data from the Google Search Console API.
    This node fetches data from the Google Search Console API. It returns information like search impressions, clicks, position, query string, and more. Before use, an Authenticator node must be connected and executed.
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
        if self.property_type.property == None:
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

            if user_row_limit != 0:
                exec_context.set_progress(start_row / user_row_limit)

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
@knext.input_port(name="Search Analytics Auth Port", description="", port_type=search_auth_port_type)
@knext.input_table(name="URL Table", description="")
@knext.output_table(name="Result Table", description="")
class UrlInspection:
    """Retrieve detailed indexing information and issues from the Google Search Console URL Inspection API.
    This node fetches data from the URL Inspection API, which is part of the Google Search Console. It returns information on the Index Status, Mobile Usability, Accelerated Mobile Pages, and Rich Results. Before use, an Authenticator node must be connected and executed.
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
    def ensure_keys(self, dict, none_keys=[], list_keys=[]):
        for k in none_keys:
            dict[k] = dict.get(k, None)

        for k in list_keys:
            dict[k] = dict.get(k, [])

        return dict
    

    def get_index_status_columns(self, api_response):
        isr = api_response.get("inspectionResult", {}).get("indexStatusResult", {})
        isr = self.ensure_keys(dict=isr, none_keys=["coverageState", "crawledAs", "googleCanonical",
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
        mur = self.ensure_keys(dict=mur, none_keys=["verdict"], list_keys=["issues"])

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
        ampr = self.ensure_keys(dict=ampr, none_keys=["ampIndexStatusVerdict", "ampUrl",
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
        rrr = self.ensure_keys(dict=rrr, none_keys=["verdict"], list_keys=["detectedItems"])

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
        if self.property_inspection_url_column.property == None:
            raise ValueError("No value for 'Property' parameter selected!")

        inspection_url_column = self.property_inspection_url_column.inspection_url_column
        if inspection_url_column == None:
            raise ValueError("No value for 'Inspection URL Column' parameter selected!")
        
        max_workers = 10
        if auth_port_object.get_is_pro() != True:
            max_workers = 1

        credentials = lib.credentials.parse_json(auth_port_object.get_credentials())

        inspection_url_df = inspection_url_port_object[inspection_url_column].to_pandas()
        inspection_url_series = inspection_url_df[inspection_url_column]
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

                exec_context.set_progress(len(rows) / len(inspection_url_series))

        return knext.Table.from_pandas(
            data=pandas.DataFrame(data=rows),
            row_ids="auto"
        )
