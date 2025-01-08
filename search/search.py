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
import socket
import time
from datetime import datetime, timedelta, timezone
from random import randint

import knime.extension as knext
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials

import lib.api_request_delay
import lib.credentials
import lib.property_parameter


OAUTH_CLIENT_CONFIG = {
    "installed": {
        "client_id": "",
        "project_id": "",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_secret": ""
    }
}
GOOGLE_API_SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]

KNIME_NODE_KEYWORDS = [
    "Google Search Console",
    "GSC"
]


logger = logging.getLogger(__name__)


class SearchAuthPortSpec(knext.PortObjectSpec):
    def __init__(self, spec: str = "") -> None:
        super().__init__()
        self._spec = spec


    def serialize(self) -> dict:
        return {
            "spec": self._spec
        }


    @classmethod
    def deserialize(cls, data: dict) -> "SearchAuthPortSpec":
        cls(data["spec"])


    def get_spec(self):
        return self._spec


class SearchAuthPortObject(knext.PortObject):
    def __init__(self, spec: SearchAuthPortSpec, credentials) -> None:
        super().__init__(spec)
        self._credentials = credentials


    def serialize(self) -> bytes:
        return pickle.dumps(self._credentials)


    @classmethod
    def deserialize(cls, spec: SearchAuthPortSpec, storage: bytes) -> "SearchAuthPortSpec":
        return cls(spec, pickle.loads(storage))


    def get_credentials(self):
        return self._credentials


search_auth_port_type = knext.port_type(
    name="Search Authentication Port",
    spec_class=SearchAuthPortSpec,
    object_class=SearchAuthPortObject
)


@knext.node(name="Search Analytics - Authenticator", node_type=knext.NodeType.OTHER, icon_path="authenticator.png", category="/", keywords=KNIME_NODE_KEYWORDS)
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


    def configure(self, config_context):
        return SearchAuthPortSpec()
    

    def get_free_port(self):
        for i in range(0, 100):
            port = randint(10000, 30000)
            if self.is_port_free(port=port) == True:
                return port
        
        raise RuntimeError("Can not find free port on loopback interface.")


    def is_port_free(self, port):
        with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return True
            except OSError:
                return False


    def set_available_props(self, exec_context, credentials):
        service = build(
            serviceName="searchconsole",
            version="v1",
            credentials=credentials
        )
        
        site_entries = service.sites().list().execute()["siteEntry"]

        service.close()

        available_props = []
        for e in site_entries:
            if "unverified" in e["permissionLevel"].lower():
                continue
            available_props.append(e["siteUrl"])

        exec_context.flow_variables["available_props"] = available_props


    def execute(self, exec_context):
        flow = InstalledAppFlow.from_client_config(
            client_config=OAUTH_CLIENT_CONFIG,
            scopes=GOOGLE_API_SCOPES
        )

        credentials = flow.run_local_server(
            host="127.0.0.1",
            port=self.get_free_port(),
            authorization_prompt_message=None,
            success_message="Authorized successfully.\n\nRevoke access of this application to your Google Account anytime at https://myaccount.google.com/connections\n\nHeads up! Your authentication details are saved in your workflow. If you share a workflow with an executed Authenticator node, anyone who has access to the workflow can use it to run queries on your Google Search Console properties.\n\nYou can close this window now.",
            open_browser=True,
            timeout_seconds=None
        )

        self.set_available_props(exec_context=exec_context, credentials=credentials)

        strip_keys = []
        if self.expiration != self.ExpirationOptions.never.name:
            strip_keys.append("refresh_token")

        return SearchAuthPortObject(
            spec=SearchAuthPortSpec(),
            credentials=credentials.to_json(strip=strip_keys)
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
        description="Specify how many rows should be returned at most.",
        default_value=100000,
        min_value=1,
        max_value=100000
    )


@knext.node(name="Search Analytics - Query", node_type=knext.NodeType.SOURCE, icon_path="query.png", category="/", keywords=KNIME_NODE_KEYWORDS)
@knext.input_port(name="Search Analytics Auth Port", description="", port_type=search_auth_port_type)
@knext.output_table(name="Result Table", description="")
class SearchQuery:
    """This node queries data from Google Search Console
    Query data from Google Search Console. This node requires an Authenticator node to be connected and executed before use.
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
        rows = []

        i = 0
        while True:
            start_row = i * api_row_limit

            exec_context.set_progress(start_row / self.advanced.row_limit)

            api_response = service.searchanalytics().query(
                siteUrl=self.property_type.property,
                body=self.get_request_body(row_limit=api_row_limit, start_row=start_row)
            ).execute()

            new_rows = self.parse_response(api_response)
            rows += new_rows

            if self.advanced.row_limit != 0 and self.advanced.row_limit <= len(rows):
                rows = rows[:self.advanced.row_limit]
                break

            if len(new_rows) < api_row_limit:
                break

            i += 1
            time.sleep(lib.api_request_delay.get(i))

        service.close()

        return knext.Table.from_pandas(
            data=pandas.DataFrame(data=rows),
            row_ids="auto"
        )


@knext.parameter_group(label="Property and URL Column")
class UrlInspectionPropertyInspectionUrlColumnParameterGroup:
    property = lib.property_parameter.create()
    inspection_url_column = knext.ColumnParameter(
        label="Inspection URL Column",
        description="",
        port_index=1
    )


@knext.parameter_group(label="Modules")
class UrlInspectionModulesParameterGroup:
    index_status = knext.BoolParameter(label="Index Status", description="", default_value=True)
    mobile_usability = knext.BoolParameter(label="Mobile Usability", description="", default_value=False)
    accelerated_mobile_pages = knext.BoolParameter(label="Accelerated Mobile Pages", description="", default_value=False)
    rich_results = knext.BoolParameter(label="Rich Results", description="", default_value=False)


@knext.parameter_group(label="Advanced", is_advanced=True)
class UrlInspectionAdvancedParameterGroup:
    add_web_link = knext.BoolParameter(label="Add Link to Google Search Console Website", description="", default_value=False)
    json = knext.BoolParameter(label="Output Results as JSON", description="", default_value=False)


@knext.node(name="Search Analytics - URL Inspection", node_type=knext.NodeType.SOURCE, icon_path="query.png", category="/", keywords=KNIME_NODE_KEYWORDS)
@knext.input_port(name="Search Analytics Auth Port", description="", port_type=search_auth_port_type)
@knext.input_table(name="Inspection URL", description="")
@knext.output_table(name="Result Table", description="")
class UrlInspection:
    """TODO short
    TODO long
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
    

    def get_index_status_columns(self, api_response):
        isr = api_response.get("inspectionResult", {}).get("indexStatusResult", {})
        isr["referringUrls"] = isr.get("referringUrls", [])
        isr["sitemap"] = isr.get("sitemap", [])

        if self.advanced.json == True:
            return {
                "IS: JSON": isr
            }

        return {
            "IS: Coverage State": isr.get("coverageState", ""),
            "IS: Crawled As": isr.get("crawledAs", ""),
            "IS: Google Canonical": isr.get("googleCanonical", ""),
            "IS: Indexing State": isr.get("indexingState", ""),
            "IS: Last Crawl Time": isr.get("lastCrawlTime", ""),
            "IS: Page Fetch State": isr.get("pageFetchState", ""),
            "IS: Referring URLs": "\n".join(isr["referringUrls"]),
            "IS: robots.txt State": isr.get("robotsTxtState", ""),
            "IS: Sitemaps": "\n".join(isr["sitemap"]),
            "IS: User Canonical": isr.get("userCanonical", ""),
            "IS: Verdict": isr.get("verdict", "")
        }
    

    def get_mobile_usability_columns(self, api_response):
        mur = api_response.get("inspectionResult", {}).get("mobileUsabilityResult", {})

        if self.advanced.json == True:
            return {
                "MU: JSON": mur
            }

        issues = []
        for i in mur.get("issues", []):
            issues.append(
                i.get("severity", "") + " " + i.get("issueType", "") + " " + i.get("message", ""))

        return {
            "MU: Issues": "\n".join(issues),
            "MU: Verdict": mur.get("verdict", "")
        }
    

    def get_accelerated_mobile_pages_columns(self, api_response):
        ampr = api_response.get("inspectionResult", {}).get("ampResult", {})

        if self.advanced.json == True:
            return {
                "AMP: JSON": ampr
            }

        issues = []
        for i in ampr.get("issues", []):
            issues.append(
                i.get("severity", "") + " " + i.get("issueMessage", ""))

        return {
            "AMP: Index Status Verdict": ampr.get("ampIndexStatusVerdict", ""),
            "AMP: URL": ampr.get("ampUrl", ""),
            "AMP: Indexing State": ampr.get("indexingState", ""),
            "AMP: Issues": "\n".join(issues),
            "AMP: Last Crawl Time": ampr.get("lastCrawlTime", ""),
            "AMP: Page Fetch State": ampr.get("pageFetchState", ""),
            "AMP: robots.txt State": ampr.get("robotsTxtState", ""),
            "AMP: Verdict": ampr.get("verdict", "")
        }
    

    def get_rich_results_columns(self, api_response):
        return {
            "RR: JSON": api_response.get("inspectionResult", {}).get("richResultsResult", {})
        }


    def execute(self, exec_context, auth_port_object, inspection_url_port_object):
        if self.property_inspection_url_column.property == None:
            raise ValueError("No value for 'Property' parameter selected!")

        inspection_url_column = self.property_inspection_url_column.inspection_url_column
        if inspection_url_column == None:
            raise ValueError("No value for 'Inspection URL Column' parameter selected!")
        
        service = build(
            serviceName="searchconsole",
            version="v1",
            credentials=lib.credentials.parse_json(auth_port_object.get_credentials())
        )

        inspection_url_df = inspection_url_port_object[inspection_url_column].to_pandas()
        inspection_url_series = inspection_url_df[inspection_url_column]
        rows = []

        i = 0
        for url in inspection_url_series:
            exec_context.set_progress(i / len(inspection_url_series))

            api_response = service.urlInspection().index().inspect(
                body={
                    "siteUrl": self.property_inspection_url_column.property,
                    "inspectionUrl": url
                }
            ).execute()

            rows.append(self.build_row(url=url, api_response=api_response))

            i += 1
            time.sleep(lib.api_request_delay.get(i))

        service.close()

        return knext.Table.from_pandas(
            data=pandas.DataFrame(data=rows),
            row_ids="auto"
        )
