<p align="center">
  <img src="icons/query.png" width="80" alt="Search Analytics Node logo">
</p>

<h1 align="center">Search Analytics Node</h1>

<p align="center">
  <b>Google Search Console data in KNIME. No code, no CSV exports, no 1,000-row limit.</b>
</p>

<p align="center">
  <a href="https://hub.knime.com/lorenzhab/extensions/com.searchanalyticsnode.features.Search-Analytics-Node/latest">KNIME Hub</a> ·
  <a href="https://searchanalyticsnode.com">Website</a> ·
  <a href="https://github.com/lorenzhaberzettl/Search-Analytics-Node/releases">Releases</a>
</p>

The Search Console web interface shows and exports at most 1,000 rows per report. Search Analytics Node connects KNIME directly to the official Google Search Console API, so you get the data behind those reports at full scale and can blend it with anything else in your workflows.

- **More data than the UI.** Up to 100,000 rows per query for free, or the complete dataset with [Pro](https://searchanalyticsnode.com/#section-pro).
- **Flexible breakdowns.** Combine date, country, device, page, and query in a single query. Search appearance is available as a separate breakdown.
- **Bulk URL inspection.** Index status, last crawl time, canonicals, and rich results for up to 2,000 URLs per property per day.
- **Repeatable.** Re-run or schedule a workflow instead of exporting reports by hand.
- **Private by design.** View-only access, and your data goes straight from Google to your computer. See [Privacy & Security](#privacy--security).


> [!TIP]
> **New to the extension?** [Download the demo workflow](https://searchanalyticsnode.com/assets/workflows/Search%20Analytics%20Node%20-%20Demo%20Workflow.knwf) and open it in KNIME. It walks you through every node step by step. Want a quick look first? [View a screenshot of the demo workflow](https://searchanalyticsnode.com/assets/workflows/Search%20Analytics%20Node%20-%20Demo%20Workflow.webp).


## Nodes

| Icon | Name | Description |
| :--: | ---- | ----------- |
| <img src="https://searchanalyticsnode.com/assets/img/nodes/authenticator-node.webp" width="25%" alt="Screenshot of the Search Analytics Authenticator Node"> | Authenticator | Handles the authentication with Google's API |
| <img src="https://searchanalyticsnode.com/assets/img/nodes/query-node.webp" width="25%" alt="Screenshot of the Search Analytics Query Node"> | Query | Retrieves search performance data, including impressions, clicks, and more |
| <img src="https://searchanalyticsnode.com/assets/img/nodes/url-inspection-node.webp" width="25%" alt="Screenshot of the Search Analytics URL Inspection Node"> | URL Inspection | Retrieves indexing information and issues, including last crawl time, referring URLs, and more |
| <img src="https://searchanalyticsnode.com/assets/img/nodes/property-details-node.webp" width="25%" alt="Screenshot of the Search Analytics Property Details Node"> | Property Details | Retrieves Google Search Console properties, including property type, verification status, and more |


## Privacy & Security

- **View-only access.** The extension can only read your Search Console data. It can't change anything in your account.
- **Sign in with Google.** You log in on Google's usual sign-in page. We never see your password.
- **Your data stays with you.** Your data goes straight from Google to KNIME on your computer, without passing through any other server.
- **Careful when sharing workflows.** Your login is saved in the workflow. Reset the Authenticator node before you share a workflow, or others can access your data.
- **Remove access anytime** in your [Google account settings](https://myaccount.google.com/connections).
- **Open source.** Anyone can review the code here on GitHub.


## Installation

For more details and instructions on how to install the extension in KNIME, see [https://searchanalyticsnode.com](https://searchanalyticsnode.com).
