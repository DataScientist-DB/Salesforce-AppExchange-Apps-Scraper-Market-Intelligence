🚀 Salesforce AppExchange Apps Scraper & Market Intelligence 

Extract Salesforce AppExchange apps, categories, ratings, and ecosystem insights.
Discover, Analyze, and Export Salesforce AppExchange Apps
The Salesforce AppExchange Intelligence Engine is an advanced Apify Actor that automatically discovers Salesforce AppExchange applications and extracts structured metadata for market research, competitive intelligence, and ecosystem analysis.

The Actor explores AppExchange across three discovery groups:
•	Business Needs
•	Industries
•	Products
It collects app metadata and generates analysis-ready datasets and market intelligence reports.
 
👥 Who This Actor Is For
This Actor is useful for:
• Salesforce consulting firms
• SaaS product managers
• Competitive intelligence teams
• Market researchers
• Salesforce partners
• Data teams building ecosystem intelligence pipelines
• Agencies preparing AppExchange landscape reports
 
🔎 What This Actor Does

The Actor automatically:

• Scrapes Salesforce AppExchange apps
• Extracts ratings, reviews, pricing signals, and descriptions
• Builds a structured dataset of apps
• Performs automated market intelligence analysis
• Generates competitive insights and opportunity signals
• Produces executive-ready reports

All outputs are available as structured data files ready for analysis.
Data Extracted
For each Salesforce AppExchange app the Actor collects:
•	App name
•	App URL
•	Category group
•	Market segment
•	Rating
•	Number of reviews
•	Pricing model (free / freemium / paid / nonprofit discount)
•	Price text
•	Short description
•	Market classification signals

Market Intelligence Outputs
The Actor automatically generates strategic analysis:
Market Overview
•	total apps analyzed
•	average rating
•	review distribution
Competitive Landscape
•	mature leaders
•	under-discovered challengers
•	vulnerable incumbents
•	mid-market apps
Pricing Landscape
•	pricing model distribution
•	freemium adoption patterns
Opportunity Signals
•	high-rated low-visibility apps
•	underserved segments
•	competitive gaps
Strategic Insights
•	market concentration analysis
•	category segmentation
•	adoption trends


Outputs include:
•	📦 Apify dataset /APPS dataset
•	📄 CSV export /APPS.csv
•	📊 XLSX export  /APPS.xlsx
•	🧠 Market intelligence reports JSON
            -MARKET_INTELLIGENCE.json
            -EXECUTIVE_SUMMARY.txt
            -LLM_MARKET_SUMMARY.json
            -MARKET_REPORT.pdf
•	📝 Executive summary
•	📑 PDF market report
 
These files are used for:
•	competitive analysis
•	market research
•	Salesforce ecosystem mapping
•	investment research
•	lead generation
•	product strategy

Typical Use Cases
•	Competitive Intelligence
•	Analyze competitors in specific Salesforce categories.
•	Market Research
•	Understand the structure of the AppExchange ecosystem.
•	Startup Research
•	Identify gaps and opportunities for new Salesforce apps.
•	Investment Analysis
•	Evaluate categories with strong adoption signals.
•	Consulting
•	Produce executive market reports for clients.

🧭 Discovery Modes
The Actor supports three AppExchange discovery groups controlled by the categoryGroup input.
 
1️⃣ Business Needs Mode
Explores:
https://appexchange.salesforce.com/explore/business-needs
Example business need categories:
sales
marketing
finance
analytics
customer-service
it-and-admin
productivity
commerce
human-resources
erp
integration

Example input:
{
  "mode": "apps",
  "categoryGroup": "business-needs",
  "sphere": "business-needs",
  "categoryPreset": ["sales"],
  "appGroup": ["sales"],
  "maxPages": 2,
  "headless": true
}
 
2️⃣ Industries Mode
Explores:
https://appexchange.salesforce.com/explore/industries
Example industry filters:
automotive
manufacturing
healthcare
financial-services
public-sector
retail
education
energy

Example input:
{
  "mode": "apps",
  "categoryGroup": "industries",
  "sphere": "industries",
  "categoryPreset": ["automotive"],
  "appGroup": ["automotive"],
  "maxPages": 2,
  "headless": true
}
 
3️⃣ Products Mode

Explores:
https://appexchange.salesforce.com/explore/products
Products mode works differently:
the Actor clicks the sidebar filter rather than relying on URL parameters.
Examples:
data cloud
b2b-commerce
b2c-commerce
sales cloud
service cloud
marketing cloud
experience cloud
Example input:
{
  "mode": "apps",
  "categoryGroup": "products",
  "sphere": "products",
  "categoryPreset": ["b2b-commerce"],
  "appGroup": ["b2b-commerce"],
  "maxPages": 2,
  "headless": true
}
 
⚙️ Input Parameters
mode
Type: string
Available values:
apps
reviews
apps+reviews
Default:
apps
 
categoryGroup
Type: string
Controls the AppExchange section.
Allowed values:
business-needs
industries
products
 
sphere
Type: string
Optional label stored in dataset output.
Examples:
business-needs
industries
products
 
categoryPreset
Type: array[string]
Label used to group the run.
Examples:
["sales"]
["automotive"]
["b2b-commerce"]
 
appGroup
Type: array[string]
Defines the actual filters to crawl.
Examples:
["sales"]
["marketing","finance"]
["data cloud","b2b-commerce"]
 
maxPages
Type: integer
Defines how many scroll passes are performed per category.
Recommended values:
1–2   (fast runs)
3–5   (balanced runs)
10+   (large ecosystem scans)
 
pricingFilter
Type: string
Optional filter by inferred pricing model.
Allowed values:
"" (no filter)
free
freemium
paid
unknown
 
minRating
Type: number
Filter apps below a rating threshold.
Example:
4.2
Default:
0
 
headless
Type: boolean
Controls Playwright browser mode.
true  → recommended
false → debugging
 
proxySettings
Type: object
Apify proxy configuration.
Recommended for stability on large runs.
 
analysisOptions
Controls generation of market intelligence outputs:
MARKET_INTELLIGENCE.json
EXECUTIVE_SUMMARY.txt
MARKET_REPORT.pdf
 
Example Input
{
  "mode": "apps",
  "categoryGroup": "business-needs",
  "categoryPreset": ["marketing", "sales"],
  "maxPages": 3,
  "analysisOptions": {
    "enableExecutiveSummary": true,
    "outputJsonReport": true,
    "outputPdfReport": true
  }
}

📦 Dataset Output
Each dataset record includes:
Field	Description
listing_id	Unique AppExchange listing identifier
sphere	Discovery group
category_preset	Run label
primary_category_name	Primary category name
app_name	Application name
name	Compatibility alias
short_description	Short app description
rating	Average rating
reviews_count	Number of reviews
price_text	Raw pricing text
clouds	Supported Salesforce clouds
app_url	App listing URL
last_seen_at	Timestamp of last observation
 


📁 Files Generated
The Actor writes the following files to the Key-Value Store:
APPS.csv
APPS.xlsx
MARKET_INTELLIGENCE.json
EXECUTIVE_SUMMARY.txt
MARKET_REPORT.pdf
These files are designed for consulting reports, competitive intelligence, and BI dashboards.
 
🧠 Market Intelligence Outputs
The Actor can automatically generate structured analysis including:
• Competitive landscape overview
• App distribution by category
• Pricing distribution
• Market signals
• Executive summary insights
• SWOT-style analysis
 
📍 How to Use on Apify
1️⃣ Open the Actor in Apify Console
2️⃣ Click Run
3️⃣ Provide input JSON
4️⃣ Download outputs:
•	Dataset
•	CSV
•	XLSX
•	Market intelligence reports
 
🔐 Compliance
This Actor:
✔ Collects publicly available information only
✔ Does not require login
✔ Does not access private or gated data
Users are responsible for compliance with Salesforce terms and local regulations.
 
⭐ Why This Actor Is Unique
Unlike simple scrapers, this engine:
✔ Supports three AppExchange exploration modes
✔ Generates consulting-grade market reports
✔ Produces analysis-ready datasets
✔ Handles dynamic loading and sidebar filtering
✔ Provides structured outputs for BI pipelines
 
📌 Roadmap
Future improvements may include:
• AppExchange review extraction mode
• Vendor-level aggregation
• Category performance scoring
• Historical ecosystem monitoring
• AI-generated ecosystem insights
 
💰 Pricing (Suggested)
Event definition:
1 event = 1 AppExchange app processed
Suggested pricing:
Typical run costs

200 apps → ~$0.02
1,000 apps → ~$0.10
5,000 apps → ~$0.50
Market intelligence report → ~$0.03
 
🚀 Start Small, Then Scale
Use small runs first to validate results:
maxPages = 1
Then scale to larger ecosystem analysis runs.

🚀 Salesforce AppExchange Apps Scraper & Market Intelligence 
Discover, Analyze & Export Salesforce AppExchange Ecosystem Data
The Salesforce AppExchange Intelligence Engine is a powerful automation tool that discovers Salesforce AppExchange applications and extracts structured metadata for market research, competitive intelligence, and ecosystem analysis.
It explores the marketplace across three discovery groups:
•	Business Needs
•	Industries
•	Products
The Actor automatically collects app information and generates structured datasets and market intelligence reports ready for analytics.
 
📊 What This Actor Produces
After every run the Actor generates:
Output	Description
Dataset	Structured app metadata
APPS.csv	Spreadsheet export
APPS.xlsx	Excel export
MARKET_INTELLIGENCE.json	Machine-readable market analysis
EXECUTIVE_SUMMARY.txt	Human-readable summary
MARKET_REPORT.pdf	Visual market overview
These outputs are ideal for:
•	consulting reports
•	competitive analysis
•	ecosystem research
•	BI dashboards
 
🧭 How Discovery Works
The Actor follows a 4-step discovery pipeline:
User Input
   │
   ▼
AppExchange Explore Page
   │
   ▼
Dynamic Listing Discovery
   │
   ▼
App Detail Extraction
   │
   ▼
Dataset + Market Reports
 
🧩 Discovery Modes
The Actor supports three exploration sections.
1️⃣ Business Needs
Explore:
https://appexchange.salesforce.com/explore/business-needs
Example categories:
sales
marketing
finance
analytics
customer-service
it-and-admin
productivity
commerce
human-resources
erp
integration
Example input:
{
  "mode": "apps",
  "categoryGroup": "business-needs",
  "sphere": "business-needs",
  "categoryPreset": ["sales"],
  "appGroup": ["sales"],
  "maxPages": 2,
  "headless": true
}
 
2️⃣ Industries
Explore:
https://appexchange.salesforce.com/explore/industries
Example filters:
automotive
manufacturing
healthcare
financial-services
retail
education
energy
public-sector
Example input:
{
  "mode": "apps",
  "categoryGroup": "industries",
  "sphere": "industries",
  "categoryPreset": ["automotive"],
  "appGroup": ["automotive"],
  "maxPages": 2,
  "headless": true
}
 
3️⃣ Products
Explore:
https://appexchange.salesforce.com/explore/products
Products use sidebar filtering rather than URL parameters.
Examples:
data cloud
b2b-commerce
b2c-commerce
sales cloud
service cloud
marketing cloud
experience cloud
Example input:
{
  "mode": "apps",
  "categoryGroup": "products",
  "sphere": "products",
  "categoryPreset": ["b2b-commerce"],
  "appGroup": ["b2b-commerce"],
  "maxPages": 2,
  "headless": true
}
 
⚙️ Input Parameters
mode
Type: string
apps
reviews
apps+reviews
Default:
apps
 
categoryGroup
Controls which AppExchange explore section is used.
business-needs
industries
products
 
sphere
Optional label stored in dataset output.
Example:
business-needs
industries
products
 
categoryPreset
Array label used to group the run.
Examples:
["sales"]
["automotive"]
["b2b-commerce"]
 
appGroup
Defines the filters to crawl.
Examples:
["sales"]
["marketing","finance"]
["data cloud","b2b-commerce"]
 
maxPages
Number of scroll passes performed during listing discovery.
Recommended:
1–2   quick runs
3–5   normal runs
10+   large ecosystem scans
 
pricingFilter
Optional pricing model filter.
free
freemium
paid
unknown
 
minRating
Minimum rating threshold.
Example:
4.2
 
headless
Browser mode:
true  (recommended)
false (debugging)
 
proxySettings
Apify proxy configuration.
Recommended for large runs.
 
analysisOptions
Controls generation of market intelligence outputs:
MARKET_INTELLIGENCE.json
EXECUTIVE_SUMMARY.txt
MARKET_REPORT.pdf
 
📦 Dataset Schema
Each dataset record contains:
Field	Description
listing_id	Unique listing identifier
sphere	Discovery group
category_preset	Run label
primary_category_name	Category name
app_name	Application name
name	Compatibility alias
short_description	Short description
rating	Average rating
reviews_count	Review count
price_text	Pricing information
clouds	Supported Salesforce clouds
app_url	Listing URL
last_seen_at	Timestamp
 
📈 Market Intelligence Report
The Actor automatically generates analytical signals including:
•	Top apps by rating
•	Category distribution
•	Pricing distribution
•	Ecosystem signals
•	Competitive landscape overview
These insights are exported to:
MARKET_INTELLIGENCE.json
EXECUTIVE_SUMMARY.txt
MARKET_REPORT.pdf
 
▶️ Running Locally
Run locally with the Apify CLI:
apify run
Push updates to the platform:
apify push
 
🧪 Recommended Test Input
Use a small run first:
{
  "mode": "apps",
  "categoryGroup": "business-needs",
  "sphere": "business-needs",
  "categoryPreset": ["sales"],
  "appGroup": ["sales"],
  "maxPages": 1,
  "headless": true
}
 
🧠 Use Cases
Typical applications include:
•	Salesforce ecosystem analysis
•	competitor benchmarking
•	SaaS market research
•	AppExchange landscape mapping
•	consulting ecosystem reports
•	product category analysis
 
🔐 Compliance
This Actor:
✔ collects publicly available information only
✔ does not require login
✔ does not access private data
Users must ensure compliance with Salesforce terms and local regulations.
 
⭐ Why This Actor Is Unique
Compared with typical web scrapers, this engine:
✔ supports three AppExchange discovery modes
✔ generates market intelligence reports automatically
✔ exports structured datasets for analytics pipelines
✔ handles dynamic page loading and filter interaction
 
📌 Roadmap
Planned enhancements:
•	review extraction improvements
•	vendor aggregation analytics
•	ecosystem trend monitoring
•	AI-generated insights
•	category performance scoring
 
💰 Pricing Model (Recommended)
Event definition:
1 event = 1 AppExchange app processed
Example pricing tiers:
Plan	Price
Starter	$3 / 1,000 apps
Pro	$2 / 1,000 apps
Scale	$1.50 / 1,000 apps
 
🚀 Start Small, Then Scale
Start with small runs:
maxPages = 1
Then scale to full ecosystem analysis.


