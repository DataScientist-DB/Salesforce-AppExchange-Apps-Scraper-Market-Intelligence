
# 🚀 Salesforce AppExchange Apps Scraper & Market Intelligence
Scrape Salesforce AppExchange apps, reviews, ratings, pricing models, and ecosystem insights for market research and competitive intelligence.

This project powers the Apify Store Actor:

https://apify.com/adinfosys-labs/salesforce-appexchange-apps-scraper-market-intelligence

Extract Salesforce AppExchange apps, categories, ratings, and ecosystem insights.
Discover, Analyze, and Export Salesforce AppExchange Apps

The Salesforce AppExchange Intelligence Engine is an advanced Apify Actor that automatically discovers Salesforce AppExchange applications and extracts structured metadata for market research, competitive intelligence, and ecosystem analysis.
The Actor explores AppExchange across three discovery groups:
• Business Needs
• Industries
• Products
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
 
📊 Data Extracted
For each Salesforce AppExchange app the Actor collects:
• App name
• App URL
• Category group
• Market segment
• Rating
• Number of reviews
• Pricing model (free / freemium / paid / nonprofit discount)
• Price text
• Short description
• Market classification signals
 
📦 Output Files
After each run the Actor produces:
File	Description
APPS.csv	Spreadsheet export
APPS.xlsx	Excel export
MARKET_INTELLIGENCE.json	Machine-readable analysis
EXECUTIVE_SUMMARY.txt	Human-readable insights
LLM_MARKET_SUMMARY.json	AI-ready summary
MARKET_REPORT.pdf	Executive PDF report
These outputs are commonly used for:
• competitive analysis
• market research
• Salesforce ecosystem mapping
• investment research
• lead generation
• product strategy
 
📄 Example Output Record
Example dataset item produced by the Actor:
{
  "listing_id": "a0N30000003IUgVEAW",
  "app_name": "Cirrus Insight",
  "primary_category_name": "Sales Productivity",
  "rating": 4.8,
  "reviews_count": 1200,
  "pricing_model": "paid",
  "price_text": "$22.95 per user/month",
  "short_description": "Email integration and productivity tools for Salesforce",
  "app_url": "https://appexchange.salesforce.com/appxListingDetail?listingId=a0N30000003IUgVEAW",
  "sphere": "business-needs",
  "category_preset": "sales",
  "last_seen_at": "2026-03-05T12:10:00Z"
}
 
📊 Example Output & Interface
Dataset Output (Apify Dataset)
The Actor produces a structured dataset of discovered Salesforce AppExchange apps.
 
 
CSV / Excel Export
All results can be exported as CSV or Excel files.
 
Example exported files:
APPS.csv
APPS.xlsx
 
Market Intelligence Report
The Actor can generate an executive market intelligence report summarizing the ecosystem.
 
Generated files:
MARKET_INTELLIGENCE.json
EXECUTIVE_SUMMARY.txt
MARKET_REPORT.pdf
 
Actor Input Configuration
The Actor can be configured directly in the Apify Console.
 
Example configuration:
{
  "mode": "apps",
  "categoryGroup": "business-needs",
  "sphere": "business-needs",
  "categoryPreset": ["marketing", "sales"],
  "appGroup": ["marketing", "sales"],
  "maxPages": 1,
  "headless": true
}
 
⚡ Quick Start
Example input configuration:
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
 
🧭 Discovery Modes
The Actor supports three AppExchange discovery groups.
1️⃣ Business Needs
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
 
2️⃣ Industries
https://appexchange.salesforce.com/explore/industries
Example filters:
automotive
manufacturing
healthcare
financial-services
public-sector
retail
education
energy
 
3️⃣ Products
https://appexchange.salesforce.com/explore/products
Examples:
data cloud
b2b-commerce
b2c-commerce
sales cloud
service cloud
marketing cloud
experience cloud
 
⚙️ Input Parameters
Parameter	Description
mode	apps / reviews / apps+reviews
categoryGroup	business-needs / industries / products
categoryPreset	Label used for grouping runs
appGroup	Filters applied
maxPages	Number of scroll passes
pricingFilter	Optional pricing filter
minRating	Minimum rating threshold
headless	Browser mode
proxySettings	Apify proxy configuration
 
📈 Market Intelligence Insights
The Actor automatically generates analytics including:
• market overview
• category distribution
• pricing distribution
• competitive landscape
• opportunity signals
• ecosystem trends
These insights are exported to:
• MARKET_INTELLIGENCE.json
• EXECUTIVE_SUMMARY.txt
• MARKET_REPORT.pdf
 
▶️ Running Locally
Run locally:
apify run
Push updates:
apify push
 
🧠 Typical Use Cases
• Salesforce ecosystem analysis
• competitor benchmarking
• SaaS market research
• AppExchange landscape mapping
• consulting ecosystem reports
 
🔐 Compliance
This Actor:
✔ collects publicly available information
✔ does not require login
✔ does not access private data
Users must ensure compliance with Salesforce terms and local regulations.
 
⭐ Why This Actor Is Unique
Compared with typical scrapers, this engine:
✔ supports three AppExchange exploration modes
✔ generates market intelligence reports automatically
✔ produces analysis-ready datasets
✔ handles dynamic page loading and filters
 
📌 Roadmap
Future improvements may include:
• review extraction improvements
• vendor aggregation analytics
• ecosystem trend monitoring
• AI-generated insights
• category performance scoring
 
💰 Example Run Cost
Typical run costs using pay-per-result pricing:
Apps Crawled	Estimated Cost
200	~$0.02
1,000	~$0.10
5,000	~$0.50
20,000	~$2.00
 
🚀 Start Small, Then Scale
Start with:
maxPages = 1
Then increase for full ecosystem analysis.

