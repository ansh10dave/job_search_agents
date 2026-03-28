import requests
import json
import datetime
import re

# 1. The Canada-Heavy Target List
TARGETS = {
    "greenhouse": [
        # AI/ML Companies
        "cohere", "anthropic", "scaleai", "assemblyai", "glean",
        "dataiku", "weights-and-biases", "arizeai",
        # Big Tech Canada presence
        "drweng", "benchsci", "ada", "layer6",
        # Fintech Canada
        "wealthsimple", "koho", "nuvei", "clearco",
        # SaaS / Cloud
        "stripe", "databricks", "reddit", "pinterest",
        "discord", "instacart", "twitch",
        # Canadian startups
        "ecopia", "coveo", "d2l", "borealisai"
    ],
    "lever": [
       # Core tech
        "shopify", "netflix", "figma", "lyft", "yelp",
        "coursera", "atlassian", "canva", "block",
        # Canadian / Canada office
        "hootsuite", "clio", "tulip-retail", "faire",
        "ritual", "properly", "clearbit",
        # Fintech
        "affirm", "brex", "rippling",
        # AI/ML
        "scale", "huggingface", "together"
    ],
    "ashby": [
         "notion", "linear", "loom", "vercel", "fivetran",
        "replit", "descript", "coreweave", "modal",
        "perplexity", "mistral", "cohere"
    ],
    "workday": [
        # Already have these
        {"tenant": "autodesk", "site_id": "autodesk"},
        {"tenant": "wbd", "site_id": "wbd_careers"},
        {"tenant": "mastercard", "site_id": "Corporate_Careers"},
        {"tenant": "nvidia", "site_id": "NVIDIAExternalCareerSite"},
        {"tenant": "cibc", "site_id": "cibc_careers"},
        {"tenant": "electronicarts", "site_id": "EA_ext"},
        # Banks and Fintech — big AI hiring in Toronto
        {"tenant": "rbc", "site_id": "RBCCareers"},
        {"tenant": "td", "site_id": "TD_Bank_Careers"},
        {"tenant": "scotiabank", "site_id": "scotiabank"},
        {"tenant": "bmo", "site_id": "bmo"},
        {"tenant": "manulife", "site_id": "manulife"},
        # Big Tech
        {"tenant": "amazon", "site_id": "Amazon_Jobs"},
        {"tenant": "google", "site_id": "Google"},
        {"tenant": "microsoft", "site_id": "Microsoft"},
        {"tenant": "apple", "site_id": "apple"},
        {"tenant": "salesforce", "site_id": "salesforce"},
        # Canadian Enterprise
        {"tenant": "telus", "site_id": "telus"},
        {"tenant": "rogers", "site_id": "rogers"},
        {"tenant": "bell", "site_id": "bell"},
        {"tenant": "cgi", "site_id": "cgi"},
        {"tenant": "opentext", "site_id": "opentext"},
        # AI/Data
        {"tenant": "palantir", "site_id": "palantir"},
        {"tenant": "databricks", "site_id": "databricks"},
        {"tenant": "snowflake", "site_id": "Snowflake"},
        {"tenant": "servicenow", "site_id": "servicenow"}
    ]
}

# ─────────────────────────────────────────
# TITLE FILTERS
# ─────────────────────────────────────────
TITLE_INCLUDE = [
    "ai engineer", "ml engineer", "machine learning engineer",
    "software engineer", "software developer", "backend engineer",
    "backend developer", "backend software developer", "data scientist", "ai developer",
    "applied scientist", "nlp engineer", "llm engineer",
    "applied ml", "applied ai", "generative ai", "gen ai",
    "mlops engineer", "platform engineer", "data engineer",
    "full stack engineer", "fullstack engineer", "python engineer", "python developer"
]

TITLE_EXCLUDE = [
    "phd", "intern", "internship", "co-op", "coop", "student",
    "director", "vp ", "vice president", "principal engineer",
    "staff engineer", "distinguished", "head of", "senior staff",
    "manager", "lead engineer", "founding engineer", "partner"
]

# ─────────────────────────────────────────
# LOCATION FILTER
# ─────────────────────────────────────────
CANADA_ZONES = [
    "toronto", "ottawa", "ontario", "canada", "waterloo",
    "vancouver", "montreal", "calgary", "remote"  # remote included
]

# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────
def is_target_title(title):
    t = title.lower()
    if any(ex in t for ex in TITLE_EXCLUDE):
        return False
    return any(kw in t for kw in TITLE_INCLUDE)

def is_target_location(loc):
    if not loc:
        return False
    l = loc.lower()
    return any(z in l for z in CANADA_ZONES)

def parse_wd_date(posted_str):
    if not posted_str:
        return "2000-01-01"
    s = posted_str.lower()
    today = datetime.datetime.now()
    if "today" in s:
        return today.strftime('%Y-%m-%d')
    if "yesterday" in s:
        return (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    match = re.search(r'(\d+)', s)
    if match:
        return (today - datetime.timedelta(days=int(match.group(1)))).strftime('%Y-%m-%d')
    return "2000-01-01"

def days_ago(date_str):
    try:
        d = datetime.datetime.strptime(date_str[:10], '%Y-%m-%d')
        return (datetime.datetime.now() - d).days
    except:
        return 999

# ─────────────────────────────────────────
# FETCH FUNCTIONS
# ─────────────────────────────────────────
def fetch_greenhouse(company_id):
    url = f"https://boards-api.greenhouse.io/v1/boards/{company_id}/jobs?content=true"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        return [{
            "company": company_id.capitalize(),
            "title": job.get("title", ""),
            "location": job.get("location", {}).get("name", "Remote"),
            "url": job.get("absolute_url", ""),
            "description": job.get("content", ""),
            "posted_at": job.get("updated_at", "2000-01-01")[:10],
            "source": "greenhouse"
        } for job in res.json().get('jobs', [])]
    except Exception:
        return []

def fetch_lever(company_id):
    url = f"https://api.lever.co/v0/postings/{company_id}?mode=json"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        return [{
            "company": company_id.capitalize(),
            "title": job.get("text", ""),
            "location": job.get("categories", {}).get("location", "Remote"),
            "url": job.get("hostedUrl", ""),
            "description": job.get("descriptionPlain", ""),
            "posted_at": datetime.datetime.fromtimestamp(
                job.get("createdAt", 0) / 1000
            ).strftime('%Y-%m-%d') if job.get("createdAt") else "2000-01-01",
            "source": "lever"
        } for job in res.json()]
    except Exception:
        return []

def fetch_ashby(company_id):
    url = f"https://api.ashbyhq.com/posting-api/job-board/{company_id}?includeCompensation=true"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        return [{
            "company": company_id.capitalize(),
            "title": job.get("title", ""),
            "location": job.get("location", "Remote"),
            "url": job.get("jobUrl", ""),
            "description": job.get("descriptionPlain", ""),
            "posted_at": job.get("publishedAt", job.get("createdAt", "2000-01-01"))[:10],
            "source": "ashby"
        } for job in res.json().get('jobs', [])]
    except Exception:
        return []


def fetch_workday(tenant, site_id):
    all_jobs = []
    offset = 0
    limit = 100
    while True:
        url = f"https://{tenant}.wd1.myworkdayjobs.com/wday/cxs/{tenant}/{site_id}/jobs"
        payload = {
            "appliedFacets": {},
            "limit": limit,
            "offset": offset,
            "searchText": "engineer OR scientist OR developer"
        }
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        try:
            res = requests.post(url, json=payload, headers=headers, timeout=10)
            res.raise_for_status()
            data = res.json()
            jobs = data.get('jobPostings', [])
            if not jobs:
                break
            all_jobs.extend([{
                "company": tenant.capitalize(),
                "title": job.get("title", ""),
                "location": job.get("locationsText", "Remote"),
                "url": f"https://{tenant}.wd1.myworkdayjobs.com/en-US/{site_id}{job.get('externalPath', '')}",
                "description": "Requires secondary fetch",
                "posted_at": parse_wd_date(job.get("postedOn", "")),
                "source": "workday"
            } for job in jobs])
            if len(jobs) < limit:
                break
            offset += limit
        except Exception:
            break
    return all_jobs

# ─────────────────────────────────────────
# EXECUTE
# ─────────────────────────────────────────
all_jobs = []
for c in TARGETS["greenhouse"]: all_jobs.extend(fetch_greenhouse(c))
for c in TARGETS["lever"]:      all_jobs.extend(fetch_lever(c))
for c in TARGETS["ashby"]:      all_jobs.extend(fetch_ashby(c))
for w in TARGETS["workday"]:    all_jobs.extend(fetch_workday(w["tenant"], w["site_id"]))

# Filter: location + title + posted within 30 days
filtered = [
    job for job in all_jobs
    if is_target_location(job["location"])
    and is_target_title(job["title"])
    and days_ago(job["posted_at"]) <= 30
]

print(json.dumps(filtered))