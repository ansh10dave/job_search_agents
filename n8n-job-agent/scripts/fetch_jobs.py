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
        "shopify", "netflix", "figma", "lyft", "yelp", "coursera", 
        "atlassian", "canva", "block" # Block/Square has a huge Toronto hub
    ],
    "ashby": [
        "notion", "linear", "loom", "vercel", "fivetran"
    ],
    "workday": [
        {"tenant": "autodesk", "site_id": "autodesk"},
        {"tenant": "wbd", "site_id": "wbd_careers"},
        {"tenant": "mastercard", "site_id": "Corporate_Careers"},
        {"tenant": "nvidia", "site_id": "NVIDIAExternalCareerSite"},
        {"tenant": "cibc", "site_id": "cibc_careers"},
        {"tenant": "electronicarts", "site_id": "EA_ext"}
    ]
}

# Date filter 
def parse_wd_date(posted_str):
    if not posted_str: return "2000-01-01" # Fall back for missing dates 
    s = posted_str.lower() 
    today = datetime.datetime.now()
    if "today" in s: return today.strftime('%Y-%m-%d')
    if "yesterday" in s: return (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    match = re.search(r'(\d+)', s)
    if match:
        return (today - datetime.timedelta(days=int(match.group(1)))).strftime('%Y-%m-%d')
    return "2000-01-01"

# 2. The Geographic Filter
def is_target_location(location_str):
    if not location_str:
        return False
    
    loc = location_str.lower()
    # Accept anything in Toronto, Ottawa, Ontario generally, or Canada explicitly
    valid_zones = ["toronto", "ottawa", "ontario", "canada"]
    
    return any(zone in loc for zone in valid_zones)

# --- Fetch Functions ---

def fetch_greenhouse(company_id):
    url = f"https://boards-api.greenhouse.io/v1/boards/{company_id}/jobs?content=true"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        jobs = res.json().get('jobs', [])
        return [{
            "company": company_id.capitalize(),
            "title": job.get("title", ""),
            "location": job.get("location", {}).get("name", "Remote"),
            "url": job.get("absolute_url", ""),
            "description": job.get("content", ""),
            "posted_at": job.get("updated_at", "2000-01-01")[:10]
        } for job in jobs]
    except Exception: return []

def fetch_lever(company_id):
    url = f"https://api.lever.co/v0/postings/{company_id}?mode=json"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        jobs = res.json()
        return [{
            "company": company_id.capitalize(),
            "title": job.get("text", ""),
            "location": job.get("categories", {}).get("location", "Remote"),
            "url": job.get("hostedUrl", ""),
            "description": job.get("descriptionPlain", ""),
            "posted_at": datetime.datetime.fromtimestamp(job.get("createdAt", 0)/1000).strftime('%Y-%m-%d') if job.get("createdAt") else "2000-01-01"
        } for job in jobs]
    except Exception: return []

def fetch_ashby(company_id):
    url = f"https://api.ashbyhq.com/posting-api/job-board/{company_id}?includeCompensation=true"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        jobs = res.json().get('jobs', [])
        return [{
            "company": company_id.capitalize(),
            "title": job.get("title", ""),
            "location": job.get("location", "Remote"),
            "url": job.get("jobUrl", ""),
            "description": job.get("descriptionPlain", ""),
            "posted_at": job.get("publishedAt", job.get("createdAt", "2000-01-01"))[:10]
        } for job in jobs]
    except Exception: return []

def fetch_workday(tenant, site_id):
    url = f"https://{tenant}.wd1.myworkdayjobs.com/wday/cxs/{tenant}/{site_id}/jobs"
    payload = {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": ""}
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=10)
        res.raise_for_status()
        jobs = res.json().get('jobPostings', [])
        return [{
            "company": tenant.capitalize(),
            "title": job.get("title", ""),
            "location": job.get("locationsText", "Remote"),
            "url": f"https://{tenant}.wd1.myworkdayjobs.com/en-US/{site_id}{job.get('externalPath', '')}",
            "description": "Requires secondary fetch",
            "posted_at": parse_wd_date(job.get("postedOn", ""))
        } for job in jobs]
    except Exception: return []

# --- Execution ---

all_jobs = []
for company in TARGETS["greenhouse"]: all_jobs.extend(fetch_greenhouse(company))
for company in TARGETS["lever"]: all_jobs.extend(fetch_lever(company))
for company in TARGETS["ashby"]: all_jobs.extend(fetch_ashby(company))
for wd in TARGETS["workday"]: all_jobs.extend(fetch_workday(wd["tenant"], wd["site_id"]))

# 3. Apply the Canada Filter before outputting
local_jobs = [job for job in all_jobs if is_target_location(job["location"])]

# Output only the local/Canadian jobs to n8n
print(json.dumps(local_jobs))