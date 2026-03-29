import json
import datetime
from jobspy import scrape_jobs

# ─────────────────────────────────────────
# ATS TARGETS — Greenhouse, Lever, Ashby
# ─────────────────────────────────────────
import requests

ATS_TARGETS = {
    "greenhouse": [
        "cohere", "anthropic", "scaleai", "assemblyai", "glean",
        "dataiku", "benchsci", "ada", "wealthsimple", "koho",
        "stripe", "databricks", "reddit", "pinterest", "discord",
        "instacart", "twitch", "coveo", "d2l"
    ],
    "lever": [
        "shopify", "netflix", "figma", "lyft", "yelp", "coursera",
        "atlassian", "canva", "block", "hootsuite", "clio",
        "faire", "affirm", "rippling", "huggingface"
    ],
    "ashby": [
        "notion", "linear", "loom", "vercel", "fivetran",
        "replit", "descript", "coreweave", "perplexity"
    ]
}

# ─────────────────────────────────────────
# FILTERS
# ─────────────────────────────────────────
TITLE_INCLUDE = [
    "ai engineer", "ml engineer", "machine learning engineer",
    "software engineer", "software developer", "backend engineer",
    "backend developer", "data scientist", "ai developer",
    "applied scientist", "nlp engineer", "llm engineer",
    "mlops", "platform engineer", "data engineer",
    "fullstack engineer", "full stack engineer",
    "python engineer", "generative ai"
]

TITLE_EXCLUDE = [
    "phd", "intern", "internship", "co-op", "coop", "student",
    "director", "vp ", "vice president", "principal engineer",
    "staff engineer", "head of", "manager", "founding"
]

CANADA_ZONES = [
    "toronto", "ottawa", "ontario", "canada", "waterloo",
    "vancouver", "montreal", "calgary", "remote"
]

def is_target_title(title):
    if not title:
        return False
    t = title.lower()
    if any(ex in t for ex in TITLE_EXCLUDE):
        return False
    return any(kw in t for kw in TITLE_INCLUDE)

def is_target_location(loc):
    if not loc:
        return False
    return any(z in str(loc).lower() for z in CANADA_ZONES)

def days_ago(date_str):
    try:
        d = datetime.datetime.strptime(str(date_str)[:10], '%Y-%m-%d')
        return (datetime.datetime.now() - d).days
    except:
        return 999

# ─────────────────────────────────────────
# ATS FETCH FUNCTIONS
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
    except:
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
    except:
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
    except:
        return []

# ─────────────────────────────────────────
# JOBSPY — Indeed, LinkedIn, Google Jobs
# ─────────────────────────────────────────
def fetch_jobspy():
    try:
        jobs_df = scrape_jobs(
            site_name=["indeed", "linkedin", "google"],
            search_term="AI Engineer OR ML Engineer OR Software Engineer OR Data Scientist",
            location="Canada",
            results_wanted=50,
            hours_old=168,          # 7 days
            country_indeed="canada",
            job_type="fulltime",
            description_format="markdown",
            linkedin_fetch_description=True
        )
        if jobs_df is None or jobs_df.empty:
            return []

        results = []
        seen_urls = set()
        for _, row in jobs_df.iterrows():
            url = str(row.get("job_url", "") or "")
            if url in seen_urls:
                continue
            seen_urls.add(url)
            results.append({
                "company": str(row.get("company", "") or "Unknown"),
                "title": str(row.get("title", "") or ""),
                "location": str(row.get("location", "") or ""),
                "url": url,
                "description": str(row.get("description", "") or "")[:5000],
                "posted_at": str(row.get("date_posted", "") or "2000-01-01")[:10],
                "source": str(row.get("site", "") or "jobspy")
            })
        return results
    except Exception as e:
        return []

# ─────────────────────────────────────────
# EXECUTE
# ─────────────────────────────────────────
all_jobs = []

# ATS sources
for c in ATS_TARGETS["greenhouse"]: all_jobs.extend(fetch_greenhouse(c))
for c in ATS_TARGETS["lever"]:      all_jobs.extend(fetch_lever(c))
for c in ATS_TARGETS["ashby"]:      all_jobs.extend(fetch_ashby(c))

# JobSpy sources
all_jobs.extend(fetch_jobspy())

# Apply all filters
seen_urls = set()
filtered = []
for job in all_jobs:
    url = job.get("url", "")
    if url in seen_urls:
        continue
    seen_urls.add(url)
    if not is_target_title(job.get("title", "")):
        continue
    if not is_target_location(job.get("location", "")):
        continue
    if days_ago(job.get("posted_at", "2000-01-01")) > 30:
        continue
    filtered.append(job)

print(json.dumps(filtered))
