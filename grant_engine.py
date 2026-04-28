"""
grant_engine.py — All grant scanning, DB, keyword, and export logic.
Consolidated from the Jupyter notebook into a single importable module.
"""

import requests
import feedparser
import sqlite3
import hashlib
import json
import re
import time
import logging
from bs4 import BeautifulSoup
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional
from contextlib import contextmanager
import pandas as pd
from datetime import datetime, timedelta, timezone

try:
    from dateutil.relativedelta import relativedelta
except ImportError:
    relativedelta = None

try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    from openpyxl.worksheet.datavalidation import DataValidation
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False


# ==============================================================
# UTILITIES
# ==============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("grant_monitor.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("grant_monitor")

CONFIG = {
    "db_path": "grants.db",
    "request_delay_seconds": 2,
    "request_timeout_seconds": 30,
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

DEFAULT_KEYWORDS = [
    "cybersecurity", "AI", "experiential learning", "medical technology",
    "data science", "machine learning", "artificial intelligence",
    "STEM education", "novel education", "workforce",
    "workforce development", "quantum",
]


# ==============================================================
# DATABASE
# ==============================================================

@contextmanager
def get_db(db_path: str = None):
    if db_path is None:
        db_path = CONFIG["db_path"]
    conn = sqlite3.connect(db_path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@dataclass
class Grant:
    title: str
    url: str
    source: str
    description: str = ""
    deadline: Optional[str] = None
    posted_date: Optional[str] = None
    funding_amount: Optional[str] = None
    matched_keywords: list = field(default_factory=list)
    content_hash: str = ""
    first_seen: str = ""
    last_seen: str = ""
    is_new: bool = True

    def compute_hash(self):
        raw = f"{self.title}|{self.description}|{self.deadline}".lower().strip()
        self.content_hash = hashlib.sha256(raw.encode()).hexdigest()
        return self.content_hash


def init_db(db_path: str = None):
    if db_path is None:
        db_path = CONFIG["db_path"]
    with get_db(db_path) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS grants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL, url TEXT NOT NULL, source TEXT NOT NULL,
                description TEXT, deadline TEXT, posted_date TEXT,
                funding_amount TEXT, matched_keywords TEXT,
                content_hash TEXT UNIQUE, first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL, notified INTEGER DEFAULT 0
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS scan_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL, scan_time TEXT NOT NULL,
                grants_found INTEGER DEFAULT 0, new_grants INTEGER DEFAULT 0,
                updated_grants INTEGER DEFAULT 0, status TEXT DEFAULT 'success',
                error_message TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT UNIQUE NOT NULL,
                added_date TEXT NOT NULL, active INTEGER DEFAULT 1
            )
        """)
        now = utc_now().strftime("%Y-%m-%dT%H:%M:%S")
        for kw in DEFAULT_KEYWORDS:
            c.execute(
                "INSERT OR IGNORE INTO keywords (keyword, added_date) VALUES (?, ?)",
                (kw, now),
            )
    logger.info("Database initialized.")


def upsert_grant(grant: Grant, db_path: str = None) -> str:
    if db_path is None:
        db_path = CONFIG["db_path"]
    now = utc_now().strftime("%Y-%m-%dT%H:%M:%S")
    grant.compute_hash()
    grant.last_seen = now

    with get_db(db_path) as conn:
        c = conn.cursor()
        c.execute("SELECT id, content_hash FROM grants WHERE url = ?", (grant.url,))
        row = c.fetchone()

        if row is None:
            grant.first_seen = now
            c.execute("""
                INSERT INTO grants
                (title, url, source, description, deadline, posted_date,
                 funding_amount, matched_keywords, content_hash, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                grant.title, grant.url, grant.source, grant.description,
                grant.deadline, grant.posted_date, grant.funding_amount,
                json.dumps(grant.matched_keywords), grant.content_hash,
                grant.first_seen, grant.last_seen,
            ))
            return "new"
        elif row[1] != grant.content_hash:
            c.execute("""
                UPDATE grants SET title=?, description=?, deadline=?, posted_date=?,
                    funding_amount=?, matched_keywords=?, content_hash=?,
                    last_seen=?, notified=0
                WHERE url=?
            """, (
                grant.title, grant.description, grant.deadline,
                grant.posted_date, grant.funding_amount,
                json.dumps(grant.matched_keywords), grant.content_hash,
                grant.last_seen, grant.url,
            ))
            return "updated"
        else:
            c.execute("UPDATE grants SET last_seen=? WHERE url=?", (now, grant.url))
            return "unchanged"


def log_scan(source, grants_found, new_grants, updated_grants,
             status="success", error_message=None):
    with get_db() as conn:
        conn.execute("""
            INSERT INTO scan_log
            (source, scan_time, grants_found, new_grants, updated_grants, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (source, utc_now().strftime("%Y-%m-%dT%H:%M:%S"), grants_found,
              new_grants, updated_grants, status, error_message))


# ==============================================================
# KEYWORDS
# ==============================================================

def get_active_keywords(db_path: str = None) -> list:
    if db_path is None:
        db_path = CONFIG["db_path"]
    with get_db(db_path) as conn:
        c = conn.cursor()
        c.execute("SELECT keyword FROM keywords WHERE active = 1")
        return [row[0] for row in c.fetchall()]


def add_keyword(kw: str):
    now = utc_now().strftime("%Y-%m-%dT%H:%M:%S")
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO keywords (keyword, added_date, active) VALUES (?, ?, 1)",
            (kw, now),
        )
        conn.execute(
            "UPDATE keywords SET active = 1 WHERE keyword = ? AND active = 0", (kw,)
        )


def remove_keyword(kw: str):
    with get_db() as conn:
        conn.execute("UPDATE keywords SET active = 0 WHERE keyword = ?", (kw,))


def match_keywords(text: str, keywords: list = None) -> list:
    if keywords is None:
        keywords = get_active_keywords()
    text_lower = text.lower()
    matched = []
    for kw in keywords:
        if kw == "AI":
            if re.search(r'\bAI\b', text):
                matched.append(kw)
        else:
            if re.search(r'\b' + re.escape(kw.lower()) + r'\b', text_lower):
                matched.append(kw)
    return matched


def passes_filter(title: str, description: str) -> list:
    return match_keywords(f"{title} {description}")


# ==============================================================
# HTTP SESSION
# ==============================================================

def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": CONFIG["user_agent"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


SESSION = get_session()


def safe_request(url: str, **kwargs):
    try:
        time.sleep(CONFIG["request_delay_seconds"])
        resp = SESSION.get(url, timeout=CONFIG["request_timeout_seconds"], **kwargs)
        resp.raise_for_status()
        return resp
    except requests.RequestException as e:
        logger.error(f"Request failed for {url}: {e}")
        return None


# ==============================================================
# SCRAPERS
# ==============================================================

def scrape_grants_gov(keywords=None):
    if keywords is None:
        keywords = get_active_keywords()
    grants = []
    base_url = "https://apply07.grants.gov/grantsws/rest/opportunities/search/"
    for kw in keywords:
        payload = {
            "keyword": kw, "oppStatuses": "forecasted|posted",
            "sortBy": "openDate|desc", "rows": 50, "startRecordNum": 0,
        }
        try:
            time.sleep(CONFIG["request_delay_seconds"])
            resp = requests.post(base_url, json=payload,
                                 headers={"Content-Type": "application/json"},
                                 timeout=CONFIG["request_timeout_seconds"])
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"Grants.gov API error for '{kw}': {e}")
            continue
        opportunities = data.get("oppHits", [])
        logger.info(f"Grants.gov: '{kw}' returned {len(opportunities)} results")
        for opp in opportunities:
            title = opp.get("title", "")
            description = opp.get("description", title)
            url = f"https://www.grants.gov/search-results-detail/{opp.get('id', '')}"
            matched = passes_filter(title, description)
            if matched:
                g = Grant(
                    title=title, url=url, source="grants.gov",
                    description=description[:2000],
                    deadline=opp.get("closeDate", ""),
                    posted_date=opp.get("openDate", ""),
                    funding_amount=opp.get("awardCeiling", ""),
                    matched_keywords=matched,
                )
                grants.append(g)
    seen = set()
    unique = []
    for g in grants:
        if g.url not in seen:
            seen.add(g.url)
            unique.append(g)
    logger.info(f"Grants.gov total unique matches: {len(unique)}")
    return unique


def scrape_nsf():
    feeds = [
        "https://www.nsf.gov/rss/rss_www_funding.xml",
        "https://www.nsf.gov/rss/rss_www_funding_pgm_annc_cise.xml",
        "https://www.nsf.gov/rss/rss_www_funding_pgm_annc_ehr.xml",
    ]
    grants = []
    for feed_url in feeds:
        try:
            time.sleep(CONFIG["request_delay_seconds"])
            feed = feedparser.parse(feed_url)
            logger.info(f"NSF feed: {len(feed.entries)} entries from {feed_url}")
        except Exception as e:
            logger.error(f"NSF feed error ({feed_url}): {e}")
            continue
        for entry in feed.entries:
            title = entry.get("title", "")
            summary = entry.get("summary", "")
            link = entry.get("link", "")
            published = entry.get("published", "")
            matched = passes_filter(title, summary)
            if matched:
                g = Grant(title=title, url=link, source="nsf.gov",
                          description=summary[:2000], posted_date=published,
                          matched_keywords=matched)
                grants.append(g)
    seen = set()
    unique = []
    for g in grants:
        if g.url not in seen:
            seen.add(g.url)
            unique.append(g)
    logger.info(f"NSF total unique matches: {len(unique)}")
    return unique


def scrape_nih_reporter():
    grants = []
    keywords = get_active_keywords()
    url = "https://api.reporter.nih.gov/v2/projects/search"
    search_text = " OR ".join(
        [kw for kw in keywords if kw.lower() in
         ("medical technology", "ai", "artificial intelligence",
          "data science", "machine learning", "cybersecurity")]
    )
    payload = {
        "criteria": {
            "advanced_text_search": {
                "operator": "or", "search_field": "projecttitle,terms",
                "search_text": search_text,
            },
            "fiscal_years": [2025, 2026],
            "newly_added_projects_only": True,
        },
        "offset": 0, "limit": 50,
        "sort_field": "project_start_date", "sort_order": "desc",
    }
    try:
        time.sleep(CONFIG["request_delay_seconds"])
        resp = requests.post(url, json=payload, timeout=CONFIG["request_timeout_seconds"])
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"NIH Reporter API error: {e}")
        return grants
    results = data.get("results", [])
    logger.info(f"NIH Reporter: {len(results)} results")
    for proj in results:
        title = proj.get("project_title", "")
        abstract = proj.get("abstract_text", "") or ""
        link = f"https://reporter.nih.gov/project-details/{proj.get('application_id', '')}"
        matched = passes_filter(title, abstract)
        if matched:
            award = proj.get("award_amount")
            g = Grant(
                title=title.strip(), url=link, source="nih_reporter",
                description=abstract[:2000],
                posted_date=proj.get("project_start_date", ""),
                funding_amount=str(award) if award else "",
                matched_keywords=matched,
            )
            grants.append(g)
    logger.info(f"NIH Reporter matches: {len(grants)}")
    return grants


def scrape_rss_source(feed_url, source_name):
    grants = []
    try:
        time.sleep(CONFIG["request_delay_seconds"])
        feed = feedparser.parse(feed_url)
        logger.info(f"{source_name}: {len(feed.entries)} entries")
    except Exception as e:
        logger.error(f"{source_name} feed error: {e}")
        return grants
    for entry in feed.entries:
        title = entry.get("title", "")
        summary = entry.get("summary", entry.get("description", ""))
        link = entry.get("link", "")
        published = entry.get("published", entry.get("updated", ""))
        matched = passes_filter(title, summary)
        if matched:
            g = Grant(title=title, url=link, source=source_name,
                      description=summary[:2000], posted_date=published,
                      matched_keywords=matched)
            grants.append(g)
    logger.info(f"{source_name} matches: {len(grants)}")
    return grants


ADDITIONAL_RSS_SOURCES = [
    {"url": "https://www.ed.gov/feed", "name": "ed.gov"},
    {"url": "https://www.sbir.gov/rss-feed", "name": "sbir.gov"},
    {"url": "https://www.darpa.mil/rss", "name": "darpa.mil"},
]


# ==============================================================
# FULL SCAN
# ==============================================================

def run_full_scan() -> dict:
    logger.info("=" * 60)
    logger.info("STARTING FULL GRANT SCAN")
    logger.info(f"Active keywords: {get_active_keywords()}")
    logger.info("=" * 60)

    summary = {
        "scan_time": utc_now().isoformat(),
        "sources_scanned": 0, "total_found": 0,
        "new": 0, "updated": 0, "unchanged": 0, "errors": [],
    }
    all_grants = []

    try:
        results = scrape_grants_gov()
        all_grants.extend(results)
        log_scan("grants.gov", len(results), 0, 0)
        summary["sources_scanned"] += 1
    except Exception as e:
        logger.error(f"Grants.gov scraper failed: {e}")
        log_scan("grants.gov", 0, 0, 0, "error", str(e))
        summary["errors"].append(f"grants.gov: {e}")

    try:
        results = scrape_nsf()
        all_grants.extend(results)
        log_scan("nsf.gov", len(results), 0, 0)
        summary["sources_scanned"] += 1
    except Exception as e:
        logger.error(f"NSF scraper failed: {e}")
        log_scan("nsf.gov", 0, 0, 0, "error", str(e))
        summary["errors"].append(f"nsf.gov: {e}")

    try:
        results = scrape_nih_reporter()
        all_grants.extend(results)
        log_scan("nih_reporter", len(results), 0, 0)
        summary["sources_scanned"] += 1
    except Exception as e:
        logger.error(f"NIH Reporter scraper failed: {e}")
        log_scan("nih_reporter", 0, 0, 0, "error", str(e))
        summary["errors"].append(f"nih_reporter: {e}")

    for src in ADDITIONAL_RSS_SOURCES:
        try:
            results = scrape_rss_source(src["url"], src["name"])
            all_grants.extend(results)
            log_scan(src["name"], len(results), 0, 0)
            summary["sources_scanned"] += 1
        except Exception as e:
            logger.error(f"{src['name']} scraper failed: {e}")
            log_scan(src["name"], 0, 0, 0, "error", str(e))
            summary["errors"].append(f"{src['name']}: {e}")

    summary["total_found"] = len(all_grants)
    for grant in all_grants:
        result = upsert_grant(grant)
        summary[result] = summary.get(result, 0) + 1

    logger.info("=" * 60)
    logger.info(f"SCAN COMPLETE: {summary}")
    logger.info("=" * 60)
    return summary


# ==============================================================
# REPORTS
# ==============================================================

def get_grants_df(since_days: int = 7, source_filter: str = None) -> pd.DataFrame:
    cutoff = (utc_now() - timedelta(days=since_days)).strftime("%Y-%m-%dT%H:%M:%S")
    with get_db() as conn:
        df = pd.read_sql_query("""
            SELECT title, url, source, description, deadline,
                   posted_date, funding_amount, matched_keywords,
                   first_seen, last_seen,
                   CASE WHEN first_seen >= ? THEN 'NEW' ELSE 'UPDATED' END as status
            FROM grants
            WHERE first_seen >= ? OR (last_seen >= ? AND notified = 0)
            ORDER BY first_seen DESC
        """, conn, params=(cutoff, cutoff, cutoff))
    if not df.empty:
        df["matched_keywords"] = df["matched_keywords"].apply(
            lambda x: ", ".join(json.loads(x)) if x else ""
        )
    if source_filter and source_filter != "all":
        df = df[df["source"] == source_filter]
    return df


def get_all_grants_df() -> pd.DataFrame:
    with get_db() as conn:
        df = pd.read_sql_query("""
            SELECT title, url, source, description, deadline,
                   posted_date, funding_amount, matched_keywords,
                   first_seen, last_seen
            FROM grants ORDER BY first_seen DESC
        """, conn)
    if not df.empty:
        df["matched_keywords"] = df["matched_keywords"].apply(
            lambda x: ", ".join(json.loads(x)) if x else ""
        )
    return df


def export_csv(since_days: int = 7) -> str:
    df = get_grants_df(since_days)
    filename = f"grant_report_{utc_now().strftime('%Y%m%d')}.csv"
    df.to_csv(filename, index=False)
    logger.info(f"CSV exported: {filename}")
    return filename


# ==============================================================
# EXCEL EXPORT
# ==============================================================

def parse_deadline(raw: str) -> Optional[datetime]:
    if not raw or raw.strip() == "":
        return None
    formats = [
        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y",
        "%B %d, %Y", "%b %d, %Y", "%m-%d-%Y", "%Y%m%d",
    ]
    cleaned = raw.strip().split("+")[0].split("Z")[0]
    for fmt in formats:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    return None


def generate_grant_excel(output_filename: str = None) -> str:
    if not HAS_OPENPYXL:
        raise ImportError("openpyxl is required. pip install openpyxl")
    if output_filename is None:
        output_filename = f"Grant_Tracker_{utc_now().strftime('%Y%m%d')}.xlsx"

    df = get_all_grants_df()
    if df.empty:
        return None

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Grant Tracker"
    ws.freeze_panes = "A2"

    HEADER_FILL = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    HEADER_FONT = Font(name="Calibri", bold=True, color="FFFFFF", size=11)
    THIN_BORDER = Border(
        left=Side(style="thin", color="B4C6E7"),
        right=Side(style="thin", color="B4C6E7"),
        top=Side(style="thin", color="B4C6E7"),
        bottom=Side(style="thin", color="B4C6E7"),
    )

    columns = [
        ("Grant / Program", 50), ("Source", 16), ("Keywords", 28),
        ("Deadline", 16), ("Funding", 18), ("URL", 50),
        ("First Seen", 20), ("Status", 12),
    ]
    for col_idx, (header, width) in enumerate(columns, 1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = THIN_BORDER
        ws.column_dimensions[get_column_letter(col_idx)].width = width

    for row_idx, (_, row) in enumerate(df.iterrows(), 2):
        dl = parse_deadline(row.get("deadline", ""))
        days_left = (dl - utc_now()).days if dl else None
        status = "NEW" if row_idx <= 10 else ""

        values = [
            row["title"], row["source"], row["matched_keywords"],
            dl.strftime("%b %d, %Y") if dl else "TBD",
            row.get("funding_amount", ""),
            row["url"], row["first_seen"], status,
        ]
        for col_idx, val in enumerate(values, 1):
            cell = ws.cell(row=row_idx, column=col_idx, value=val)
            cell.border = THIN_BORDER
            cell.alignment = Alignment(wrap_text=True, vertical="top")
            if col_idx == 6:
                cell.font = Font(color="0563C1", underline="single", size=9)
                cell.hyperlink = str(val)
            if col_idx == 4 and days_left is not None and 0 < days_left <= 30:
                cell.font = Font(color="C62828", bold=True)

    ws.auto_filter.ref = f"A1:{get_column_letter(len(columns))}{len(df) + 1}"
    wb.save(output_filename)
    logger.info(f"Excel saved: {output_filename}")
    return output_filename
