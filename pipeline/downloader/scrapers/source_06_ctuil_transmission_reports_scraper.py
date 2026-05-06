"""
Scraper for: https://cea.nic.in/transmission-reports/?lang=en
Download PDFs from "Transmission Reports" year wise for 2 years.
"""
import os
import re
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse, unquote

import requests
from dateutil.relativedelta import relativedelta
from urllib3.exceptions import InsecureRequestWarning
from requests.exceptions import RequestException

BASE_URL = "https://cea.nic.in/transmission-reports/?lang=en"
AJAX_URL = "https://cea.nic.in/wp-admin/admin-ajax.php"
BASE_DIR = "source_output/CTUIL-Transmission-Reports"

CACHE_DB_PATH = None
CACHE_SOURCE_KEY = "transmission_reports"
CACHE_SOURCE_NAME = "CTUIL-Transmission-Reports"
MAX_DOWNLOAD_WORKERS = 5
MAX_RETRIES = 4

TBCB_MARKER_PATTERN = re.compile(
    r"(tbcb|tariff\s+based\s+competitive\s+bidding|competitive\s+bidding\s+route|प्रतिस्पर्धी\s+बोली)",
    re.I,
)
RTM_MARKER_PATTERN = re.compile(
    r"(rtm|regulated\s+tariff\s+mechanism)",
    re.I,
)
UC_MARKER_PATTERN = re.compile(
    r"(\buc\b|under\s+construction|निर्माणाधीन)",
    re.I,
)
COMPLETED_MARKER_PATTERN = re.compile(
    r"(completed|commissioned|\bcomm\b|पूर्ण)",
    re.I,
)

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


def get_last_24_months():
    months = []
    today = datetime.today().replace(day=1)
    for i in range(24):
        m = today - relativedelta(months=i)
        months.append((m.strftime("%Y"), m.strftime("%B"), m.strftime("%Y-%m")))
    return months


def month_name_from_ym(ym):
    year, month_num = ym.split("-")
    dt = datetime.strptime(month_num, "%m")
    month_name = dt.strftime("%B")
    month_index = dt.strftime("%m")
    return year, f"{month_index}_{month_name}"


def clean_text(html_fragment):
    text = re.sub(r"<[^>]+>", " ", html_fragment, flags=re.S)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def categorize_report(title, filename):
    text = f"{title} {filename}"
    text_lower = text.lower()

    if RTM_MARKER_PATTERN.search(text):
        return "rtm_uc"

    if TBCB_MARKER_PATTERN.search(text):
        if COMPLETED_MARKER_PATTERN.search(text):
            return "tbcb_completed"
        if UC_MARKER_PATTERN.search(text):
            return "tbcb_uc"

    # Fallback: some months publish only UC/Comm filenames with Hindi titles.
    if UC_MARKER_PATTERN.search(text) and not ("commissioned_during" in text_lower or "commissioned_in" in text_lower):
        return "tbcb_uc"
    if COMPLETED_MARKER_PATTERN.search(text) and not ("commissioned_during" in text_lower or "commissioned_in" in text_lower):
        return "tbcb_completed"

    return None


def parse_reports_from_html(html, ym):
    data = []
    year, month = month_name_from_ym(ym)
    rows = re.findall(r"<tr[^>]*>.*?</tr>", html, flags=re.S | re.I)
    seen = set()
    selected_by_category = {}

    for row in rows:
        cols = re.findall(r"<td[^>]*>(.*?)</td>", row, flags=re.S | re.I)
        title = clean_text(cols[1]) if len(cols) >= 2 else ""

        href_matches = re.findall(r'href=["\']([^"\']+\.pdf(?:\?[^"\']*)?)["\']', row, flags=re.I)
        if not href_matches:
            continue

        for href in href_matches:
            full_url = urljoin(BASE_URL, href.strip())
            if full_url in seen:
                continue

            filename = unquote(os.path.basename(urlparse(full_url).path)).strip()
            if not filename:
                continue

            category = categorize_report(title, filename)
            if not category:
                continue

            seen.add(full_url)
            # Keep only one file per expected report type for a month.
            if category in selected_by_category:
                continue

            item = {
                "year": year,
                "month": month,
                "title": title,
                "filename": filename,
                "url": full_url,
                "ym": ym,
                "category": category,
            }
            selected_by_category[category] = item
            data.append(item)

    return data


def fetch_reports_for_month(session, ym):
    payload = {
        "action": "monthly_archive_report",
        "selMonthYear": ym,
        "reportType": "transmission",
        "lang": "en",
    }
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.post(AJAX_URL, data=payload, timeout=45, verify=False)
            if resp.status_code == 200:
                return parse_reports_from_html(resp.text, ym)
            last_error = RuntimeError(f"AJAX failed ({resp.status_code}) for {ym}")
        except RequestException as e:
            last_error = e
        time.sleep(1.2 * attempt)

    raise RuntimeError(f"AJAX request failed for {ym}: {last_error}")


def download_pdf(session, item):
    try:
        folder = os.path.join(BASE_DIR, item["year"], item["month"])
        os.makedirs(folder, exist_ok=True)

        filename = item["filename"]
        path = os.path.join(folder, filename)

        from pipeline.downloader.pdf_cache import get_pdf_cache
        cache = get_pdf_cache(CACHE_DB_PATH, CACHE_SOURCE_KEY, CACHE_SOURCE_NAME)
        if cache.is_cached(filename, pdf_path=path):
            return

        if os.path.exists(path):
            print(f"Skipping: {path}")
            return

        last_error = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = session.get(item["url"], timeout=90, verify=False)
                if resp.status_code == 200:
                    with open(path, "wb") as f:
                        f.write(resp.content)
                    cache.record_download(pdf_name=filename, pdf_path=path)
                    print(f"Downloaded -> {path}")
                    return
                last_error = f"HTTP {resp.status_code}"
            except RequestException as e:
                last_error = str(e)
            time.sleep(1.2 * attempt)

        print(f"Failed ({last_error}) -> {item['url']}")
    except Exception as e:
        print(f"Error -> {e}")


def main():
    all_results = []
    months = get_last_24_months()

    print(f"Opening {BASE_URL}")

    with requests.Session() as session:
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": BASE_URL,
                "Connection": "close",
            }
        )
        # Important: CEA returns different transmission rows by WPML language context.
        # Force English dataset so RTM/TBCB rows are consistently available.
        session.cookies.set("wp-wpml_current_language", "en", domain="cea.nic.in", path="/")
        for year, month_name, ym in months:
            print(f"---> Selecting {month_name} {year}")
            try:
                reports = fetch_reports_for_month(session, ym)
                all_results.extend(reports)
                print(f"      Found {len(reports)} reports")
            except Exception as e:
                print(f"Error for {month_name} {year}: {e}")

        print(f"\nTotal reports: {len(all_results)}")

        with ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as executor:
            futures = [executor.submit(download_pdf, session, item) for item in all_results]
            for _ in as_completed(futures):
                pass

    print("\nAll downloads completed.")


if __name__ == "__main__":
    main()
