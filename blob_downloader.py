"""Download reports from Azure Blob Storage using a SAS URL."""

import argparse
import os
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse
from xml.etree import ElementTree

import requests

def _load_dotenv(path: str = ".env") -> None:
    """Load simple KEY=VALUE pairs from a .env file into os.environ."""
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'").strip('"')
            if key:
                os.environ.setdefault(key, value)


_load_dotenv()

BASE_SAS_URL = os.getenv("BASE_SAS_URL", "")
DEFAULT_TIMEOUT = (10, 120)
DEFAULT_CHUNK_SIZE = 256 * 1024

# Shared session keeps TCP/TLS connections warm across requests.
SESSION = requests.Session()


def _sas_params(sas_url: str) -> str:
    """Extract the query string (SAS token) from the full URL."""
    parsed = urlparse(sas_url)
    return parsed.query


def _base_url(sas_url: str) -> str:
    """Extract the base URL (scheme + host + path) without query params."""
    parsed = urlparse(sas_url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def list_blobs(sas_url: str = BASE_SAS_URL) -> list[dict]:
    """List all blobs in the container. Returns list of {name, last_modified, size}."""
    base = _base_url(sas_url)
    params = _sas_params(sas_url)
    blobs = []
    marker = None

    while True:
        url = f"{base}?restype=container&comp=list&{params}"
        if marker:
            url += f"&marker={marker}"

        resp = SESSION.get(url, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()

        root = ElementTree.fromstring(resp.content)
        for blob in root.findall(".//Blob"):
            name = blob.findtext("Name")
            props = blob.find("Properties")
            last_modified = props.findtext("Last-Modified") if props is not None else None
            size = props.findtext("Content-Length") if props is not None else None
            blobs.append(
                {
                    "name": name,
                    "last_modified": last_modified,
                    "size": int(size) if size else 0,
                }
            )

        next_marker = root.findtext("NextMarker")
        if not next_marker:
            break
        marker = next_marker

    return blobs


def download_blob(blob_name: str, dest_dir: str = "downloads", sas_url: str = BASE_SAS_URL):
    """Download a single blob to dest_dir, preserving subfolder structure."""
    base = _base_url(sas_url)
    params = _sas_params(sas_url)
    blob_url = f"{base}/{blob_name}?{params}"

    out_path = os.path.join(dest_dir, blob_name)
    os.makedirs(os.path.dirname(out_path) or dest_dir, exist_ok=True)

    with SESSION.get(blob_url, stream=True, timeout=DEFAULT_TIMEOUT) as resp:
        resp.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
                if chunk:
                    f.write(chunk)

    return out_path


def _parse_last_modified(date_str: str) -> datetime:
    """Parse the Last-Modified header format (RFC 1123) into a datetime."""
    # e.g. "Tue, 11 Feb 2026 12:00:00 GMT"
    return parsedate_to_datetime(date_str)


def _parse_yyyy_mm_dd(value: str) -> date:
    """Parse a YYYY-MM-DD string into a date."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid date '{value}'. Expected YYYY-MM-DD.") from exc


def _blob_modified_date(blob: dict) -> date | None:
    """Return blob last-modified as a UTC calendar date."""
    if not blob.get("last_modified"):
        return None
    dt = _parse_last_modified(blob["last_modified"])
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).date()


def _download_filtered(blobs: list[dict], label: str, dest_dir: str, sas_url: str):
    """Download a pre-filtered blob list with consistent logging."""
    print(f"Found {len(blobs)} blob(s) {label}. Downloading...")
    for blob in blobs:
        path = download_blob(blob["name"], dest_dir, sas_url)
        print(f"  -> {path} ({blob['last_modified']})")
    print("Done.")


def download_all(dest_dir: str = "downloads", sas_url: str = BASE_SAS_URL):
    """Download every blob in the container."""
    blobs = list_blobs(sas_url)
    print(f"Found {len(blobs)} blob(s). Downloading...")
    for blob in blobs:
        path = download_blob(blob["name"], dest_dir, sas_url)
        print(f"  -> {path}")
    print("Done.")


def download_since(since: str, dest_dir: str = "downloads", sas_url: str = BASE_SAS_URL):
    """Download blobs modified on or after `since` (YYYY-MM-DD)."""
    cutoff = _parse_yyyy_mm_dd(since)
    blobs = list_blobs(sas_url)
    filtered = [
        b for b in blobs
        if (modified_date := _blob_modified_date(b)) and modified_date >= cutoff
    ]
    _download_filtered(filtered, f"modified since {since}", dest_dir, sas_url)


def download_date(target_date: str, dest_dir: str = "downloads", sas_url: str = BASE_SAS_URL):
    """Download blobs modified on an exact UTC date (YYYY-MM-DD)."""
    target = _parse_yyyy_mm_dd(target_date)
    blobs = list_blobs(sas_url)
    filtered = [b for b in blobs if _blob_modified_date(b) == target]
    _download_filtered(filtered, f"modified on {target_date}", dest_dir, sas_url)


def download_range(
    start_date: str,
    end_date: str,
    dest_dir: str = "downloads",
    sas_url: str = BASE_SAS_URL,
):
    """Download blobs modified in an inclusive UTC date range (YYYY-MM-DD to YYYY-MM-DD)."""
    start = _parse_yyyy_mm_dd(start_date)
    end = _parse_yyyy_mm_dd(end_date)
    if end < start:
        raise ValueError("End date must be the same as or after start date.")

    blobs = list_blobs(sas_url)
    filtered = [
        b for b in blobs
        if (modified_date := _blob_modified_date(b)) and start <= modified_date <= end
    ]
    _download_filtered(filtered, f"from {start_date} to {end_date}", dest_dir, sas_url)


def main():
    parser = argparse.ArgumentParser(description="Azure Blob Storage report downloader")
    sub = parser.add_subparsers(dest="command", required=True)

    # list
    sub.add_parser("list", help="List all blobs in the container")

    # download-all
    dl_all = sub.add_parser("download-all", help="Download every blob")
    dl_all.add_argument("-o", "--output", default="downloads", help="Output directory")

    # download-since
    dl_since = sub.add_parser("download-since", help="Download blobs modified since a date")
    dl_since.add_argument("date", help="Cutoff date (YYYY-MM-DD)")
    dl_since.add_argument("-o", "--output", default="downloads", help="Output directory")

    # download-date
    dl_date = sub.add_parser("download-date", help="Download blobs modified on an exact date")
    dl_date.add_argument("date", help="Target date (YYYY-MM-DD)")
    dl_date.add_argument("-o", "--output", default="downloads", help="Output directory")

    # download-range
    dl_range = sub.add_parser("download-range", help="Download blobs modified in a date range")
    dl_range.add_argument("start_date", help="Range start date (YYYY-MM-DD)")
    dl_range.add_argument("end_date", help="Range end date (YYYY-MM-DD)")
    dl_range.add_argument("-o", "--output", default="downloads", help="Output directory")

    parser.add_argument("--sas-url", default=BASE_SAS_URL, help="Full SAS URL (overrides .env)")

    args = parser.parse_args()
    sas_url = args.sas_url

    if not sas_url:
        parser.error("No SAS URL found. Set BASE_SAS_URL in .env or pass --sas-url.")

    try:
        if args.command == "list":
            blobs = list_blobs(sas_url)
            print(f"{'Name':<60} {'Size':>10}  {'Last Modified'}")
            print("-" * 100)
            for b in blobs:
                size_kb = f"{b['size'] / 1024:.1f} KB" if b["size"] else "?"
                print(f"{b['name']:<60} {size_kb:>10}  {b['last_modified'] or '?'}")
            print(f"\nTotal: {len(blobs)} blob(s)")

        elif args.command == "download-all":
            download_all(args.output, sas_url)

        elif args.command == "download-since":
            download_since(args.date, args.output, sas_url)

        elif args.command == "download-date":
            download_date(args.date, args.output, sas_url)

        elif args.command == "download-range":
            download_range(args.start_date, args.end_date, args.output, sas_url)
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
