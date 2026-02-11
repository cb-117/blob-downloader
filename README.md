# Montecito Blob Downloader

Small Python CLI for listing and downloading Azure Blob reports using a SAS URL.

## Setup

1. Install dependency:
   ```bash
   pip install requests
   ```
2. Create `.env` in the project root:
   ```env
   BASE_SAS_URL=https://<your-container-url>?sp=...&sig=...
   ```

## Usage

List blobs:
```bash
python3 blob_downloader.py list
```

Download all:
```bash
python3 blob_downloader.py download-all -o downloads
```

Download since date (inclusive):
```bash
python3 blob_downloader.py download-since 2026-02-01 -o downloads
```

Download exact date:
```bash
python3 blob_downloader.py download-date 2026-02-10 -o downloads
```

Download date range (inclusive):
```bash
python3 blob_downloader.py download-range 2026-02-01 2026-02-10 -o downloads
```

Override `.env` URL at runtime:
```bash
python3 blob_downloader.py list --sas-url "https://...?...&sig=..."
```
