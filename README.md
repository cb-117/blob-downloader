# Montecito Blob Downloader

Small Python CLI for listing and downloading Azure Blob reports using a SAS URL.

## Setup

1. Create a virtual environment:
   ```bash
   python3 -m venv .venv
   ```
2. Activate it:
   macOS/Linux:
   ```bash
   source .venv/bin/activate
   ```
   Windows (PowerShell):
   ```powershell
   .\.venv\Scripts\Activate.ps1
   ```
   Windows (Command Prompt):
   ```bat
   .\.venv\Scripts\activate.bat
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Create `.env` in the project root:
   ```env
   BASE_SAS_URL=https://<your-container-url>?sp=...&sig=...
   ```

## Usage

List blobs:
```bash
python blob_downloader.py list
```

Download all:
```bash
python blob_downloader.py download-all -o downloads
```

Download since date (inclusive):
```bash
python blob_downloader.py download-since 2026-02-01 -o downloads
```

Download exact date:
```bash
python blob_downloader.py download-date 2026-02-10 -o downloads
```

Download date range (inclusive):
```bash
python blob_downloader.py download-range 2026-02-01 2026-02-10 -o downloads
```

Override `.env` URL at runtime:
```bash
python blob_downloader.py list --sas-url "https://...?...&sig=..."
```
