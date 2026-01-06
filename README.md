# CMS Bulk Download Tool

A Python tool to bulk download all datasets from [data.cms.gov](https://data.cms.gov), including all versions and formats.

The code was written by Claude. It was pretty straightfoward, mostly driven off of a simpple [prompt](https://docs.google.com/document/d/1-BrO5aruFIsMk8W6kFwt51TBXIi-ynWoh20aGPcWP7k/edit?tab=t.0).

## Features

- Downloads all 149+ datasets from CMS data portal
- Handles multiple versions and formats of each dataset
- Downloads CSV files, ZIP files, and API data
- Automatically resumes if interrupted (skips already downloaded files)
- Respectful rate limiting with configurable delays
- Progress logging and statistics tracking
- Saves dataset metadata for reference

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

Download all datasets to the default directory (`cms_data`):

```bash
python cms_bulk_download.py
```

### Custom Output Directory

```bash
python cms_bulk_download.py --output-dir /path/to/data
```

### Adjust Request Delay

To be more respectful of the server, increase the delay between requests:

```bash
python cms_bulk_download.py --delay 1.0
```

### Resume Interrupted Download

The tool automatically skips files that have already been downloaded, so simply run it again:

```bash
python cms_bulk_download.py
```

## Output Structure

```
cms_data/
├── {dataset-id}_{dataset-name}/
│   ├── metadata.json                    # Dataset metadata
│   ├── {distribution-name}.csv          # CSV files
│   ├── {distribution-name}.zip          # ZIP files
│   ├── {distribution-name}.json         # API data
│   └── ...                              # Other formats
└── ...
```

## API Information

This tool uses the CMS Data API:

- **Catalog Endpoint**: `https://data.cms.gov/data.json`
- **Data API Format**: `https://data.cms.gov/data-api/v1/dataset/{dataset-id}/data`
- **No authentication required**

### API Documentation

- [CMS API Documentation](https://data.cms.gov/api-docs)
- [Provider Data Catalog API Docs](https://data.cms.gov/provider-data/docs)

## Implementation Details

### Dataset Discovery

The tool fetches the complete dataset catalog from `https://data.cms.gov/data.json`, which contains:
- Array of all datasets
- For each dataset: metadata and distributions
- For each distribution: format, temporal version, and download URLs

### Download Methods

1. **CSV Files** (`mediaType: "text/csv"`): Direct download via HTTP
2. **ZIP Files** (`mediaType: "application/zip"`): Direct download via HTTP
3. **API Data** (`format: "API"`): Paginated download using the data API with batches of 5000 rows
4. **Other Formats**: Direct download with format-appropriate file extension

### Pagination

For API endpoints, the tool:
1. Fetches dataset statistics to determine total rows
2. Downloads data in batches of 5000 rows using `size` and `offset` parameters
3. Combines all batches into a single JSON file

## Statistics

After completion, the tool displays:
- Number of datasets processed
- Files downloaded vs. skipped
- Total data downloaded (in MB)
- Number of errors
- Total time elapsed

## Notes

- **Disk Space**: Ensure you have sufficient disk space. CMS datasets can be very large.
- **Time**: Downloading all datasets may take several hours or days depending on your connection and the delay setting.
- **Rate Limiting**: The default delay of 0.5 seconds between requests is respectful of the server. Avoid setting it too low.
- **Errors**: Some datasets may fail to download due to network issues or server errors. The tool logs all errors and continues with other datasets.

## Example Output

```
[2026-01-05 10:30:15] [INFO] Starting CMS bulk download...
[2026-01-05 10:30:15] [INFO] Output directory: /Users/user/cms_data
[2026-01-05 10:30:15] [INFO] Fetching dataset catalog from data.cms.gov...
[2026-01-05 10:30:16] [INFO] Found 149 datasets in catalog
[2026-01-05 10:30:16] [INFO] Processing 149 datasets...

[2026-01-05 10:30:16] [INFO] [1/149] Dataset 1 of 149
[2026-01-05 10:30:16] [INFO] ================================================================================
[2026-01-05 10:30:16] [INFO] Processing dataset: Accountable Care Organization Participants
[2026-01-05 10:30:16] [INFO] Identifier: aco-participants
[2026-01-05 10:30:16] [INFO] Found 12 distributions/versions
[2026-01-05 10:30:16] [INFO] Processing distribution 1/12
[2026-01-05 10:30:16] [INFO] Downloading: Accountable Care Organization Participants - Q1 2024
...
```

## License

This tool is provided as-is for downloading publicly available CMS data.

## Sources

- [CMS API Documentation](https://data.cms.gov/api-docs)
- [API Guide PDF (v1.6)](https://data.cms.gov/sites/default/files/2024-10/7ef65521-65a4-41ed-b600-3a0011f8ec4b/API%20Guide%20Formatted%201_6.pdf)
- [CKAN API Documentation](https://docs.ckan.org/en/latest/api/)

