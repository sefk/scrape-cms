#!/usr/bin/env python3
"""
CMS Bulk Download Tool
Downloads latest data from data.cms.gov for each dataset.

Based on the CMS Data API specification:
- Catalog: https://data.cms.gov/data.json
- API Format: https://data.cms.gov/data-api/v1/dataset/{dataset-id}/data
"""

import json
import os
import sys
import time
import requests
import re
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime
from typing import Dict, List, Optional


class CMSBulkDownloader:
    """Download latest data for all datasets from data.cms.gov"""

    def __init__(self, output_dir: str = "cms_data", delay: float = 0.5):
        """
        Initialize the downloader.

        Args:
            output_dir: Directory to save downloaded files
            delay: Delay between requests in seconds to be respectful of the server
        """
        self.catalog_url = "https://data.cms.gov/data.json"
        self.output_dir = Path(output_dir)
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CMS-Bulk-Downloader/1.0'
        })

        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Statistics
        self.stats = {
            'datasets_processed': 0,
            'files_downloaded': 0,
            'files_skipped': 0,
            'errors': 0,
            'total_bytes': 0
        }

    def log(self, message: str, level: str = "INFO"):
        """Log a message with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] [{level}] {message}", flush=True)

    def fetch_catalog(self) -> Dict:
        """Fetch the data.json catalog containing all datasets"""
        self.log("Fetching dataset catalog from data.cms.gov...")
        try:
            response = self.session.get(self.catalog_url, timeout=30)
            response.raise_for_status()
            catalog = response.json()
            dataset_count = len(catalog.get('dataset', []))
            self.log(f"Found {dataset_count} datasets in catalog")
            return catalog
        except Exception as e:
            self.log(f"Error fetching catalog: {e}", "ERROR")
            raise

    def sanitize_filename(self, filename: str) -> str:
        """Sanitize filename to be filesystem-safe"""
        # Replace problematic characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        # Limit length
        if len(filename) > 200:
            filename = filename[:200]
        return filename

    def download_file(self, url: str, output_path: Path, description: str = "") -> bool:
        """
        Download a file from URL to output_path.

        Returns:
            True if successful, False otherwise
        """
        # Skip if file already exists
        if output_path.exists():
            file_size = output_path.stat().st_size
            self.log(f"Skipping (already exists): {output_path.name} ({file_size:,} bytes)")
            self.stats['files_skipped'] += 1
            return True

        try:
            self.log(f"Downloading: {description or output_path.name}")
            response = self.session.get(url, timeout=300, stream=True)
            response.raise_for_status()

            # Create parent directory if needed
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Download with progress
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0

            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

            self.stats['files_downloaded'] += 1
            self.stats['total_bytes'] += downloaded
            self.log(f"Downloaded: {output_path.name} ({downloaded:,} bytes)")

            # Be respectful - add delay between downloads
            time.sleep(self.delay)
            return True

        except Exception as e:
            self.log(f"Error downloading {url}: {e}", "ERROR")
            self.stats['errors'] += 1
            # Clean up partial download
            if output_path.exists():
                output_path.unlink()
            return False

    def download_api_data(self, dataset_id: str, output_path: Path, description: str = "") -> bool:
        """
        Download complete dataset via API endpoint.

        The API supports pagination using size and offset parameters.
        Default limit is 1000 rows per request.
        """
        # Skip if file already exists
        if output_path.exists():
            self.log(f"Skipping (already exists): {output_path.name}")
            self.stats['files_skipped'] += 1
            return True

        try:
            # First get statistics to know total rows
            stats_url = f"https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data/stats"
            self.log(f"Fetching dataset stats: {description or dataset_id}")

            stats_response = self.session.get(stats_url, timeout=30)
            stats_response.raise_for_status()
            stats = stats_response.json()
            total_rows = stats.get('total_rows', 0)

            self.log(f"Dataset has {total_rows:,} rows, downloading via API...")

            # Download data in batches
            all_data = []
            batch_size = 5000
            offset = 0

            while offset < total_rows:
                data_url = f"https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data?size={batch_size}&offset={offset}"
                self.log(f"Fetching rows {offset:,} to {min(offset + batch_size, total_rows):,}")

                response = self.session.get(data_url, timeout=300)
                response.raise_for_status()
                batch_data = response.json()

                all_data.extend(batch_data)
                offset += batch_size

                # Be respectful
                time.sleep(self.delay)

            # Save to file
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, indent=2)

            self.stats['files_downloaded'] += 1
            file_size = output_path.stat().st_size
            self.stats['total_bytes'] += file_size
            self.log(f"Downloaded API data: {output_path.name} ({file_size:,} bytes, {len(all_data):,} rows)")

            return True

        except Exception as e:
            self.log(f"Error downloading API data for {dataset_id}: {e}", "ERROR")
            self.stats['errors'] += 1
            # Clean up partial download
            if output_path.exists():
                output_path.unlink()
            return False

    def extract_latest_date(self, temporal: str) -> Optional[str]:
        """
        Extract the latest date from a temporal field.

        Temporal field can be:
        - A single date: "2024-01-01"
        - A date range: "2024-01-01 to 2024-12-31"
        - Empty string

        Returns the latest date in ISO format or None
        """
        if not temporal:
            return None

        # Look for dates in YYYY-MM-DD format
        dates = re.findall(r'\d{4}-\d{2}-\d{2}', temporal)
        if dates:
            # Return the latest date found
            return max(dates)

        # Try to find just years
        years = re.findall(r'\d{4}', temporal)
        if years:
            return max(years)

        return None

    def filter_latest_distributions(self, distributions: List[Dict]) -> List[Dict]:
        """
        Filter distributions to only include those with the latest temporal value.

        Returns list of distributions with the most recent data.
        """
        if not distributions:
            return []

        # Extract dates for each distribution
        distributions_with_dates = []
        for dist in distributions:
            temporal = dist.get('temporal', '')
            latest_date = self.extract_latest_date(temporal)
            distributions_with_dates.append({
                'distribution': dist,
                'latest_date': latest_date,
                'temporal': temporal
            })

        # Find the maximum date
        max_date = None
        for item in distributions_with_dates:
            if item['latest_date']:
                if max_date is None or item['latest_date'] > max_date:
                    max_date = item['latest_date']

        # If no dates found, return all distributions (can't determine latest)
        if max_date is None:
            self.log("No temporal information found, downloading all distributions", "WARNING")
            return distributions

        # Filter to only distributions with the maximum date
        latest_distributions = [
            item['distribution']
            for item in distributions_with_dates
            if item['latest_date'] == max_date
        ]

        return latest_distributions

    def process_distribution(self, distribution: Dict, dataset_dir: Path, dataset_title: str):
        """Process a single distribution (version/format) of a dataset"""
        try:
            # Get distribution metadata
            title = distribution.get('title', 'unknown')
            dist_format = distribution.get('format', 'unknown')
            media_type = distribution.get('mediaType', '')
            temporal = distribution.get('temporal', '')
            download_url = distribution.get('downloadURL', '')
            access_url = distribution.get('accessURL', '')

            # Create filename
            filename_parts = [self.sanitize_filename(title)]
            if temporal:
                filename_parts.append(self.sanitize_filename(temporal))

            # Determine file extension and download method
            if media_type == 'text/csv' or dist_format.lower() == 'csv':
                filename = '_'.join(filename_parts) + '.csv'
                url = download_url or access_url
                if url:
                    output_path = dataset_dir / filename
                    self.download_file(url, output_path, f"{dataset_title} - {title}")

            elif media_type == 'application/zip' or dist_format.lower() == 'zip':
                filename = '_'.join(filename_parts) + '.zip'
                url = download_url or access_url
                if url:
                    output_path = dataset_dir / filename
                    self.download_file(url, output_path, f"{dataset_title} - {title}")

            elif dist_format == 'API':
                # Extract dataset ID from access URL
                # Format: https://data.cms.gov/data-api/v1/dataset/{dataset-id}/data
                url = access_url or download_url
                if url and '/dataset/' in url:
                    dataset_id = url.split('/dataset/')[1].split('/')[0]
                    filename = '_'.join(filename_parts) + '.json'
                    output_path = dataset_dir / filename
                    self.download_api_data(dataset_id, output_path, f"{dataset_title} - {title}")

            else:
                # Try to download other formats
                url = download_url or access_url
                if url:
                    # Try to determine extension from URL or format
                    parsed = urlparse(url)
                    ext = os.path.splitext(parsed.path)[1] or f".{dist_format.lower()}"
                    filename = '_'.join(filename_parts) + ext
                    output_path = dataset_dir / filename
                    self.download_file(url, output_path, f"{dataset_title} - {title}")

        except Exception as e:
            self.log(f"Error processing distribution: {e}", "ERROR")
            self.stats['errors'] += 1

    def process_dataset(self, dataset: Dict):
        """Process a single dataset and all its distributions"""
        try:
            # Get dataset metadata
            title = dataset.get('title', 'Unknown Dataset')
            identifier = dataset.get('identifier', 'unknown-id')
            description = dataset.get('description', '')

            self.log(f"\n{'='*80}")
            self.log(f"Processing dataset: {title}")
            self.log(f"Identifier: {identifier}")

            # Create dataset directory
            dataset_dir_name = self.sanitize_filename(f"{identifier}_{title}")
            dataset_dir = self.output_dir / dataset_dir_name
            dataset_dir.mkdir(parents=True, exist_ok=True)

            # Save metadata
            metadata_file = dataset_dir / "metadata.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(dataset, f, indent=2)

            # Filter to only latest distributions
            all_distributions = dataset.get('distribution', [])
            self.log(f"Found {len(all_distributions)} total distributions/versions")

            latest_distributions = self.filter_latest_distributions(all_distributions)
            self.log(f"Filtered to {len(latest_distributions)} latest distribution(s)")

            for idx, distribution in enumerate(latest_distributions, 1):
                temporal = distribution.get('temporal', 'no date')
                self.log(f"Processing distribution {idx}/{len(latest_distributions)} (temporal: {temporal})")
                self.process_distribution(distribution, dataset_dir, title)

            self.stats['datasets_processed'] += 1

        except Exception as e:
            self.log(f"Error processing dataset: {e}", "ERROR")
            self.stats['errors'] += 1

    def run(self):
        """Main execution method"""
        start_time = time.time()
        self.log("Starting CMS bulk download...")
        self.log(f"Output directory: {self.output_dir.absolute()}")

        try:
            # Fetch catalog
            catalog = self.fetch_catalog()
            datasets = catalog.get('dataset', [])

            if not datasets:
                self.log("No datasets found in catalog!", "ERROR")
                return

            # Process each dataset
            total_datasets = len(datasets)
            self.log(f"\nProcessing {total_datasets} datasets...\n")

            for idx, dataset in enumerate(datasets, 1):
                self.log(f"\n[{idx}/{total_datasets}] Dataset {idx} of {total_datasets}")
                self.process_dataset(dataset)

            # Print statistics
            elapsed = time.time() - start_time
            self.log(f"\n{'='*80}")
            self.log("Download complete!")
            self.log(f"{'='*80}")
            self.log(f"Datasets processed: {self.stats['datasets_processed']}")
            self.log(f"Files downloaded: {self.stats['files_downloaded']}")
            self.log(f"Files skipped: {self.stats['files_skipped']}")
            self.log(f"Errors: {self.stats['errors']}")
            self.log(f"Total data downloaded: {self.stats['total_bytes']:,} bytes ({self.stats['total_bytes'] / 1024 / 1024:.2f} MB)")
            self.log(f"Time elapsed: {elapsed:.2f} seconds ({elapsed / 60:.2f} minutes)")

        except KeyboardInterrupt:
            self.log("\n\nDownload interrupted by user", "WARNING")
            self.log("Partial downloads have been saved")
        except Exception as e:
            self.log(f"Fatal error: {e}", "ERROR")
            raise


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Bulk download all datasets from data.cms.gov',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download all datasets to default directory (cms_data)
  python cms_bulk_download.py

  # Download to custom directory
  python cms_bulk_download.py --output-dir /path/to/data

  # Increase delay between requests (more respectful of server)
  python cms_bulk_download.py --delay 1.0

  # Resume interrupted download (already downloaded files are skipped)
  python cms_bulk_download.py
        """
    )

    parser.add_argument(
        '--output-dir', '-o',
        default='cms_data',
        help='Output directory for downloaded files (default: cms_data)'
    )

    parser.add_argument(
        '--delay', '-d',
        type=float,
        default=0.5,
        help='Delay between requests in seconds (default: 0.5)'
    )

    args = parser.parse_args()

    # Create downloader and run
    downloader = CMSBulkDownloader(
        output_dir=args.output_dir,
        delay=args.delay
    )

    downloader.run()


if __name__ == '__main__':
    main()
