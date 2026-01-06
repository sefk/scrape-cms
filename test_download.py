#!/usr/bin/env python3
"""
Test script for CMS bulk downloader.
Downloads only the first 2 datasets to verify functionality.
"""

import json
import sys
from pathlib import Path
from cms_bulk_download import CMSBulkDownloader


def test_download():
    """Test download with a small subset of datasets"""
    print("CMS Bulk Downloader - Test Mode")
    print("=" * 80)
    print("This will download only the first 2 datasets to verify functionality")
    print("=" * 80)
    print()

    # Create test downloader
    downloader = CMSBulkDownloader(
        output_dir="cms_data_test",
        delay=0.5
    )

    try:
        # Fetch catalog
        catalog = downloader.fetch_catalog()
        datasets = catalog.get('dataset', [])

        if not datasets:
            print("ERROR: No datasets found in catalog!")
            return False

        # Process only first 2 datasets
        test_count = min(2, len(datasets))
        print(f"\nTesting with {test_count} datasets (out of {len(datasets)} total)\n")

        for idx, dataset in enumerate(datasets[:test_count], 1):
            print(f"\n[{idx}/{test_count}] Testing dataset {idx} of {test_count}")
            downloader.process_dataset(dataset)

        # Print results
        print(f"\n{'=' * 80}")
        print("Test complete!")
        print(f"{'=' * 80}")
        print(f"Datasets processed: {downloader.stats['datasets_processed']}")
        print(f"Files downloaded: {downloader.stats['files_downloaded']}")
        print(f"Files skipped: {downloader.stats['files_skipped']}")
        print(f"Errors: {downloader.stats['errors']}")
        print(f"Total data: {downloader.stats['total_bytes']:,} bytes ({downloader.stats['total_bytes'] / 1024 / 1024:.2f} MB)")

        if downloader.stats['errors'] == 0:
            print("\n✓ Test passed! The downloader is working correctly.")
            print("  Run 'python cms_bulk_download.py' to download all datasets.")
            return True
        else:
            print(f"\n⚠ Test completed with {downloader.stats['errors']} errors.")
            print("  Check the logs above for details.")
            return False

    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        return False
    except Exception as e:
        print(f"\nERROR: Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = test_download()
    sys.exit(0 if success else 1)
