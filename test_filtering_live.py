#!/usr/bin/env python3
"""
Live test of filtering with actual CMS data.
Shows how the filtering works on real datasets without downloading files.
"""

from cms_bulk_download import CMSBulkDownloader


def test_filtering_with_live_data():
    """Test filtering logic with real CMS catalog data"""
    print("=" * 80)
    print("Live Filtering Test - Examining CMS Datasets")
    print("=" * 80)
    print()

    downloader = CMSBulkDownloader(output_dir="test_data", delay=0)

    try:
        # Fetch the catalog
        catalog = downloader.fetch_catalog()
        datasets = catalog.get('dataset', [])

        if not datasets:
            print("ERROR: No datasets found")
            return False

        # Test on first 5 datasets to show filtering in action
        test_count = min(5, len(datasets))
        print(f"\nExamining first {test_count} datasets to demonstrate filtering...\n")

        for idx, dataset in enumerate(datasets[:test_count], 1):
            title = dataset.get('title', 'Unknown')
            print(f"\n[{idx}/{test_count}] Dataset: {title}")
            print("-" * 80)

            distributions = dataset.get('distribution', [])
            print(f"Total distributions: {len(distributions)}")

            if distributions:
                # Show all temporal values
                print("\nAll distributions:")
                for i, dist in enumerate(distributions, 1):
                    dist_title = dist.get('title', 'unknown')
                    temporal = dist.get('temporal', 'no date')
                    dist_format = dist.get('format', 'unknown')
                    print(f"  {i}. {dist_title} ({dist_format}) - temporal: {temporal}")

                # Filter and show results
                filtered = downloader.filter_latest_distributions(distributions)
                print(f"\n✓ Filtered to {len(filtered)} distribution(s) with latest data:")
                for i, dist in enumerate(filtered, 1):
                    dist_title = dist.get('title', 'unknown')
                    temporal = dist.get('temporal', 'no date')
                    dist_format = dist.get('format', 'unknown')
                    print(f"  {i}. {dist_title} ({dist_format}) - temporal: {temporal}")

        print("\n" + "=" * 80)
        print("✓ Filtering test complete!")
        print("=" * 80)
        return True

    except Exception as e:
        print(f"\nERROR: Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    import sys
    success = test_filtering_with_live_data()
    sys.exit(0 if success else 1)
