#!/usr/bin/env python3
"""
Unit tests for the latest data filtering functionality.
"""

import unittest
from cms_bulk_download import CMSBulkDownloader


class TestLatestDataFiltering(unittest.TestCase):
    """Test cases for filtering to latest data"""

    def setUp(self):
        """Set up test fixtures"""
        self.downloader = CMSBulkDownloader(output_dir="test_data", delay=0)

    def test_extract_latest_date_single_date(self):
        """Test extracting a single date"""
        result = self.downloader.extract_latest_date("2024-01-15")
        self.assertEqual(result, "2024-01-15")

    def test_extract_latest_date_range(self):
        """Test extracting the latest date from a range"""
        result = self.downloader.extract_latest_date("2023-01-01 to 2024-12-31")
        self.assertEqual(result, "2024-12-31")

    def test_extract_latest_date_multiple_dates(self):
        """Test extracting the latest from multiple dates"""
        result = self.downloader.extract_latest_date("2023-06-01, 2024-01-01, 2023-12-31")
        self.assertEqual(result, "2024-01-01")

    def test_extract_latest_date_year_only(self):
        """Test extracting year when no full date available"""
        result = self.downloader.extract_latest_date("2024")
        self.assertEqual(result, "2024")

    def test_extract_latest_date_empty(self):
        """Test handling empty temporal field"""
        result = self.downloader.extract_latest_date("")
        self.assertIsNone(result)

    def test_extract_latest_date_none(self):
        """Test handling None temporal field"""
        result = self.downloader.extract_latest_date(None)
        self.assertIsNone(result)

    def test_filter_latest_distributions_simple(self):
        """Test filtering with clear latest date"""
        distributions = [
            {'title': 'Old Data', 'temporal': '2023-01-01', 'format': 'CSV'},
            {'title': 'Latest Data', 'temporal': '2024-01-01', 'format': 'CSV'},
            {'title': 'Middle Data', 'temporal': '2023-06-01', 'format': 'CSV'},
        ]
        result = self.downloader.filter_latest_distributions(distributions)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['title'], 'Latest Data')

    def test_filter_latest_distributions_multiple_formats(self):
        """Test that all formats for the latest date are included"""
        distributions = [
            {'title': 'Old CSV', 'temporal': '2023-01-01', 'format': 'CSV'},
            {'title': 'Latest CSV', 'temporal': '2024-01-01', 'format': 'CSV'},
            {'title': 'Latest API', 'temporal': '2024-01-01', 'format': 'API'},
            {'title': 'Latest ZIP', 'temporal': '2024-01-01', 'format': 'ZIP'},
        ]
        result = self.downloader.filter_latest_distributions(distributions)
        self.assertEqual(len(result), 3)
        titles = [d['title'] for d in result]
        self.assertIn('Latest CSV', titles)
        self.assertIn('Latest API', titles)
        self.assertIn('Latest ZIP', titles)

    def test_filter_latest_distributions_date_ranges(self):
        """Test filtering with date ranges"""
        distributions = [
            {'title': 'Q1 2023', 'temporal': '2023-01-01 to 2023-03-31', 'format': 'CSV'},
            {'title': 'Q2 2024', 'temporal': '2024-04-01 to 2024-06-30', 'format': 'CSV'},
            {'title': 'Q3 2024', 'temporal': '2024-07-01 to 2024-09-30', 'format': 'CSV'},
        ]
        result = self.downloader.filter_latest_distributions(distributions)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['title'], 'Q3 2024')

    def test_filter_latest_distributions_no_temporal(self):
        """Test that distributions without temporal info are all returned"""
        distributions = [
            {'title': 'Data 1', 'temporal': '', 'format': 'CSV'},
            {'title': 'Data 2', 'temporal': '', 'format': 'API'},
            {'title': 'Data 3', 'format': 'ZIP'},
        ]
        result = self.downloader.filter_latest_distributions(distributions)
        # Should return all distributions when no temporal info available
        self.assertEqual(len(result), 3)

    def test_filter_latest_distributions_mixed_temporal(self):
        """Test filtering with mix of temporal and non-temporal distributions"""
        distributions = [
            {'title': 'Dated Old', 'temporal': '2023-01-01', 'format': 'CSV'},
            {'title': 'Dated New', 'temporal': '2024-01-01', 'format': 'CSV'},
            {'title': 'No Date', 'temporal': '', 'format': 'API'},
        ]
        result = self.downloader.filter_latest_distributions(distributions)
        # Should return only the one with latest date
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['title'], 'Dated New')

    def test_filter_latest_distributions_empty_list(self):
        """Test filtering empty distribution list"""
        result = self.downloader.filter_latest_distributions([])
        self.assertEqual(result, [])

    def test_filter_latest_distributions_year_comparison(self):
        """Test filtering with year-only temporal values"""
        distributions = [
            {'title': '2022 Data', 'temporal': '2022', 'format': 'CSV'},
            {'title': '2023 Data', 'temporal': '2023', 'format': 'CSV'},
            {'title': '2024 Data', 'temporal': '2024', 'format': 'CSV'},
        ]
        result = self.downloader.filter_latest_distributions(distributions)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['title'], '2024 Data')

    def test_real_world_scenario(self):
        """Test a realistic scenario with multiple versions and formats"""
        distributions = [
            # 2023 data in multiple formats
            {'title': 'Hospital Data 2023', 'temporal': '2023-01-01 to 2023-12-31', 'format': 'CSV'},
            {'title': 'Hospital Data 2023', 'temporal': '2023-01-01 to 2023-12-31', 'format': 'API'},
            # Q1 2024 data
            {'title': 'Hospital Data Q1 2024', 'temporal': '2024-01-01 to 2024-03-31', 'format': 'CSV'},
            # Q2 2024 data
            {'title': 'Hospital Data Q2 2024', 'temporal': '2024-04-01 to 2024-06-30', 'format': 'CSV'},
            # Full 2024 data (latest)
            {'title': 'Hospital Data 2024', 'temporal': '2024-01-01 to 2024-12-31', 'format': 'CSV'},
            {'title': 'Hospital Data 2024', 'temporal': '2024-01-01 to 2024-12-31', 'format': 'ZIP'},
            {'title': 'Hospital Data 2024', 'temporal': '2024-01-01 to 2024-12-31', 'format': 'API'},
        ]
        result = self.downloader.filter_latest_distributions(distributions)
        # Should get all 3 formats for the full 2024 data (ends 2024-12-31)
        self.assertEqual(len(result), 3)
        for dist in result:
            self.assertEqual(dist['temporal'], '2024-01-01 to 2024-12-31')


def run_tests():
    """Run all tests and print results"""
    print("=" * 80)
    print("Testing Latest Data Filtering Functionality")
    print("=" * 80)
    print()

    # Create test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestLatestDataFiltering)

    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Print summary
    print()
    print("=" * 80)
    if result.wasSuccessful():
        print("✓ All tests passed!")
    else:
        print(f"✗ Tests failed: {len(result.failures)} failures, {len(result.errors)} errors")
    print("=" * 80)

    return result.wasSuccessful()


if __name__ == '__main__':
    import sys
    success = run_tests()
    sys.exit(0 if success else 1)
