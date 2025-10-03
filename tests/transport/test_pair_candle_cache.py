"""Unit tests for pair candle cache utilities."""

import datetime as dt
import json
import os
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from tradingstrategy.transport.pair_candle_cache import (
    PairCandleInfo,
    PairCandleMetadata,
    PairCandleCache
)


class TestPairCandleInfo:
    """Test PairCandleInfo dataclass functionality."""

    def test_initialization_defaults(self):
        """Test that PairCandleInfo initializes with None values."""
        info = PairCandleInfo()
        assert info.start_time is None
        assert info.end_time is None

    def test_initialization_with_values(self):
        """Test PairCandleInfo initialization with specific values."""
        start = dt.datetime(2023, 1, 1)
        end = dt.datetime(2023, 1, 31)
        info = PairCandleInfo(start_time=start, end_time=end)

        assert info.start_time == start
        assert info.end_time == end

    @pytest.mark.parametrize(
        "existing_start, existing_end, new_start, new_end, expected_start, expected_end",
        [
            # First update - both None initially
            (None, None,
             dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 31),
             dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 31)),

            # Expand start time (new start is earlier)
            (dt.datetime(2023, 1, 15), dt.datetime(2023, 1, 31),
             dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 20),
             dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 31)),

            # Expand end time (new end is later)
            (dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 15),
             dt.datetime(2023, 1, 10), dt.datetime(2023, 1, 31),
             dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 31)),

            # Expand both start and end
            (dt.datetime(2023, 1, 10), dt.datetime(2023, 1, 20),
             dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 31),
             dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 31)),

            # No expansion needed (new times within existing range)
            (dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 31),
             dt.datetime(2023, 1, 10), dt.datetime(2023, 1, 20),
             dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 31)),

            # Expand start but not end
            (dt.datetime(2023, 1, 10), dt.datetime(2023, 1, 31),
             dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 20),
             dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 31)),

            # Expand end but not start
            (dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 20),
             dt.datetime(2023, 1, 10), dt.datetime(2023, 1, 31),
             dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 31)),
        ]
    )
    def test_update_time_range_expansion(
        self, existing_start, existing_end, new_start, new_end, expected_start, expected_end
    ):
        """Test update method correctly expands time ranges."""
        info = PairCandleInfo(start_time=existing_start, end_time=existing_end)
        info.update(new_start, new_end)

        assert info.start_time == expected_start
        assert info.end_time == expected_end

    def test_json_serialization_roundtrip(self):
        """Test JSON serialization and deserialization with datetime handling."""
        original = PairCandleInfo(
            start_time=dt.datetime(2023, 1, 1, 12, 30, 45),
            end_time=dt.datetime(2023, 1, 31, 23, 59, 59)
        )

        # Serialize to JSON
        json_str = original.to_json()

        # Deserialize back
        restored = PairCandleInfo.from_json(json_str)

        assert restored.start_time == original.start_time
        assert restored.end_time == original.end_time

    def test_json_serialization_with_none_values(self):
        """Test JSON serialization handles None datetime values."""
        original = PairCandleInfo()

        json_str = original.to_json()
        restored = PairCandleInfo.from_json(json_str)

        assert restored.start_time is None
        assert restored.end_time is None


class TestPairCandleMetadata:
    """Test PairCandleMetadata functionality."""

    @pytest.fixture
    def sample_metadata(self):
        """Create sample metadata for testing."""
        metadata = PairCandleMetadata()
        metadata.pairs["1"] = PairCandleInfo(
            start_time=dt.datetime(2023, 1, 1),
            end_time=dt.datetime(2023, 1, 15)
        )
        metadata.pairs["2"] = PairCandleInfo(
            start_time=dt.datetime(2023, 1, 10),
            end_time=dt.datetime(2023, 1, 31)
        )
        return metadata

    def test_initialization_empty(self):
        """Test metadata initializes with empty pairs dict."""
        metadata = PairCandleMetadata()
        assert len(metadata.pairs) == 0
        assert metadata._file_path == ""
        assert metadata._last_modified_at is None

    def test_post_init_creates_defaultdict(self):
        """Test __post_init__ converts pairs to defaultdict."""
        metadata = PairCandleMetadata()

        # Accessing non-existent key should create new PairCandleInfo
        new_info = metadata.pairs["999"]
        assert isinstance(new_info, PairCandleInfo)
        assert new_info.start_time is None
        assert new_info.end_time is None

    def test_load_nonexistent_file(self, tmp_path):
        """Test loading metadata from non-existent file creates empty metadata."""
        nonexistent_file = str(tmp_path / "nonexistent.json")

        metadata = PairCandleMetadata.load(nonexistent_file)

        assert len(metadata.pairs) == 0
        assert metadata._file_path == nonexistent_file
        assert metadata._last_modified_at is None

    def test_load_existing_file(self, tmp_path):
        """Test loading metadata from existing file."""
        # Create test metadata file
        test_data = {
            "pairs": {
                "1": {
                    "start_time": "2023-01-01T00:00:00",
                    "end_time": "2023-01-15T23:59:59"
                }
            }
        }

        metadata_file = tmp_path / "test_metadata.json"
        metadata_file.write_text(json.dumps(test_data))

        # Load metadata
        metadata = PairCandleMetadata.load(str(metadata_file))

        assert len(metadata.pairs) == 1
        assert "1" in metadata.pairs
        assert metadata.pairs["1"].start_time == dt.datetime(2023, 1, 1)
        assert metadata.pairs["1"].end_time == dt.datetime(2023, 1, 15, 23, 59, 59)
        assert metadata._file_path == str(metadata_file)
        assert metadata._last_modified_at is not None

    def test_save_without_file_path_raises_error(self):
        """Test save raises ValueError when no file path is set."""
        metadata = PairCandleMetadata()

        with pytest.raises(ValueError, match="Cannot save: no file path set"):
            metadata.save()

    def test_save_creates_file_and_updates_modified_time(self, tmp_path):
        """Test save creates file and updates last_modified_at."""
        metadata_file = tmp_path / "test_save.json"

        metadata = PairCandleMetadata()
        metadata._file_path = str(metadata_file)
        metadata.pairs["1"] = PairCandleInfo(
            start_time=dt.datetime(2023, 1, 1),
            end_time=dt.datetime(2023, 1, 15)
        )

        # Save should create file
        metadata.save()

        assert metadata_file.exists()
        assert metadata._last_modified_at is not None

        # Verify file content
        saved_data = json.loads(metadata_file.read_text())
        assert "pairs" in saved_data
        assert "1" in saved_data["pairs"]

    def test_latest_end_time_empty_metadata(self):
        """Test latest_end_time returns None for empty metadata."""
        metadata = PairCandleMetadata()
        assert metadata.latest_end_time() is None

    def test_latest_end_time_single_pair(self):
        """Test latest_end_time with single pair."""
        metadata = PairCandleMetadata()
        end_time = dt.datetime(2023, 1, 31)
        metadata.pairs["1"] = PairCandleInfo(
            start_time=dt.datetime(2023, 1, 1),
            end_time=end_time
        )

        assert metadata.latest_end_time() == end_time

    def test_latest_end_time_multiple_pairs(self, sample_metadata):
        """Test latest_end_time returns maximum across all pairs."""
        latest = sample_metadata.latest_end_time()
        # Should be 2023-01-31 from pair "2"
        assert latest == dt.datetime(2023, 1, 31)

    def test_latest_end_time_ignores_none_values(self):
        """Test latest_end_time ignores pairs with None end_time."""
        metadata = PairCandleMetadata()
        metadata.pairs["1"] = PairCandleInfo(end_time=None)
        metadata.pairs["2"] = PairCandleInfo(end_time=dt.datetime(2023, 1, 15))

        assert metadata.latest_end_time() == dt.datetime(2023, 1, 15)

    def test_update_new_pairs(self):
        """Test update method with new pair IDs."""
        metadata = PairCandleMetadata()
        start_time = dt.datetime(2023, 1, 1)
        end_time = dt.datetime(2023, 1, 31)

        metadata.update([100, 200], start_time, end_time)

        assert len(metadata.pairs) == 2
        assert metadata.pairs["100"].start_time == start_time
        assert metadata.pairs["100"].end_time == end_time
        assert metadata.pairs["200"].start_time == start_time
        assert metadata.pairs["200"].end_time == end_time

    def test_update_existing_pairs(self, sample_metadata):
        """Test update method expands existing pair ranges."""
        # Update pair "1" with expanded range
        new_start = dt.datetime(2022, 12, 1)  # Earlier than existing
        new_end = dt.datetime(2023, 2, 1)     # Later than existing

        sample_metadata.update([1], new_start, new_end)

        # Should have expanded the range
        assert sample_metadata.pairs["1"].start_time == new_start
        assert sample_metadata.pairs["1"].end_time == new_end

    @pytest.mark.parametrize(
        "pair_data, request_start, request_end, expected_full, expected_delta",
        [
            # Scenario 1: New pair (never cached)
            ({},
             dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 31),
             {100}, set()),

            # Scenario 2: Need data before cached range
            ({"100": PairCandleInfo(dt.datetime(2023, 1, 15), dt.datetime(2023, 1, 31))},
             dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 20),
             {100}, set()),

            # Scenario 3: All requested data already cached
            ({"100": PairCandleInfo(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 31))},
             dt.datetime(2023, 1, 10), dt.datetime(2023, 1, 20),
             set(), set()),

            # Scenario 4: Need only recent delta
            ({"100": PairCandleInfo(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 20))},
             dt.datetime(2023, 1, 10), dt.datetime(2023, 1, 31),
             set(), {100}),

        ]
    )
    def test_partition_for_fetch_scenarios(
        self, pair_data, request_start, request_end, expected_full, expected_delta
    ):
        """Test partition_for_fetch logic for various scenarios."""
        metadata = PairCandleMetadata()

        # Set up pair data
        for pair_id, pair_info in pair_data.items():
            metadata.pairs[pair_id] = pair_info

        # Partition
        partition = metadata.partition_for_fetch([100], request_start, request_end)

        assert partition.full_fetch_ids == expected_full
        assert partition.delta_fetch_ids == expected_delta

    def test_partition_for_fetch_gap_scenario(self):
        """Test partition_for_fetch with gap between cached data and latest."""
        metadata = PairCandleMetadata()

        # Pair 100: cached until Jan 15
        # Pair 200: cached until Jan 31 (this is the latest)
        metadata.pairs["100"] = PairCandleInfo(
            dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 15)
        )
        metadata.pairs["200"] = PairCandleInfo(
            dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 31)
        )

        # Request data for pair 100 until Feb 15
        # Since there's a gap between Jan 15 and Jan 31 (latest), need full fetch
        partition = metadata.partition_for_fetch(
            [100], dt.datetime(2023, 1, 10), dt.datetime(2023, 2, 15)
        )

        assert partition.full_fetch_ids == {100}
        assert partition.delta_fetch_ids == set()

    def test_partition_for_fetch_mixed_scenarios(self):
        """Test partition_for_fetch with mixed pair requirements."""
        metadata = PairCandleMetadata()

        # Pair 100: well cached, needs delta
        metadata.pairs["100"] = PairCandleInfo(
            dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 31)
        )

        # Pair 200: new pair
        # (pair 200 not in metadata)

        # Pair 300: has gap, needs full fetch
        metadata.pairs["300"] = PairCandleInfo(
            dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 15)
        )

        partition = metadata.partition_for_fetch(
            [100, 200, 300], dt.datetime(2023, 1, 20), dt.datetime(2023, 2, 15)
        )

        assert partition.full_fetch_ids == {200, 300}  # New + gap
        assert partition.delta_fetch_ids == {100}     # Delta only

    def test_delta_fetch_start_time_no_save(self):
        """Test delta_fetch_start_time returns None if never saved."""
        metadata = PairCandleMetadata()
        assert metadata.delta_fetch_start_time() is None

    def test_delta_fetch_start_time_with_modification_time(self, tmp_path):
        """Test delta_fetch_start_time calculation with modification time."""
        # Create and save metadata to get modification time
        metadata_file = tmp_path / "test.json"
        metadata = PairCandleMetadata()
        metadata._file_path = str(metadata_file)

        # Add some data and save
        metadata.pairs["1"] = PairCandleInfo(
            dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 31)
        )
        metadata.save()

        # Test calculation
        lookback_hours = 24
        result = metadata.delta_fetch_start_time(lookback_hours)

        assert result is not None

        # Should be the earlier of:
        # - modification_time - lookback
        # - latest_end_time
        expected_freshness = metadata.last_modified_at - dt.timedelta(hours=lookback_hours)
        expected_latest = dt.datetime(2023, 1, 31)
        expected = min(expected_freshness, expected_latest)

        assert result == expected

    def test_delta_fetch_start_time_no_pairs_data(self, tmp_path):
        """Test delta_fetch_start_time when metadata has no pairs."""
        metadata_file = tmp_path / "empty.json"
        metadata = PairCandleMetadata()
        metadata._file_path = str(metadata_file)
        metadata.save()  # Save empty metadata to get modification time

        lookback_hours = 48
        result = metadata.delta_fetch_start_time(lookback_hours)

        # Should return freshness cutoff since no latest_end_time
        expected = metadata.last_modified_at - dt.timedelta(hours=lookback_hours)
        assert result == expected

    def test_json_serialization_excludes_private_fields(self):
        """Test that private fields are excluded from JSON serialization."""
        metadata = PairCandleMetadata()
        metadata._file_path = "/some/path"
        metadata._last_modified_at = dt.datetime.now()

        json_str = metadata.to_json()
        json_data = json.loads(json_str)

        # Private fields should not be in JSON
        assert "_file_path" not in json_data
        assert "_last_modified_at" not in json_data
        assert "pairs" in json_data

    def test_json_roundtrip_with_complex_data(self):
        """Test JSON serialization roundtrip with complex metadata."""
        original = PairCandleMetadata()
        original.pairs["1"] = PairCandleInfo(
            dt.datetime(2023, 1, 1, 12, 30), dt.datetime(2023, 1, 15, 18, 45)
        )
        original.pairs["2"] = PairCandleInfo(
            dt.datetime(2023, 1, 10), dt.datetime(2023, 1, 31)
        )
        original.pairs["3"] = PairCandleInfo()  # None values

        json_str = original.to_json()
        restored = PairCandleMetadata.from_json(json_str)

        # Check all pairs restored correctly
        assert len(restored.pairs) == 3
        assert restored.pairs["1"].start_time == dt.datetime(2023, 1, 1, 12, 30)
        assert restored.pairs["1"].end_time == dt.datetime(2023, 1, 15, 18, 45)
        assert restored.pairs["2"].start_time == dt.datetime(2023, 1, 10)
        assert restored.pairs["2"].end_time == dt.datetime(2023, 1, 31)
        assert restored.pairs["3"].start_time is None
        assert restored.pairs["3"].end_time is None

        # Private fields should remain unset
        assert restored._file_path == ""
        assert restored._last_modified_at is None


class TestPairCandleCache:
    """Test PairCandleCache context manager functionality."""

    @pytest.fixture
    def sample_candle_df(self):
        """Create sample candle DataFrame for testing."""
        return pd.DataFrame({
            "timestamp": pd.to_datetime([
                "2023-01-01 00:00:00", "2023-01-01 01:00:00",
                "2023-01-02 00:00:00", "2023-01-02 01:00:00"
            ]),
            "pair_id": [1, 1, 2, 2],
            "open": [100.0, 101.0, 200.0, 201.0],
            "high": [102.0, 103.0, 202.0, 203.0],
            "low": [99.0, 100.0, 199.0, 200.0],
            "close": [101.0, 102.0, 201.0, 202.0],
            "volume": [1000.0, 1100.0, 2000.0, 2100.0]
        })

    @pytest.fixture
    def sample_parquet_file(self, tmp_path, sample_candle_df):
        """Create sample parquet file for testing."""
        parquet_file = tmp_path / "test_cache.parquet"
        sample_candle_df.to_parquet(parquet_file, index=False)
        return parquet_file

    def test_initialization_paths(self, tmp_path):
        """Test PairCandleCache initialization sets correct file paths."""
        base_path = str(tmp_path / "test_cache")
        cache = PairCandleCache(base_path)

        assert cache.base_path == base_path
        assert cache.parquet_path == f"{base_path}.parquet"
        assert cache.metadata_path == f"{base_path}.json"
        assert cache._lock_context is None
        assert cache._data is None
        assert cache._metadata is None

    def test_context_manager_initialization(self, tmp_path):
        """Test context manager properly initializes and provides access to data/metadata."""
        base_path = str(tmp_path / "empty_cache")

        with PairCandleCache(base_path) as cache:
            # Should have initialized data and metadata
            assert cache._data is not None
            assert cache._metadata is not None
            assert isinstance(cache.data, pd.DataFrame)
            assert isinstance(cache.metadata, PairCandleMetadata)

            # Empty cache should have empty DataFrame
            assert len(cache.data) == 0

    @patch('tradingstrategy.transport.pair_candle_cache.wait_other_writers')
    def test_context_manager_file_locking(self, mock_wait_writers, tmp_path):
        """Test context manager properly handles file locking."""
        mock_lock = Mock()
        mock_lock.__enter__ = Mock(return_value=mock_lock)
        mock_lock.__exit__ = Mock(return_value=None)
        mock_wait_writers.return_value = mock_lock

        base_path = str(tmp_path / "test_cache")

        with PairCandleCache(base_path) as cache:
            # Should have called wait_other_writers and entered context
            mock_wait_writers.assert_called_once_with(base_path)
            mock_lock.__enter__.assert_called_once()

        # Should have exited context on exit
        mock_lock.__exit__.assert_called_once()

    def test_load_existing_parquet_data_with_timestamp_index(self, tmp_path, sample_candle_df):
        """Test loading existing parquet data correctly sets timestamp index."""
        base_path = str(tmp_path / "existing_cache")
        parquet_path = f"{base_path}.parquet"

        # Save sample data without index (as the cache does)
        sample_candle_df.to_parquet(parquet_path, index=False)

        with PairCandleCache(base_path) as cache:
            # Verify data was loaded
            assert len(cache.data) == 4

            # Verify timestamp index is properly set
            assert cache.data.index.name == "timestamp"
            assert "timestamp" in cache.data.columns  # Should keep column too (drop=False)

            # Verify data integrity
            assert list(cache.data["pair_id"]) == [1, 1, 2, 2]
            assert cache.data.loc[pd.Timestamp("2023-01-01 00:00:00"), "open"] == 100.0

    def test_load_nonexistent_parquet_creates_empty_dataframe(self, tmp_path):
        """Test loading from non-existent parquet creates empty DataFrame."""
        base_path = str(tmp_path / "nonexistent_cache")

        with PairCandleCache(base_path) as cache:
            assert isinstance(cache.data, pd.DataFrame)
            assert len(cache.data) == 0

    def test_load_existing_metadata(self, tmp_path):
        """Test loading existing metadata file."""
        base_path = str(tmp_path / "existing_metadata_cache")
        metadata_path = f"{base_path}.json"

        # Create test metadata file
        test_metadata = {
            "pairs": {
                "1": {
                    "start_time": "2023-01-01T00:00:00",
                    "end_time": "2023-01-31T23:59:59"
                }
            }
        }
        with open(metadata_path, "w") as f:
            json.dump(test_metadata, f)

        with PairCandleCache(base_path) as cache:
            assert len(cache.metadata.pairs) == 1
            assert "1" in cache.metadata.pairs
            assert cache.metadata.pairs["1"].start_time == dt.datetime(2023, 1, 1)

    def test_update_with_new_dataframes(self, tmp_path, sample_candle_df):
        """Test update method adds new data and maintains timestamp index."""
        base_path = str(tmp_path / "update_cache")

        # Create new data to add
        new_df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2023-01-03 00:00:00", "2023-01-03 01:00:00"]),
            "pair_id": [1, 2],
            "open": [105.0, 205.0],
            "high": [106.0, 206.0],
            "low": [104.0, 204.0],
            "close": [105.0, 205.0],
            "volume": [1200.0, 2200.0]
        })

        with PairCandleCache(base_path) as cache:
            # Initial state - empty
            assert len(cache.data) == 0

            # Update with sample data
            cache.update(
                [sample_candle_df],
                [1, 2],
                dt.datetime(2023, 1, 1),
                dt.datetime(2023, 1, 2, 1)
            )

            # Should have 4 rows
            assert len(cache.data) == 4

            # Verify timestamp index is maintained after update
            assert cache.data.index.name == "timestamp"
            assert "timestamp" in cache.data.columns

            # Update with additional data
            cache.update(
                [new_df],
                [1, 2],
                dt.datetime(2023, 1, 3),
                dt.datetime(2023, 1, 3, 1)
            )

            # Should have 6 rows total
            assert len(cache.data) == 6

            # Verify index still correct after second update
            assert cache.data.index.name == "timestamp"
            assert "timestamp" in cache.data.columns

            # Verify data is sorted by pair_id, then timestamp
            # Expected order: pair 1 data first, then pair 2 data
            expected_order = [
                pd.Timestamp("2023-01-01 00:00:00"),  # pair 1
                pd.Timestamp("2023-01-01 01:00:00"),  # pair 1
                pd.Timestamp("2023-01-03 00:00:00"),  # pair 1 (new data)
                pd.Timestamp("2023-01-02 00:00:00"),  # pair 2
                pd.Timestamp("2023-01-02 01:00:00"),  # pair 2
                pd.Timestamp("2023-01-03 01:00:00")   # pair 2 (new data)
            ]
            timestamps = cache.data["timestamp"].tolist()
            assert timestamps == expected_order

    def test_update_deduplication_logic(self, tmp_path):
        """Test update method properly deduplicates data."""
        base_path = str(tmp_path / "dedup_cache")

        # Create original data
        original_df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2023-01-01 00:00:00"]),
            "pair_id": [1],
            "open": [100.0],
            "high": [102.0],
            "low": [99.0],
            "close": [101.0],
            "volume": [1000.0]
        })

        # Create duplicate with different values (should replace original)
        duplicate_df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2023-01-01 00:00:00"]),
            "pair_id": [1],
            "open": [100.5],  # Different value
            "high": [102.5],
            "low": [99.5],
            "close": [101.5],
            "volume": [1050.0]
        })

        with PairCandleCache(base_path) as cache:
            # Add original data
            cache.update([original_df], [1], dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 1))
            assert len(cache.data) == 1
            assert cache.data.iloc[0]["open"] == 100.0

            # Add duplicate (should replace with keep="last")
            cache.update([duplicate_df], [1], dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 1))
            assert len(cache.data) == 1  # Still just one row
            assert cache.data.iloc[0]["open"] == 100.5  # Updated value

    def test_update_empty_dataframes_list(self, tmp_path):
        """Test update with empty dataframes list only updates metadata."""
        base_path = str(tmp_path / "empty_update_cache")

        with PairCandleCache(base_path) as cache:
            initial_len = len(cache.data)

            # Update with empty list of dataframes
            cache.update(
                [],  # Empty list
                [1, 2],
                dt.datetime(2023, 1, 1),
                dt.datetime(2023, 1, 31)
            )

            # Data should be unchanged
            assert len(cache.data) == initial_len

            # Metadata should be updated
            assert len(cache.metadata.pairs) == 2
            assert cache.metadata.pairs["1"].start_time == dt.datetime(2023, 1, 1)
            assert cache.metadata.pairs["2"].end_time == dt.datetime(2023, 1, 31)

    def test_data_property_outside_context_manager_raises_assertion(self, tmp_path):
        """Test accessing data property outside context manager raises AssertionError."""
        base_path = str(tmp_path / "test_cache")
        cache = PairCandleCache(base_path)

        with pytest.raises(AssertionError, match="Cache not properly initialized"):
            _ = cache.data

    def test_metadata_property_outside_context_manager_raises_assertion(self, tmp_path):
        """Test accessing metadata property outside context manager raises AssertionError."""
        base_path = str(tmp_path / "test_cache")
        cache = PairCandleCache(base_path)

        with pytest.raises(AssertionError, match="Cache not properly initialized"):
            _ = cache.metadata

    def test_save_data_creates_parquet_file(self, tmp_path, sample_candle_df):
        """Test _save_data creates parquet file correctly."""
        base_path = str(tmp_path / "save_test_cache")
        parquet_path = f"{base_path}.parquet"

        with PairCandleCache(base_path) as cache:
            # Add some data
            cache.update(
                [sample_candle_df],
                [1, 2],
                dt.datetime(2023, 1, 1),
                dt.datetime(2023, 1, 2, 1)
            )

        # File should exist after context exit
        assert os.path.exists(parquet_path)

        # Verify we can load it back and it has correct structure
        loaded_df = pd.read_parquet(parquet_path)
        assert len(loaded_df) == 4
        assert "timestamp" in loaded_df.columns
        assert list(loaded_df.columns) == list(sample_candle_df.columns)

    def test_full_workflow_integration(self, tmp_path):
        """Test complete workflow: create cache, add data, close, reopen, verify persistence."""
        base_path = str(tmp_path / "workflow_cache")

        # Create initial data
        initial_df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2023-01-01 00:00:00", "2023-01-01 01:00:00"]),
            "pair_id": [1, 1],
            "open": [100.0, 101.0],
            "high": [102.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 102.0],
            "volume": [1000.0, 1100.0]
        })

        # Phase 1: Create cache and add initial data
        with PairCandleCache(base_path) as cache:
            cache.update([initial_df], [1], dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 1, 1))

            assert len(cache.data) == 2
            assert cache.data.index.name == "timestamp"
            assert len(cache.metadata.pairs) == 1

        # Phase 2: Reopen cache and verify persistence
        with PairCandleCache(base_path) as cache:
            # Data should be loaded from parquet
            assert len(cache.data) == 2
            assert cache.data.index.name == "timestamp"  # CRITICAL: Index preserved
            assert "timestamp" in cache.data.columns

            # Metadata should be loaded from JSON
            assert len(cache.metadata.pairs) == 1
            assert cache.metadata.pairs["1"].start_time == dt.datetime(2023, 1, 1)

            # Add more data
            additional_df = pd.DataFrame({
                "timestamp": pd.to_datetime(["2023-01-02 00:00:00"]),
                "pair_id": [2],
                "open": [200.0],
                "high": [202.0],
                "low": [199.0],
                "close": [201.0],
                "volume": [2000.0]
            })

            cache.update([additional_df], [2], dt.datetime(2023, 1, 2), dt.datetime(2023, 1, 2))

            assert len(cache.data) == 3
            assert len(cache.metadata.pairs) == 2

        # Phase 3: Final verification
        with PairCandleCache(base_path) as cache:
            assert len(cache.data) == 3
            assert cache.data.index.name == "timestamp"
            assert len(cache.metadata.pairs) == 2

            # Verify data integrity
            pair_1_data = cache.data[cache.data["pair_id"] == 1]
            pair_2_data = cache.data[cache.data["pair_id"] == 2]
            assert len(pair_1_data) == 2
            assert len(pair_2_data) == 1

    def test_data_sorting_after_update(self, tmp_path):
        """Test that data is properly sorted after updates."""
        base_path = str(tmp_path / "sorting_cache")

        # Create data in non-chronological order
        out_of_order_df = pd.DataFrame({
            "timestamp": pd.to_datetime([
                "2023-01-03 00:00:00",  # Later date first
                "2023-01-01 00:00:00",  # Earlier date
                "2023-01-02 00:00:00"   # Middle date
            ]),
            "pair_id": [1, 1, 1],
            "open": [103.0, 101.0, 102.0],
            "high": [104.0, 102.0, 103.0],
            "low": [102.0, 100.0, 101.0],
            "close": [103.0, 101.0, 102.0],
            "volume": [1300.0, 1100.0, 1200.0]
        })

        with PairCandleCache(base_path) as cache:
            cache.update([out_of_order_df], [1], dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 3))

            # Verify data is sorted by pair_id, then timestamp
            timestamps = cache.data["timestamp"].tolist()
            expected_timestamps = pd.to_datetime([
                "2023-01-01 00:00:00",
                "2023-01-02 00:00:00",
                "2023-01-03 00:00:00"
            ]).tolist()

            assert timestamps == expected_timestamps

            # Verify corresponding values are in correct order
            opens = cache.data["open"].tolist()
            assert opens == [101.0, 102.0, 103.0]
