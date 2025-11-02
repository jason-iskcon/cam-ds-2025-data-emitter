"""Tests for ULBEventGenerator and _get_column_value."""
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from emitter.replay_ulb_stdout import ULBEventGenerator, _get_column_value


@pytest.fixture
def temp_csv(tmp_path):
    """Fixture to create a temporary CSV file with given data, automatically cleaned up."""
    import uuid
    def _create_csv(data: dict) -> Path:
        """Create temporary CSV file from dict data."""
        df = pd.DataFrame(data)
        # Use UUID to ensure unique filename per test
        csv_path = tmp_path / f"test_{uuid.uuid4().hex[:8]}.csv"
        df.to_csv(csv_path, index=False)
        return csv_path
    
    return _create_csv


class TestGetColumnValue:
    """Test _get_column_value helper function."""
    
    def test_primary_column_exists(self):
        """Test returns primary column value when it exists."""
        row = pd.Series({"CustomerID": "c123", "Amount": 50.0})
        assert _get_column_value(row, "CustomerID") == "c123"
        assert _get_column_value(row, "Amount") == 50.0
    
    def test_fallback_column_used(self):
        """Test uses fallback column when primary missing."""
        row = pd.Series({"amount": 50.0, "label": 1})
        assert _get_column_value(row, "Amount", "amount") == 50.0
        assert _get_column_value(row, "Class", "label") == 1
    
    def test_default_when_both_missing(self):
        """Test returns default when both primary and fallback missing."""
        row = pd.Series({"other_col": "value"})
        assert _get_column_value(row, "CustomerID", "", None) is None
        assert _get_column_value(row, "Amount", "amount", 0.0) == 0.0
    
    def test_fallback_empty_string_ignored(self):
        """Test fallback empty string is ignored."""
        row = pd.Series({"Amount": 50.0})
        # Fallback is empty string, so it won't match
        assert _get_column_value(row, "Amount", "") == 50.0
        # Primary missing, fallback empty, returns default
        assert _get_column_value(row, "Missing", "", 0.0) == 0.0


class TestULBEventGenerator:
    """Test ULBEventGenerator functionality."""
    
    def test_init_loads_data(self, temp_csv):
        """Test generator loads CSV data on initialization."""
        csv_data = {
            "CustomerID": ["c1", "c2"],
            "Amount": [100.0, 200.0],
            "Class": [0, 1],
            "V1": [0.5, -0.2]
        }
        csv_path = temp_csv(csv_data)
        generator = ULBEventGenerator(str(csv_path))
        assert generator.df is not None
        assert len(generator.df) == 2
        assert "CustomerID" in generator.df.columns
    
    def test_generate_event_basic(self, temp_csv):
        """Test basic event generation from CSV row."""
        csv_data = {
            "CustomerID": ["c123"],
            "Amount": [150.0],
            "Class": [1],
            "V1": [0.5],
            "V2": [-0.3]
        }
        csv_path = temp_csv(csv_data)
        generator = ULBEventGenerator(str(csv_path))
        event = generator(0, 1000)
        
        assert event.tx_id == "ulb_0"
        assert event.customer_id == "c123"
        assert event.amount == 150.0
        assert event.merchant_cat == "unknown"
        assert event.ts == 1000  # base_ts + event_num
        assert event.label == 1
    
    def test_row_wrapping(self, temp_csv):
        """Test event_num wraps around using modulo."""
        csv_data = {
            "CustomerID": ["c1", "c2", "c3"],
            "Amount": [100.0, 200.0, 300.0],
            "Class": [0, 0, 1],
            "V1": [0.1, 0.2, 0.3]
        }
        csv_path = temp_csv(csv_data)
        generator = ULBEventGenerator(str(csv_path))
        
        # Test wrapping: event_num % len(df)
        assert generator(0, 1000).customer_id == "c1"
        assert generator(1, 1000).customer_id == "c2"
        assert generator(2, 1000).customer_id == "c3"
        assert generator(3, 1000).customer_id == "c1"  # Wraps around
        assert generator(4, 1000).customer_id == "c2"
        assert generator(6, 1000).customer_id == "c1"  # 6 % 3 = 0
    
    def test_features_dict_excludes_special_columns(self, temp_csv):
        """Test features dict excludes CustomerID, Amount, Class."""
        csv_data = {
            "CustomerID": ["c1"],
            "Amount": [100.0],
            "Class": [0],
            "V1": [0.5],
            "V2": [-0.2],
            "Time": [100]
        }
        csv_path = temp_csv(csv_data)
        generator = ULBEventGenerator(str(csv_path))
        event = generator(0, 1000)
        
        # Should include V1, V2, Time
        assert "V1" in event.features
        assert "V2" in event.features
        assert "Time" in event.features
        
        # Should NOT include excluded columns
        assert "CustomerID" not in event.features
        assert "Amount" not in event.features
        assert "Class" not in event.features
        assert "amount" not in event.features  # Also excluded
        assert "label" not in event.features  # Also excluded
    
    def test_numpy_type_conversion(self, temp_csv):
        """Test numpy types converted to Python native types."""
        csv_data = {
            "CustomerID": ["c1"],
            "Amount": [np.float64(100.0)],  # numpy type
            "Class": [np.int64(1)],  # numpy type
            "V1": [np.float32(0.5)],  # numpy type
            "V2": [np.int32(42)]  # numpy int
        }
        csv_path = temp_csv(csv_data)
        generator = ULBEventGenerator(str(csv_path))
        event = generator(0, 1000)
        
        # Check numpy types converted to native Python
        assert isinstance(event.features["V1"], float)
        assert not isinstance(event.features["V1"], np.floating)  # Should be native Python
        assert isinstance(event.features["V2"], int)
        assert not isinstance(event.features["V2"], np.integer)  # Should be native Python
        
        # Verify values preserved
        assert event.features["V1"] == 0.5
        assert event.features["V2"] == 42
    
    def test_nan_values_converted_to_none(self, temp_csv):
        """Test NaN values converted to None for JSON serialization."""
        csv_data = {
            "CustomerID": ["c1"],
            "Amount": [100.0],
            "Class": [0],
            "V1": [0.5],
            "V2": [np.nan]  # NaN value
        }
        csv_path = temp_csv(csv_data)
        generator = ULBEventGenerator(str(csv_path))
        event = generator(0, 1000)
        
        # NaN must be None for JSON serialization
        assert event.features["V2"] is None
    
    def test_json_serialization(self, temp_csv):
        """Test event features are JSON serializable."""
        csv_data = {
            "CustomerID": ["c1"],
            "Amount": [np.float64(100.0)],  # numpy type
            "Class": [np.int64(0)],  # numpy type
            "V1": [0.5],
            "V2": [np.nan],  # NaN
            "V3": [-0.2]
        }
        csv_path = temp_csv(csv_data)
        generator = ULBEventGenerator(str(csv_path))
        event = generator(0, 1000)
        
        # Must be JSON serializable (no nan values)
        import json
        json_str = json.dumps({"features": event.features})
        assert json_str  # Should not raise
        
        # Verify NaN became None
        features_dict = json.loads(json_str)["features"]
        assert features_dict["V2"] is None
    
    def test_ulb_structure_with_many_features(self, temp_csv):
        """Test event generation with ULB-like structure (V1-V28, Time)."""
        csv_data = {
            "CustomerID": ["c1"],
            "Amount": [100.0],
            "Class": [1],
            "Time": [100],
        }
        # Add V1-V28 features
        for i in range(1, 29):
            csv_data[f"V{i}"] = [0.1 * i]
        
        csv_path = temp_csv(csv_data)
        generator = ULBEventGenerator(str(csv_path))
        event = generator(0, 1000)
        
        # Should have V1-V28 and Time
        assert len(event.features) == 29  # V1-V28 + Time
        for i in range(1, 29):
            assert f"V{i}" in event.features
            # Use pytest.approx for floating point comparison
            assert event.features[f"V{i}"] == pytest.approx(0.1 * i, abs=1e-10)
        assert event.features["Time"] == 100
        
        # Should not have excluded columns
        assert "CustomerID" not in event.features
        assert "Amount" not in event.features
        assert "Class" not in event.features
    
    def test_customer_id_fallback(self, temp_csv):
        """Test customer ID fallback when missing."""
        csv_data = {
            "Amount": [100.0],  # No CustomerID column
            "Class": [0],
            "V1": [0.5]
        }
        csv_path = temp_csv(csv_data)
        generator = ULBEventGenerator(str(csv_path))
        
        # event_num % CUSTOMER_ID_MODULO
        event = generator(0, 1000)
        assert event.customer_id == "c0"  # 0 % 5000 = 0
        
        event = generator(100, 1000)
        assert event.customer_id == "c100"  # 100 % 5000 = 100
        
        event = generator(5001, 1000)
        assert event.customer_id == "c1"  # 5001 % 5000 = 1
    
    def test_amount_fallback_columns(self, temp_csv):
        """Test amount uses fallback column names."""
        csv_data = {
            "amount": [150.0],  # Lowercase 'amount' (fallback)
            "Class": [0],
            "V1": [0.5]
        }
        csv_path = temp_csv(csv_data)
        generator = ULBEventGenerator(str(csv_path))
        event = generator(0, 1000)
        assert event.amount == 150.0
    
    def test_label_fallback_columns(self, temp_csv):
        """Test label uses fallback column name."""
        csv_data = {
            "CustomerID": ["c1"],
            "Amount": [100.0],
            "label": [1],  # Lowercase 'label' (fallback)
            "V1": [0.5]
        }
        csv_path = temp_csv(csv_data)
        generator = ULBEventGenerator(str(csv_path))
        event = generator(0, 1000)
        assert event.label == 1
    
    def test_empty_csv_raises_error_on_init(self, temp_csv):
        """Test empty CSV raises ValueError on initialization."""
        csv_data = {
            "CustomerID": [],
            "Amount": [],
            "Class": [],
            "V1": []
        }
        csv_path = temp_csv(csv_data)
        with pytest.raises(ValueError, match="is empty"):
            ULBEventGenerator(str(csv_path))
    
    def test_empty_dataframe_raises_error_on_call(self, temp_csv):
        """Test None dataframe raises ValueError on call."""
        # This shouldn't happen after fix, but test the check
        csv_data = {
            "CustomerID": ["c1"],
            "Amount": [100.0],
            "Class": [0],
            "V1": [0.5]
        }
        csv_path = temp_csv(csv_data)
        generator = ULBEventGenerator(str(csv_path))
        generator.df = None  # Simulate broken state
        with pytest.raises(ValueError, match="No data available"):
            generator(0, 1000)
    
    def test_timestamp_calculation(self, temp_csv):
        """Test timestamp is base_ts + event_num."""
        csv_data = {
            "CustomerID": ["c1"],
            "Amount": [100.0],
            "Class": [0],
            "V1": [0.5]
        }
        csv_path = temp_csv(csv_data)
        generator = ULBEventGenerator(str(csv_path))
        
        assert generator(0, 1000).ts == 1000
        assert generator(5, 1000).ts == 1005
        assert generator(10, 2000).ts == 2010
    
    def test_all_feature_columns_included(self, temp_csv):
        """Test all columns except excluded ones are in features."""
        csv_data = {
            "CustomerID": ["c1"],
            "Amount": [100.0],
            "Class": [0],
            "V1": [0.1],
            "V2": [0.2],
            "V3": [0.3],
            "V28": [-0.5],  # Many PCA features
            "Time": [100]
        }
        csv_path = temp_csv(csv_data)
        generator = ULBEventGenerator(str(csv_path))
        event = generator(0, 1000)
        
        # Should have all feature columns
        assert len(event.features) == 5  # V1, V2, V3, V28, Time
        assert "V1" in event.features
        assert "V2" in event.features
        assert "V3" in event.features
        assert "V28" in event.features
        assert "Time" in event.features
    
    def test_mixed_nan_and_numpy_types(self, temp_csv):
        """Test row with both NaN and various numpy types."""
        csv_data = {
            "CustomerID": ["c1"],
            "Amount": [100.0],
            "Class": [0],
            "V1": [np.float64(0.5)],
            "V2": [np.nan],  # numpy NaN
            "V3": [np.int32(42)],
            "V4": [None]  # pandas None
        }
        csv_path = temp_csv(csv_data)
        generator = ULBEventGenerator(str(csv_path))
        event = generator(0, 1000)
        
        # Verify numpy types converted
        assert event.features["V1"] == 0.5
        assert isinstance(event.features["V1"], float)
        assert not isinstance(event.features["V1"], np.floating)
        
        # Verify NaN becomes None
        assert event.features["V2"] is None
        
        # Verify numpy int converted
        assert event.features["V3"] == 42
        assert isinstance(event.features["V3"], int)
        assert not isinstance(event.features["V3"], np.integer)
        
        # Verify pandas None stays None
        assert event.features["V4"] is None
        
        # Verify JSON serializable (no nan values)
        import json
        json_str = json.dumps(event.features)
        features_dict = json.loads(json_str)
        assert features_dict["V2"] is None
        assert features_dict["V4"] is None

