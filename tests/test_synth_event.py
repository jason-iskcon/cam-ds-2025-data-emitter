"""Tests for synth_event fraud detection logic."""
import pytest
from unittest.mock import patch

from emitter.config import FraudConfig
from emitter.emit_stdout import DEFAULT_FRAUD_CONFIG, synth_event


class TestSynthEvent:
    """Test synth_event fraud detection logic."""
    
    def test_event_structure(self):
        """Test basic event structure and fields."""
        with patch('emitter.emit_stdout.random.randint', return_value=100):
            with patch('emitter.emit_stdout.random.choices', return_value=['grocery']):
                with patch('emitter.emit_stdout.random.lognormvariate', return_value=50.0):
                    with patch('emitter.emit_stdout.random.random', return_value=0.5):
                        event = synth_event(0, 1000)
                        
                        assert event.tx_id == "tx_0"
                        assert event.customer_id == "c100"
                        assert event.amount == 50.0
                        assert event.merchant_cat == "grocery"
                        assert event.ts == 1000  # base_ts + event_num
                        assert event.label == 0
    
    def test_customer_id_format(self):
        """Test customer_id format is correct."""
        with patch('emitter.emit_stdout.random.randint', return_value=42):
            with patch('emitter.emit_stdout.random.choices', return_value=['electronics']):
                with patch('emitter.emit_stdout.random.lognormvariate', return_value=50.0):
                    with patch('emitter.emit_stdout.random.random', return_value=0.5):
                        event = synth_event(5, 2000)
                        assert event.customer_id.startswith('c')
                        assert event.customer_id == "c42"
    
    def test_merchant_category_from_mcc(self):
        """Test merchant category is one of valid MCC values."""
        for category in DEFAULT_FRAUD_CONFIG.mcc:
            with patch('emitter.emit_stdout.random.randint', return_value=100):
                with patch('emitter.emit_stdout.random.choices', return_value=[category]):
                    with patch('emitter.emit_stdout.random.lognormvariate', return_value=50.0):
                        with patch('emitter.emit_stdout.random.random', return_value=0.5):
                            event = synth_event(0, 1000)
                            assert event.merchant_cat == category
                            assert event.merchant_cat in DEFAULT_FRAUD_CONFIG.mcc
    
    def test_timestamp_calculation(self):
        """Test timestamp is base_ts + event_num."""
        with patch('emitter.emit_stdout.random.randint', return_value=100):
            with patch('emitter.emit_stdout.random.choices', return_value=['grocery']):
                with patch('emitter.emit_stdout.random.lognormvariate', return_value=50.0):
                    with patch('emitter.emit_stdout.random.random', return_value=0.5):
                        # Test various event numbers
                        assert synth_event(0, 1000).ts == 1000
                        assert synth_event(5, 1000).ts == 1005
                        assert synth_event(10, 2000).ts == 2010
    
    def test_no_fraud_all_conditions_false(self):
        """Test label=0 when no fraud conditions are met."""
        # Low amount, low-value category, not night
        with patch('emitter.emit_stdout.random.randint', return_value=100):
            with patch('emitter.emit_stdout.random.choices', return_value=['grocery']):
                with patch('emitter.emit_stdout.random.lognormvariate', return_value=50.0):  # < 300
                    with patch('emitter.emit_stdout.random.random', return_value=0.1):  # < 0.4
                        # Hour 10 (not night), grocery (not high-value), amount 50 (< 300)
                        # base_ts = 36000 (10 AM)
                        event = synth_event(0, 36000)
                        assert event.label == 0
    
    def test_fraud_all_conditions_true(self):
        """Test label=1 when all fraud conditions are met."""
        # High amount, luxury/online, night hours, probability check passes
        with patch('emitter.emit_stdout.random.randint', return_value=100):
            with patch('emitter.emit_stdout.random.choices', return_value=['luxury']):
                with patch('emitter.emit_stdout.random.lognormvariate', return_value=500.0):  # > 300
                    with patch('emitter.emit_stdout.random.random', return_value=0.3):  # < 0.4
                        # Hour 2 (night), luxury (high-value), amount 500 (> 300)
                        # base_ts = 7200 (2 AM)
                        event = synth_event(0, 7200)
                        assert event.label == 1
    
    def test_fraud_amount_too_low(self):
        """Test label=0 when amount threshold not met."""
        with patch('emitter.emit_stdout.random.randint', return_value=100):
            with patch('emitter.emit_stdout.random.choices', return_value=['luxury']):
                with patch('emitter.emit_stdout.random.lognormvariate', return_value=200.0):  # < 300
                    with patch('emitter.emit_stdout.random.random', return_value=0.3):
                        event = synth_event(0, 7200)  # Night, luxury category
                        assert event.label == 0  # Amount too low
    
    def test_fraud_wrong_category(self):
        """Test label=0 when category is not luxury/online."""
        with patch('emitter.emit_stdout.random.randint', return_value=100):
            with patch('emitter.emit_stdout.random.choices', return_value=['grocery']):  # Not high-value
                with patch('emitter.emit_stdout.random.lognormvariate', return_value=500.0):  # > 300
                    with patch('emitter.emit_stdout.random.random', return_value=0.3):
                        event = synth_event(0, 7200)  # Night, high amount
                        assert event.label == 0  # Wrong category
    
    def test_fraud_not_night(self):
        """Test label=0 when transaction is not during night hours."""
        with patch('emitter.emit_stdout.random.randint', return_value=100):
            with patch('emitter.emit_stdout.random.choices', return_value=['luxury']):
                with patch('emitter.emit_stdout.random.lognormvariate', return_value=500.0):  # > 300
                    with patch('emitter.emit_stdout.random.random', return_value=0.3):
                        # Hour 12 (noon, not night)
                        event = synth_event(0, 43200)  # 12 PM
                        assert event.label == 0  # Not night
    
    def test_fraud_probability_check_fails(self):
        """Test label=0 when probability check fails."""
        with patch('emitter.emit_stdout.random.randint', return_value=100):
            with patch('emitter.emit_stdout.random.choices', return_value=['luxury']):
                with patch('emitter.emit_stdout.random.lognormvariate', return_value=500.0):  # > 300
                    with patch('emitter.emit_stdout.random.random', return_value=0.5):  # >= 0.4, fails check
                        event = synth_event(0, 7200)  # Night, luxury, high amount
                        assert event.label == 0  # Probability check failed (0.5 >= 0.4)
    
    def test_fraud_probability_boundary_exact(self):
        """Test probability boundary: exactly 0.4 should NOT trigger (uses < not <=)."""
        with patch('emitter.emit_stdout.random.randint', return_value=100):
            with patch('emitter.emit_stdout.random.choices', return_value=['luxury']):
                with patch('emitter.emit_stdout.random.lognormvariate', return_value=500.0):  # > 300
                    with patch('emitter.emit_stdout.random.random', return_value=0.4):  # Exactly 0.4
                        # Check uses <, so 0.4 should not trigger (0.4 < 0.4 is False)
                        event = synth_event(0, 7200)  # Night, luxury, high amount
                        assert event.label == 0  # Probability check failed (0.4 < 0.4 is False)
    
    def test_night_hours_edge_cases(self):
        """Test night hours boundary conditions."""
        with patch('emitter.emit_stdout.random.randint', return_value=100):
            with patch('emitter.emit_stdout.random.choices', return_value=['luxury']):
                with patch('emitter.emit_stdout.random.lognormvariate', return_value=500.0):
                    with patch('emitter.emit_stdout.random.random', return_value=0.3):
                        # Hour 0 (midnight) - should be night
                        event1 = synth_event(0, 0)
                        assert event1.label == 1
                        
                        # Hour 5 - should be night
                        event2 = synth_event(0, 18000)  # 5 AM
                        assert event2.label == 1
                        
                        # Hour 6 - should NOT be night (first non-night hour)
                        # All other fraud conditions met, but not night
                        event3 = synth_event(0, 21600)  # 6 AM
                        assert event3.label == 0  # Not night despite other conditions
                        
                        # Hour 22 - should NOT be night (NIGHT_HOURS is 0-5 and 23)
                        # All other fraud conditions met, but not night
                        event4 = synth_event(0, 79200)  # 10 PM (hour 22)
                        assert event4.label == 0  # Not night despite other conditions
                        
                        # Hour 23 - should be night
                        event5 = synth_event(0, 82800)  # 11 PM
                        assert event5.label == 1
    
    def test_amount_exactly_at_threshold(self):
        """Test amount exactly at threshold does not trigger fraud (uses > not >=)."""
        with patch('emitter.emit_stdout.random.randint', return_value=100):
            with patch('emitter.emit_stdout.random.choices', return_value=['luxury']):
                with patch('emitter.emit_stdout.random.lognormvariate', return_value=300.0):  # Exactly 300
                    with patch('emitter.emit_stdout.random.random', return_value=0.3):
                        # Check uses >, so 300.0 should not trigger (300.0 > 300 is False)
                        event = synth_event(0, 7200)  # Night, luxury
                        assert event.label == 0  # amount > 300 is False for 300.0
    
    def test_amount_just_above_threshold(self):
        """Test amount just above threshold triggers fraud."""
        with patch('emitter.emit_stdout.random.randint', return_value=100):
            with patch('emitter.emit_stdout.random.choices', return_value=['luxury']):
                with patch('emitter.emit_stdout.random.lognormvariate', return_value=300.01):  # Just above 300
                    with patch('emitter.emit_stdout.random.random', return_value=0.3):
                        event = synth_event(0, 7200)  # Night, luxury
                        assert event.label == 1  # amount > 300 is True for 300.01
    
    def test_online_category_high_value(self):
        """Test 'online' category is treated as high-value."""
        with patch('emitter.emit_stdout.random.randint', return_value=100):
            with patch('emitter.emit_stdout.random.choices', return_value=['online']):
                with patch('emitter.emit_stdout.random.lognormvariate', return_value=500.0):
                    with patch('emitter.emit_stdout.random.random', return_value=0.3):
                        event = synth_event(0, 7200)  # Night, high amount
                        assert event.label == 1  # Online is high-value category

