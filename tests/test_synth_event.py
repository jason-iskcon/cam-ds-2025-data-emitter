"""Tests for synth_event fraud detection logic."""

import pytest
from unittest.mock import patch

from emitter.emit_stdout import DEFAULT_FRAUD_CONFIG, synth_event


@pytest.fixture
def patched_randoms():
    """Fixture for patching common random functions used in synth_event."""
    with (
        patch("emitter.emit_stdout.random.randint") as mock_randint,
        patch("emitter.emit_stdout.random.choices") as mock_choices,
        patch("emitter.emit_stdout.random.lognormvariate") as mock_lognorm,
        patch("emitter.emit_stdout.random.random") as mock_random,
    ):
        yield mock_randint, mock_choices, mock_lognorm, mock_random


class TestSynthEvent:
    """Test synth_event fraud detection logic."""

    def test_event_structure(self, patched_randoms):
        """Test basic event structure and fields."""
        mock_randint, mock_choices, mock_lognorm, mock_random = patched_randoms
        mock_randint.return_value = 100
        mock_choices.return_value = ["grocery"]
        mock_lognorm.return_value = 50.0
        mock_random.return_value = 0.5

        event = synth_event(0, 1000)

        assert event.tx_id == "tx_0"
        assert event.customer_id == "c100"
        assert event.amount == 50.0
        assert event.merchant_cat == "grocery"
        assert event.ts == 1000  # base_ts + event_num
        assert event.label == 0

    def test_customer_id_format(self, patched_randoms):
        """Test customer_id format is correct."""
        mock_randint, mock_choices, mock_lognorm, mock_random = patched_randoms
        mock_randint.return_value = 42
        mock_choices.return_value = ["electronics"]
        mock_lognorm.return_value = 50.0
        mock_random.return_value = 0.5

        event = synth_event(5, 2000)
        assert event.customer_id.startswith("c")
        assert event.customer_id == "c42"

    def test_merchant_category_from_mcc(self, patched_randoms):
        """Test merchant category is one of valid MCC values."""
        mock_randint, mock_choices, mock_lognorm, mock_random = patched_randoms
        mock_randint.return_value = 100
        mock_lognorm.return_value = 50.0
        mock_random.return_value = 0.5

        for category in DEFAULT_FRAUD_CONFIG.mcc:
            mock_choices.return_value = [category]
            event = synth_event(0, 1000)
            assert event.merchant_cat == category
            assert event.merchant_cat in DEFAULT_FRAUD_CONFIG.mcc

    def test_timestamp_calculation(self, patched_randoms):
        """Test timestamp is base_ts + event_num."""
        mock_randint, mock_choices, mock_lognorm, mock_random = patched_randoms
        mock_randint.return_value = 100
        mock_choices.return_value = ["grocery"]
        mock_lognorm.return_value = 50.0
        mock_random.return_value = 0.5

        # Test various event numbers
        assert synth_event(0, 1000).ts == 1000
        assert synth_event(5, 1000).ts == 1005
        assert synth_event(10, 2000).ts == 2010

    def test_no_fraud_all_conditions_false(self, patched_randoms):
        """Test label=0 when no fraud conditions are met."""
        # Low amount, low-value category, not night
        mock_randint, mock_choices, mock_lognorm, mock_random = patched_randoms
        mock_randint.return_value = 100
        mock_choices.return_value = ["grocery"]
        mock_lognorm.return_value = 50.0  # < 300
        mock_random.return_value = 0.1  # < 0.4
        # Hour 10 (not night), grocery (not high-value), amount 50 (< 300)
        # base_ts = 36000 (10 AM)
        event = synth_event(0, 36000)
        assert event.label == 0

    def test_fraud_all_conditions_true(self, patched_randoms):
        """Test label=1 when all fraud conditions are met."""
        # High amount, luxury/online, night hours, probability check passes
        mock_randint, mock_choices, mock_lognorm, mock_random = patched_randoms
        mock_randint.return_value = 100
        mock_choices.return_value = ["luxury"]
        mock_lognorm.return_value = 500.0  # > 300
        mock_random.return_value = 0.3  # < 0.4
        # Hour 2 (night), luxury (high-value), amount 500 (> 300)
        # base_ts = 7200 (2 AM)
        event = synth_event(0, 7200)
        assert event.label == 1

    def test_fraud_amount_too_low(self, patched_randoms):
        """Test label=0 when amount threshold not met."""
        mock_randint, mock_choices, mock_lognorm, mock_random = patched_randoms
        mock_randint.return_value = 100
        mock_choices.return_value = ["luxury"]
        mock_lognorm.return_value = 200.0  # < 300
        mock_random.return_value = 0.3
        event = synth_event(0, 7200)  # Night, luxury category
        assert event.label == 0  # Amount too low

    def test_fraud_wrong_category(self, patched_randoms):
        """Test label=0 when category is not luxury/online."""
        mock_randint, mock_choices, mock_lognorm, mock_random = patched_randoms
        mock_randint.return_value = 100
        mock_choices.return_value = ["grocery"]  # Not high-value
        mock_lognorm.return_value = 500.0  # > 300
        mock_random.return_value = 0.3
        event = synth_event(0, 7200)  # Night, high amount
        assert event.label == 0  # Wrong category

    def test_fraud_not_night(self, patched_randoms):
        """Test label=0 when transaction is not during night hours."""
        mock_randint, mock_choices, mock_lognorm, mock_random = patched_randoms
        mock_randint.return_value = 100
        mock_choices.return_value = ["luxury"]
        mock_lognorm.return_value = 500.0  # > 300
        mock_random.return_value = 0.3
        # Hour 12 (noon, not night)
        event = synth_event(0, 43200)  # 12 PM
        assert event.label == 0  # Not night

    def test_fraud_probability_check_fails(self, patched_randoms):
        """Test label=0 when probability check fails."""
        mock_randint, mock_choices, mock_lognorm, mock_random = patched_randoms
        mock_randint.return_value = 100
        mock_choices.return_value = ["luxury"]
        mock_lognorm.return_value = 500.0  # > 300
        mock_random.return_value = 0.5  # >= 0.4, fails check
        event = synth_event(0, 7200)  # Night, luxury, high amount
        assert event.label == 0  # Probability check failed (0.5 >= 0.4)

    def test_fraud_probability_boundary_exact(self, patched_randoms):
        """Test probability boundary: exactly 0.4 should NOT trigger (uses < not <=)."""
        mock_randint, mock_choices, mock_lognorm, mock_random = patched_randoms
        mock_randint.return_value = 100
        mock_choices.return_value = ["luxury"]
        mock_lognorm.return_value = 500.0  # > 300
        mock_random.return_value = 0.4  # Exactly 0.4
        # Check uses <, so 0.4 should not trigger (0.4 < 0.4 is False)
        event = synth_event(0, 7200)  # Night, luxury, high amount
        assert event.label == 0  # Probability check failed (0.4 < 0.4 is False)

    def test_night_hours_edge_cases(self, patched_randoms):
        """Test night hours boundary conditions."""
        mock_randint, mock_choices, mock_lognorm, mock_random = patched_randoms
        mock_randint.return_value = 100
        mock_choices.return_value = ["luxury"]
        mock_lognorm.return_value = 500.0
        mock_random.return_value = 0.3
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

    def test_amount_exactly_at_threshold(self, patched_randoms):
        """Test amount exactly at threshold does not trigger fraud (uses > not >=)."""
        mock_randint, mock_choices, mock_lognorm, mock_random = patched_randoms
        mock_randint.return_value = 100
        mock_choices.return_value = ["luxury"]
        mock_lognorm.return_value = 300.0  # Exactly 300
        mock_random.return_value = 0.3
        # Check uses >, so 300.0 should not trigger (300.0 > 300 is False)
        event = synth_event(0, 7200)  # Night, luxury
        assert event.label == 0  # amount > 300 is False for 300.0

    def test_amount_just_above_threshold(self, patched_randoms):
        """Test amount just above threshold triggers fraud."""
        mock_randint, mock_choices, mock_lognorm, mock_random = patched_randoms
        mock_randint.return_value = 100
        mock_choices.return_value = ["luxury"]
        mock_lognorm.return_value = 300.01  # Just above 300
        mock_random.return_value = 0.3
        event = synth_event(0, 7200)  # Night, luxury
        assert event.label == 1  # amount > 300 is True for 300.01

    def test_online_category_high_value(self, patched_randoms):
        """Test 'online' category is treated as high-value."""
        mock_randint, mock_choices, mock_lognorm, mock_random = patched_randoms
        mock_randint.return_value = 100
        mock_choices.return_value = ["online"]
        mock_lognorm.return_value = 500.0
        mock_random.return_value = 0.3
        event = synth_event(0, 7200)  # Night, high amount
        assert event.label == 1  # Online is high-value category
