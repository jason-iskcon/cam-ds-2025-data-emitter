"""Tests for BurstController."""
import pytest
from unittest.mock import patch

from emitter._streaming import BurstController


class TestBurstController:
    """Test BurstController behavior."""
    
    def test_init(self):
        """Test BurstController initialization."""
        controller = BurstController(
            probability=0.05,
            multiplier=5.0,
            duration_events=10,
            base_rate=10.0,
        )
        assert controller.probability == 0.05
        assert controller.multiplier == 5.0
        assert controller.duration_events == 10
        assert controller.base_rate == 10.0
        assert controller.remaining == 0
        assert controller.burst_rate == 50.0  # 10.0 * 5.0
    
    def test_should_start_burst_disabled(self):
        """Test that burst doesn't start when probability is 0."""
        controller = BurstController(
            probability=0.0,
            multiplier=5.0,
            duration_events=10,
            base_rate=10.0,
        )
        # Even if random returns 0 (always trigger), probability=0 means no burst
        with patch('emitter._streaming.random.random', return_value=0.0):
            assert controller.should_start_burst() is False
    
    def test_should_start_burst_already_in_burst(self):
        """Test that new burst doesn't start if already in burst."""
        controller = BurstController(
            probability=1.0,
            multiplier=5.0,
            duration_events=10,
            base_rate=10.0,
        )
        controller.remaining = 5  # Already in burst
        
        with patch('emitter._streaming.random.random', return_value=0.0):
            assert controller.should_start_burst() is False
    
    def test_should_start_burst_probability_check(self):
        """Test burst start probability check."""
        # Test should trigger - random < probability
        controller1 = BurstController(
            probability=0.5,
            multiplier=5.0,
            duration_events=10,
            base_rate=10.0,
        )
        with patch('emitter._streaming.random.random', return_value=0.3):
            assert controller1.should_start_burst() is True
            # Verify state unchanged (predicate only)
            assert controller1.remaining == 0
        
        # Test should not trigger - random >= probability (new instance)
        controller2 = BurstController(
            probability=0.5,
            multiplier=5.0,
            duration_events=10,
            base_rate=10.0,
        )
        with patch('emitter._streaming.random.random', return_value=0.7):
            assert controller2.should_start_burst() is False
    
    def test_start_burst(self):
        """Test starting a burst."""
        controller = BurstController(
            probability=0.05,
            multiplier=5.0,
            duration_events=10,
            base_rate=10.0,
        )
        assert controller.remaining == 0
        
        controller.start_burst()
        assert controller.remaining == 10
    
    def test_tick_during_burst(self):
        """Test tick during active burst returns burst rate interval."""
        controller = BurstController(
            probability=0.05,
            multiplier=5.0,
            duration_events=3,
            base_rate=10.0,
        )
        controller.start_burst()  # remaining = 3
        
        # First tick: in burst, should return burst rate interval
        interval = controller.tick()
        assert interval == 1.0 / 50.0  # 1.0 / burst_rate
        assert controller.remaining == 2
        
        # Second tick: still in burst
        interval = controller.tick()
        assert interval == 1.0 / 50.0
        assert controller.remaining == 1
        
        # Third tick: last one in burst
        interval = controller.tick()
        assert interval == 1.0 / 50.0
        assert controller.remaining == 0
    
    def test_tick_after_burst(self):
        """Test tick after burst ends returns base rate interval."""
        controller = BurstController(
            probability=0.05,
            multiplier=5.0,
            duration_events=2,
            base_rate=10.0,
        )
        controller.start_burst()
        
        # Consume burst
        controller.tick()  # remaining = 1
        controller.tick()  # remaining = 0
        
        # After burst: should return base rate
        interval = controller.tick()
        assert interval == 1.0 / 10.0  # 1.0 / base_rate
        assert controller.remaining == 0
    
    def test_tick_no_burst(self):
        """Test tick when no burst active returns base rate."""
        controller = BurstController(
            probability=0.05,
            multiplier=5.0,
            duration_events=10,
            base_rate=10.0,
        )
        # Never started burst, remaining = 0
        
        interval = controller.tick()
        assert interval == 1.0 / 10.0  # base rate
        assert controller.remaining == 0
    
    def test_burst_lifecycle(self):
        """Test complete burst lifecycle matching actual usage pattern."""
        controller = BurstController(
            probability=1.0,
            multiplier=3.0,
            duration_events=2,
            base_rate=5.0,
        )
        
        # Start in base rate (no burst active)
        assert controller.tick() == 1.0 / 5.0
        assert controller.remaining == 0
        
        # Check if burst should start, then explicitly start it (matches stream_events usage)
        with patch('emitter._streaming.random.random', return_value=0.0):
            assert controller.should_start_burst() is True
        controller.start_burst()  # Explicit start (as in stream_events)
        assert controller.remaining == 2
        
        # During burst - consume both events
        assert controller.tick() == 1.0 / 15.0  # burst_rate = 5.0 * 3.0
        assert controller.remaining == 1
        assert controller.tick() == 1.0 / 15.0
        assert controller.remaining == 0
        
        # Back to base rate after burst ends
        assert controller.tick() == 1.0 / 5.0
        assert controller.remaining == 0
        
        # Can check for new burst again (state reset)
        with patch('emitter._streaming.random.random', return_value=0.0):
            assert controller.should_start_burst() is True
    
    def test_init_zero_base_rate(self):
        """Test with zero base rate raises validation error."""
        with pytest.raises(AssertionError, match="base_rate must be positive"):
            BurstController(
                probability=0.05, multiplier=5.0, duration_events=10, base_rate=0.0
            )
    
    def test_init_negative_base_rate(self):
        """Test with negative base rate raises validation error."""
        with pytest.raises(AssertionError, match="base_rate must be positive"):
            BurstController(
                probability=0.05, multiplier=5.0, duration_events=10, base_rate=-1.0
            )
    
    def test_init_invalid_probability(self):
        """Test invalid probability values raise validation error."""
        with pytest.raises(AssertionError, match="probability must be in"):
            BurstController(
                probability=-0.1, multiplier=5.0, duration_events=10, base_rate=10.0
            )
        with pytest.raises(AssertionError, match="probability must be in"):
            BurstController(
                probability=1.5, multiplier=5.0, duration_events=10, base_rate=10.0
            )
    
    def test_init_invalid_multiplier(self):
        """Test invalid multiplier values raise validation error."""
        with pytest.raises(AssertionError, match="multiplier must be positive"):
            BurstController(
                probability=0.05, multiplier=0.0, duration_events=10, base_rate=10.0
            )
        with pytest.raises(AssertionError, match="multiplier must be positive"):
            BurstController(
                probability=0.05, multiplier=-1.0, duration_events=10, base_rate=10.0
            )
    
    def test_init_invalid_duration(self):
        """Test invalid duration values raise validation error."""
        with pytest.raises(AssertionError, match="duration_events must be positive"):
            BurstController(
                probability=0.05, multiplier=5.0, duration_events=0, base_rate=10.0
            )
        with pytest.raises(AssertionError, match="duration_events must be positive"):
            BurstController(
                probability=0.05, multiplier=5.0, duration_events=-1, base_rate=10.0
            )
    
    def test_should_start_burst_always(self):
        """Test probability=1.0 always triggers (when not in burst)."""
        controller = BurstController(
            probability=1.0, multiplier=5.0, duration_events=10, base_rate=10.0
        )
        # random.random() returns [0.0, 1.0), so any value < 1.0 should trigger
        with patch('emitter._streaming.random.random', return_value=0.99):
            assert controller.should_start_burst() is True
        with patch('emitter._streaming.random.random', return_value=0.0):
            assert controller.should_start_burst() is True
    
    def test_burst_single_event(self):
        """Test burst with duration_events=1."""
        controller = BurstController(
            probability=1.0, multiplier=5.0, duration_events=1, base_rate=10.0
        )
        controller.start_burst()
        assert controller.remaining == 1
        assert controller.tick() == 1.0 / 50.0  # burst_rate = 10.0 * 5.0
        assert controller.remaining == 0
        assert controller.tick() == 1.0 / 10.0  # Back to base
    
    def test_burst_multiplier_one(self):
        """Test burst with multiplier=1.0 (same as base rate)."""
        controller = BurstController(
            probability=0.5, multiplier=1.0, duration_events=10, base_rate=10.0
        )
        assert controller.burst_rate == 10.0
        controller.start_burst()
        
        expected_interval = 1.0 / 10.0
        # During burst: should equal base rate
        assert controller.tick() == expected_interval
        # After burst completes: still base rate
        controller.remaining = 0
        assert controller.tick() == expected_interval
    
    def test_start_burst_while_in_burst(self):
        """Test calling start_burst() while already in burst does nothing."""
        controller = BurstController(
            probability=1.0, multiplier=5.0, duration_events=5, base_rate=10.0
        )
        controller.start_burst()
        assert controller.remaining == 5
        
        # Consume one event
        controller.tick()
        assert controller.remaining == 4
        
        # Try to start again mid-burst - should not reset to 5
        controller.start_burst()
        assert controller.remaining == 4  # Should stay at 4, not reset
    
    def test_should_start_burst_boundary_probability(self):
        """Test probability boundary: random() == probability should NOT trigger."""
        controller = BurstController(
            probability=0.5, multiplier=5.0, duration_events=10, base_rate=10.0
        )
        # random() returns exactly 0.5, check is random() < probability
        # 0.5 < 0.5 is False, so should not trigger
        with patch('emitter._streaming.random.random', return_value=0.5):
            assert controller.should_start_burst() is False
    
    def test_should_start_burst_probability_zero_short_circuit(self):
        """Test probability=0.0 short-circuits before random call."""
        controller = BurstController(
            probability=0.0, multiplier=5.0, duration_events=10, base_rate=10.0
        )
        # Should short-circuit and never call random.random()
        with patch('emitter._streaming.random.random', side_effect=AssertionError("Should not be called")):
            assert controller.should_start_burst() is False

