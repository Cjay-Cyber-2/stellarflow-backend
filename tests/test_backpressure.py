import pytest
from unittest.mock import patch, MagicMock
from src.queue.backpressure import QueueCapacityController

@patch('psutil.virtual_memory')
def test_dynamic_memory_resizing(mock_virtual_memory):
    # Setup mock to return > 85% memory usage
    mock_mem = MagicMock()
    mock_mem.percent = 86.0
    mock_virtual_memory.return_value = mock_mem
    
    controller = QueueCapacityController(initial_capacity=1000, min_capacity=100)
    
    # Check that capacity contracts when memory > 85%
    new_capacity = controller.get_capacity()
    assert new_capacity == 500  # 1000 * 0.5
    
    # Check it contracts again
    new_capacity = controller.get_capacity()
    assert new_capacity == 250
    
    # Test normal memory condition (< 85%)
    mock_mem.percent = 50.0
    new_capacity_normal = controller.get_capacity()
    assert new_capacity_normal > 250  # Should start recovering
