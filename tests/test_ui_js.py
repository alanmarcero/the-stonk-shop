import subprocess
import os
import pytest

def test_ui_filters():
    """
    Executes the Node.js test script for UI filtering logic.
    This ensures that the SOURCE_DEFS and matchesSources logic in app.js
    behave as expected for various market caps and ETF sets.
    """
    script_path = os.path.join(os.path.dirname(__file__), "test_filters.js")
    
    # Run the node script
    result = subprocess.run(
        ["node", script_path],
        capture_output=True,
        text=True
    )
    
    # Assert success
    if result.returncode != 0:
        pytest.fail(f"UI filter tests failed with exit code {result.returncode}:\n{result.stdout}\n{result.stderr}")
    
    assert "SUCCESS: All 17 tests passed." in result.stdout
