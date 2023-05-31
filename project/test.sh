#!/bin/bash

# Check if files exist
if [ -f "bikeCountKonstanz_ds1.sqlite" ] && [ -f "bikeCountKonstanz_ds2.sqlite" ]; then
    echo "Both output files exist. File check test passed."
else
    echo "Either output1.sql or output2.sql is missing. File check test failed."
    exit 1
fi

# Execute extract_transform_load.py
python extract_transform_load.py

# Check if extract_transform_load.py executed successfully
if [ $? -eq 0 ]; then
    echo "extract_transform_load.py executed successfully."
else
    echo "extract_transform_load.py execution failed."
    exit 1
fi

# Execute pytest.py
python pytest.py

# Check if test.py executed successfully
if [ $? -eq 0 ]; then
    echo "All tests passed."
    exit 0
else
    echo "Some tests failed."
    exit 1
fi