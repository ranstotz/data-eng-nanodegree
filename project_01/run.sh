#!/bin/bash

# Driver to execute all scripts in required order.

echo "Starting ${0##*/} "
python3 src/create_tables.py
echo "========== Executed create_tables.py =========="
python3 src/etl.py 
echo "========== Executed etl.py =========="
echo "========== End of ${0##*/} script =========="
