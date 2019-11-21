#!/bin/bash

echo "Starting ${0##*/} "
python3 create_tables.py
echo "========== Executed create_tables.py =========="
python3 etl.py 
echo "========== Executed etl.py =========="
echo "========== End of ${0##*/} script =========="
