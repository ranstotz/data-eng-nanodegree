#!/bin/bash

# Driver to execute all scripts in required order.

echo -e "\nStarting ${0##*/} for Project 01 - Data Modeling with Postgres \n"
python3 src/create_tables.py
echo -e "\n========== Executed create_tables.py ==========\n"
python3 src/etl.py 
echo -e "\n========== Executed etl.py =========="
echo -e "========== End of ${0##*/} script ==========\n"
