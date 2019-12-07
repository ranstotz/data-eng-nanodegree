#!/bin/bash

# Driver script for ETL system.

echo -e "\nStarting ${0##*/} for Project 02 - Data Modeling with Apache Cassandra"
echo -e "========== Starting etl.py ==========\n"
python3 src/etl.py 
echo -e "\n========== Executed etl.py =========="
echo -e "========== End of ${0##*/}   ==========\n"
