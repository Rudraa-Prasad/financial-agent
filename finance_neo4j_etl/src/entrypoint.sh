#!/bin/bash

# Run any setup steps or pre-processing tasks here
echo "Running ETL to move finance data from csvs to Neo4j..."

# Run the ETL script
python finance_csv_write.py