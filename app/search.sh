#!/bin/bash
echo "This script will include commands to search for documents given the query using Spark RDD"


source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 

# Python of the excutor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit --master yarn --archives /app/.venv.tar.gz#.venv \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,com.github.jnr:jnr-posix:3.1.15 query.py "$1"
    
