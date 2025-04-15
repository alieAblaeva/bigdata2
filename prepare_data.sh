#!/bin/bash

source .venv/bin/activate


# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python)


unset PYSPARK_PYTHON

# DOWNLOAD a.parquet or any parquet file before you run this

A_PARQUET_HTTP_INSTALL="https://storage.googleapis.com/kaggle-data-sets/3521629/6146260/compressed/a.parquet.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250415%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250415T190154Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=6bac169ccdfd0750c6883bc7e4a565e18d4b611a0c4f8aac56a66a3eb509c13f8435f580b38e2da72b795870d9f725e30542c376bf344b23b9808f610af0179ead6c499ebe707f12b064f1a74d3d13708d1d585807a083c02d701ae301521ce4816767c60ad19c0a5de4dc92a3b330989d034ae90cfd5ef7f653b6d1af53356e4e27784e98e47f4eb5693c6508fbe8e5dfd22d3e3465d104c9b7892604cba6a0b2cf512c7fb778f9c277b96f3476146f589636528a8043c51e1341ee5b19b8ecf8a2dcd26f0f460c4f668b0964a71b39c6e473783322b8cf8a7e68e393bbbc1a6b19c8b45e3a0104e50d06bb91b3888120fe93aeaa4b7de4b1cadf002bb8b481"

echo "install a.parquet"
if ! wget -O /app/data.zip $A_PARQUET_HTTP_INSTALL; then
  echo "Failed to download a.parquet"
  exit 1
fi

echo "Extracting file a.parquet ..."
if ! unzip -o /app/data.zip -d /app; then
  echo "ERROR: unzip /app/data.zip"
  exit 2
fi

echo "clear local dirs" && \
rm -rf data
mkdir -p data
rm -rf index/data
mkdir -p index/data
echo "hdfs mkdir and putting" && \
hdfs dfs -mkdir -p /index/data && \
hdfs dfs -mkdir -p /data && \
hdfs dfs -put -f a.parquet / && \
echo "Starting spark-submit" && \
spark-submit --executor-memory 3g --driver-memory 4g prepare_data.py  && \
echo "Putting data to hdfs"  && \
hdfs dfs -put -f data /data && \
hdfs dfs -ls -R /data && \
hdfs dfs -ls -R /index/data && \
echo "Done data preparation!"
