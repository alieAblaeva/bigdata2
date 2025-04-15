#!/bin/bash
echo "This script include commands to run mapreduce jobs using hadoop streaming to index documents"

INPUT_PATH=${1:-/index/data}

hdfs dfs -rm -r -f /tmp/index/output1
hdfs dfs -rm -r -f /tmp/index/output2


hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -archives ".venv.tar.gz#venv" \
    -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
    -mapper "venv/bin/python3 mapper1.py" \
    -reducer "venv/bin/python3 reducer1.py" \
    -input $INPUT_PATH \
    -output /tmp/index/output1 


hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -archives ".venv.tar.gz#venv" \
    -files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
    -mapper "venv/bin/python3 mapper2.py" \
    -reducer "venv/bin/python3 reducer2.py" \
    -input $INPUT_PATH \
    -output /tmp/index/output2


hdfs dfs -rm -r /tmp/index
