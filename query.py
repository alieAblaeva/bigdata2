from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions as F
import math
import sys


try:
    conf = SparkConf().setAppName("ranker").set("spark.cassandra.connection.host", "cassandra-server")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    
    stats_df = spark.read.format("org.apache.spark.sql.cassandra").options(table="stats", keyspace="my_db").load()
    total_docs = stats_df.filter("name = 'total_docs'").first().value
    avg_length = stats_df.filter("name = 'avg_length'").first().value
    inverted_index = spark.read.format("org.apache.spark.sql.cassandra").options(table="indexs", keyspace="my_db").load()
    documents = spark.read.format("org.apache.spark.sql.cassandra").options(table="docs", keyspace="my_db").load()
    df_map = inverted_index.groupBy("term_text").count().rdd.map(lambda x: (x.term_text, x["count"])).collectAsMap()
    df_bc = sc.broadcast(df_map)
    joined_data = inverted_index.join(documents, "id", "inner")
    query_terms = [word.lower() for word in sys.argv[1].split()]
    if not query_terms:
        print("Empty query!")
        exit(1)
    filtered_data = joined_data.filter(F.col("term_text").isin(query_terms))


    def calculate_bm25(row):
        k1 = 1.0
        b = 0.75
        term = row.term_text
        tf = row.tf
        df = df_bc.value.get(term, 0)
        idf = math.log((total_docs) / df)
        score = idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (row.len / avg_length)))
        return (row.id, score)


    results = filtered_data.rdd.map(calculate_bm25).reduceByKey(lambda a, b: a + b).takeOrdered(10, key=lambda x: -x[1])
    titles = documents.rdd.map(lambda x: (x.id, x.title)).collectAsMap()
    print("\nTop 10 Results:")
    for i, (doc_id, _) in enumerate(results, 1):
        print(f"{i}. {doc_id}   {titles.get(doc_id, 'Unknown')}")
finally:
    sc.stop()

