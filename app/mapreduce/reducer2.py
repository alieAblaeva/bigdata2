import sys
from cassandra.cluster import Cluster
cluster = Cluster(['cassandra-server'], port=9042)
session = cluster.connect('my_db')
docs_num = 0
summ_len = 0
for row in sys.stdin:
    _, length = row.strip().split('\t')
    docs_num += 1
    summ_len += int(length)
session.execute(
    "INSERT INTO stats (name, value) VALUES (%s, %s)",
    ('avg_length', summ_len / docs_num if docs_num > 0 else 0)
)
session.execute(
    "INSERT INTO stats (name, value) VALUES (%s, %s)",
    ('total_docs', docs_num)
)
