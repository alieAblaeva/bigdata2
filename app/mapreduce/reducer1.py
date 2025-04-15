import sys
import uuid
from cassandra.cluster import Cluster



cluster = Cluster(['cassandra-server'], port=9042)
session = cluster.connect('my_db')
term_set = set()
for line in sys.stdin:
    try:
        term, doc_id, tf = line.strip().split('\t')
        if term not in term_set:
            session.execute(
                "INSERT INTO terms (term_id, term_text) VALUES (%s, %s)",
                (uuid.uuid1().hex, term)
            )
            term_set.add(term)
        session.execute(
            "INSERT INTO indexs (term_text, id, tf) VALUES (%s, %s, %s)",
            (term, doc_id, int(tf))
        )
    except Exception as e:
        sys.stderr.write(f"Error processing line: ERROR: {str(e)}\n")
        sys.exit(2)


