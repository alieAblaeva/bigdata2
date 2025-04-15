import sys
from cassandra.cluster import Cluster


cluster = Cluster(['cassandra-server'], port=9042)
session = cluster.connect('my_db')
for line in sys.stdin:
    try:
        doc_id, title, text = line.strip().split('\t')
        terms = text.lower().split()
        term_counts = {}
        for term in terms:
            term_counts[term] = term_counts.get(term, 0) + 1
        session.execute(
            "INSERT INTO docs (id, title, content, len) VALUES (%s, %s, %s, %s)",
            (doc_id, title, text, len(text.split()))
        )
        for term in term_counts: print(f"{term}\t{doc_id}\t{term_counts[term]}")
    except Exception as e:
        sys.stderr.write(f"Error processing line: {str(e)}\n")
