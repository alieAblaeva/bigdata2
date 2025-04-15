CREATE KEYSPACE IF NOT EXISTS my_db
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};


USE my_db;


CREATE TABLE IF NOT EXISTS indexs (
    term_text TEXT,
    id TEXT,
    tf INT,
    PRIMARY KEY (term_text, id)
);


CREATE TABLE IF NOT EXISTS docs (
    id TEXT PRIMARY KEY,
    title TEXT,
    content TEXT,
    len INT
);


CREATE TABLE IF NOT EXISTS stats (
    name TEXT PRIMARY KEY,
    value FLOAT
);


CREATE TABLE IF NOT EXISTS terms (
    term_id TEXT PRIMARY KEY,
    term_text TEXT
);


