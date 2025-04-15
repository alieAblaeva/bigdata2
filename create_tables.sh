#!/bin/bash

until cqlsh cassandra-server -e 'DESCRIBE KEYSPACES'; do
  sleep 5
done

cqlsh cassandra-server -f /app/cassandra_create_tables.cql
