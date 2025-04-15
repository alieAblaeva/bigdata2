#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz

bash create_tables.sh

# Collect data
bash prepare_data.sh

# Run the indexer
bash index.sh

# Run the ranker
bash search.sh "this is a query!"

echo "Services started. Holding the container open..."
tail -f /dev/null
