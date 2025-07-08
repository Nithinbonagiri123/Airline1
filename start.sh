#!/bin/bash
echo 'killing existing tmux sessions...'
tmux kill-session -t producer 2>/dev/null
tmux kill-session -t consumer 2>/dev/null

echo 'killing existing docker containers...'
docker compose down

echo 'starting kafka...'
docker compose up -d

echo 'waiting for kafka to start...'
sleep 10

echo 'setup nlkt...'
python3 setup_nltk.py

echo 'starting producer...'
tmux new-session -d -s producer 'python3 producer/producer.py'

echo 'starting consumer...'
tmux new-session -d -s consumer 'python3 consumer/consumer.py'

tmux attach-session -d -s consumer