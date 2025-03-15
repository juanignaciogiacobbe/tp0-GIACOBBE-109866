#!/bin/bash

CONFIG_FILE="./server/config.ini"
MESSAGE="Hello server!"

SERVER_PORT=$(awk -F '=' '/SERVER_PORT/ {gsub(/ /, "", $2); print $2}' server/config.ini)
SERVER_IP=$(awk -F '=' '/SERVER_IP/ {gsub(/ /, "", $2); print $2}' server/config.ini)

if [ -z "$SERVER_IP" ] || [ -z "$SERVER_PORT" ]; then
  echo "action: test_echo_server | result: fail"
  echo "Error: SERVER_IP or SERVER_PORT not found in $CONFIG_FILE"
  exit 1
fi


response=$(docker run --rm --network tp0_testing_net busybox:latest sh -c "echo '$MESSAGE' | nc $SERVER_IP $SERVER_PORT")

if [ "$response" = "$MESSAGE" ]; then
    echo "action: test_echo_server | result: success"
else
    echo "action: test_echo_server | result: fail"
fi