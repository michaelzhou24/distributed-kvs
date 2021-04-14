#!/bin/bash

set -e
make

cmds=(
      "./tracing-server"
      "./frontend"
      "./storage"
      "./storage -config=config/storage2_config.json"
      "./client"
      )

if [[ -d tmp ]]; then
  echo "Removing tmp"
  rm -rf tmp
fi

for cmd in "${cmds[@]}"; do {
  $cmd & pid=$!
  PID_LIST+=" $pid";
  sleep .1
} done

trap "kill $PID_LIST" SIGINT

wait $PID_LIST
