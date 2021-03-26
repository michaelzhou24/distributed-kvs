#!/bin/bash

set -e
make

cmds=(
      "./tracing-server"
      "./frontend"
      "./storage"
      "./client"
      )

for cmd in "${cmds[@]}"; do {
  $cmd & pid=$!
  PID_LIST+=" $pid";
  sleep .1
} done

trap "kill $PID_LIST" SIGINT

wait $PID_LIST
