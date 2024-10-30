#!/bin/bash

max_attempts=5

for ((i = 1; i <= max_attempts; i++)); do
  # command that needs to check result
  go test

  if [ $? -eq 0 ]; then
    rm *.log
  else
    echo "Failed! check log"
    exit 1
  fi
done

echo "clear"
