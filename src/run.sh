#!/bin/bash

DIRECTORY="../data"

for file in "$DIRECTORY"/*Monthly.csv; do
    if [ -f "$file" ]; then
        python main.py "$file"
    fi
done
