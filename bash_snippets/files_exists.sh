#!/bin/bash

if [[ $# -eq 0 ]]; then 
  echo "Usage: $0 filename"
  exit 1
fi

filename=$1

if [[ -f "$filename" ]]; then
  size=$(stat -c '%s' "$filename")
  mtime=$(stat -c '%y' "$filename")
  perms=$(stat -c '%A' "$filename")

  echo "File: $filename"
  echo "Size: $size bytes"
  echo "Modified $mtime"
  echo "Permissions: $perms"
else
  echo "Error: File '$filename' not found"
fi
