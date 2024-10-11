if [[ $# -eq 0 ]]; then
  echo "Usage: $0 filename"
  exit 1
fi

filename="$1"

if [[ -f "$filename" ]]; then

  if [[ -r "$filename" ]]; then 
      echo "$filename is readable"

      if grep -q "keyword" "$filename"; then
        echo "$filename contains the keyword 'keyword'
      else
        echo "$filename does not contain the keyword 'keyword'
      fi
else 
  echo "$filename does not exist."
fi
