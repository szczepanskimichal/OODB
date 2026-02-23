#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:5150}"
FILE="${1:-events.jsonl}"

currentStudentId=""

echo "Importing from: $FILE -> $BASE_URL/events"
echo "-------------------------------------------"

while IFS= read -r line; do
  [ -z "$line" ] && continue

  type=$(echo "$line" | jq -r '.type')

  if [ "$type" = "student_registrert" ]; then
    # wysyłamy bez studentId
    resp=$(curl -sS -X POST "$BASE_URL/events" \
      -H "Content-Type: application/json" \
      -d "$line")

    ok=$(echo "$resp" | jq -r '.ok')
    if [ "$ok" = "true" ]; then
      currentStudentId=$(echo "$resp" | jq -r '.studentId')
      echo "student_registrert -> studentId=$currentStudentId"
    else
      # duplicate_event może też zwrócić studentId
      sid=$(echo "$resp" | jq -r '.studentId // empty')
      if [ -n "$sid" ]; then
        currentStudentId="$sid"
        echo "duplicate student_registrert -> reuse studentId=$currentStudentId"
      else
        echo "WARN: student_registrert not ok: $resp"
      fi
    fi

  else
    # event kursowy: musi dostać studentId
    if [ -z "$currentStudentId" ]; then
      echo "ERROR: kurs-event før student_registrert. Line:"
      echo "$line"
      exit 1
    fi

    payload=$(echo "$line" | jq --arg sid "$currentStudentId" '. + {studentId: $sid}')
    resp=$(curl -sS -X POST "$BASE_URL/events" \
      -H "Content-Type: application/json" \
      -d "$payload")

    echo "event $type -> $(echo "$resp" | jq -c '.')"
  fi
done < "$FILE"

echo "DONE."
