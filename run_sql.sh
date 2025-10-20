#!/bin/bash
cd "$(dirname "$0")"
echo "Running run_sql..."
wine "run_sql" || ./"run_sql" "$@"
