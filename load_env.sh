#!/bin/bash

if grep -q Microsoft /proc/version; then
    # Estamos no WSL
    echo "Detected WSL environment"
    source .env
    export BIGQUERY_KEYFILE=$BIGQUERY_KEYFILE_WSL
else
    # Estamos no Git Bash/Windows
    echo "Detected Git Bash environment"
    source .env
    export BIGQUERY_KEYFILE=$BIGQUERY_KEYFILE_WINDOWS
fi
