#!/bin/bash

# A script to download specific repository folders if they don't already exist.
# Works both inside Docker and locally by finding project root automatically.

echo "--- Starting Data Download Script ---"

# 1. Detect environment and set data directory accordingly
if [ -d "/app/data" ] && [ -w "/app/data" ]; then
    # Running inside Docker container
    DATA_DIR="/app/data"
    echo " Environment: Docker container"
    echo " Data directory: $DATA_DIR"
else
    # Running locally, find project root dynamically
    SCRIPT_DIR="$(dirname "$(realpath "$0")")"
    PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
    DATA_DIR="$PROJECT_ROOT/data"
    echo " Environment: Local execution"
    echo " Script location: $SCRIPT_DIR"
    echo " Project root: $PROJECT_ROOT"
    echo " Data directory: $DATA_DIR"
fi

# 2. Ensure data directory exists and navigate to it
mkdir -p "$DATA_DIR"
cd "$DATA_DIR"
echo " Working from data directory: $(pwd)"

# 3. Handle the 'llm_evaluation_for_gene_set_interpretation' data
LLM_DATA_DIR="llm_evaluation_for_gene_set_interpretation/data"

# Check if the directory is missing or empty
if [ ! -d "$LLM_DATA_DIR" ] || [ -z "$(ls -A "$LLM_DATA_DIR")" ]; then
    echo " 'llm_evaluation_data' not found. Downloading with Git..."
    TMP_DIR=$(mktemp -d)
    
    # Clone the repo structure, get only the 'data' folder
    git clone --filter=blob:none --no-checkout --depth 1 --sparse https://github.com/idekerlab/llm_evaluation_for_gene_set_interpretation.git "$TMP_DIR"
    (cd "$TMP_DIR" && git sparse-checkout set --cone data && git checkout)

    # Create the parent directory structure 
    mkdir -p "$(dirname "$LLM_DATA_DIR")"
    
    # Move the final folder to its destination and clean up
    mv "$TMP_DIR/data" "$LLM_DATA_DIR"
    rm -rf "$TMP_DIR"
    
    echo " Download complete."
else
    echo "'llm_evaluation_data' already exists. Skipping."
fi

# 3. Handle the 'talisman-paper' data
TALISMAN_DATA_DIR="talisman-paper/genesets/human"

# Check if the directory is missing or empty
if [ ! -d "$TALISMAN_DATA_DIR" ] || [ -z "$(ls -A "$TALISMAN_DATA_DIR")" ]; then
    echo " 'talisman_paper_data' not found. Downloading with Git..."
    TMP_DIR=$(mktemp -d)
    
    # Clone the repo structure, get only the 'genesets/human' folder
    git clone --filter=blob:none --no-checkout --depth 1 --sparse https://github.com/monarch-initiative/talisman-paper.git "$TMP_DIR"
    (cd "$TMP_DIR" && git sparse-checkout set --cone "genesets/human" && git checkout)

    # Create the parent directory structure (e.g., data/talisman-paper/genesets/)
    mkdir -p "$(dirname "$TALISMAN_DATA_DIR")"
    
    # Move the final folder to its destination and clean up
    mv "$TMP_DIR/genesets/human" "$TALISMAN_DATA_DIR"
    rm -rf "$TMP_DIR"

    echo " Download complete."
else
    echo "'talisman_paper_data' already exists. Skipping."
fi

echo "--- Script Finished ---"