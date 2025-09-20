#!/bin/bash
set -e

# Biomedical Knowledge Graph Docker Entrypoint
# This script handles initial database setup and loading

echo "Starting Biomedical Knowledge Graph setup..."

# Function to wait for Neo4j to be ready
wait_for_neo4j() {
    echo "Waiting for Neo4j to be ready..."
    until curl -s http://localhost:7474/db/manage/server/info > /dev/null 2>&1; do
        echo "Neo4j not ready yet, waiting..."
        sleep 5
    done
    echo "Neo4j is ready!"
}

# Function to check if database needs to be loaded
needs_database_load() {
    # Check if this is the first run by looking for a marker file
    if [ ! -f /data/.kg_initialized ]; then
        return 0  # true - needs loading
    else
        return 1  # false - already loaded
    fi
}

# Function to download required datasets
download_datasets() {

    # 2. Clean up logs folder if it exists
    LOGS_DIR="/app/kg_scripts/logs"
    if [ -d "$LOGS_DIR" ]; then
        echo " Found logs directory: $LOGS_DIR"
        echo " Removing logs directory..."
        rm -rf "$LOGS_DIR"
        echo " Logs directory removed."
    fi
        echo "Downloading required datasets..."

    # Create data directory with proper permissions
    mkdir -p /app/data
    chown neo4j:neo4j /app/data

    # Change to data directory for downloads
    cd /app/data

    # Check if data already exists to avoid re-downloading
    if [ -d "llm_evaluation_for_gene_set_interpretation/data" ] && [ -d "talisman-paper/genesets/human" ]; then
        echo " Datasets already downloaded, skipping..."
        return 0
    fi

    # Download datasets directly to /app/data
    echo " Downloading datasets..."

    # Download LLM evaluation data
    if [ ! -d "llm_evaluation_for_gene_set_interpretation/data" ]; then
        echo "Downloading LLM evaluation data..."
        TMP_DIR=$(mktemp -d)
        git clone --filter=blob:none --no-checkout --depth 1 --sparse https://github.com/idekerlab/llm_evaluation_for_gene_set_interpretation.git "$TMP_DIR"
        (cd "$TMP_DIR" && git sparse-checkout set --cone data && git checkout)
        mkdir -p llm_evaluation_for_gene_set_interpretation
        mv "$TMP_DIR/data" llm_evaluation_for_gene_set_interpretation/
        rm -rf "$TMP_DIR"
        echo " LLM evaluation data downloaded"
    fi

    # Download Talisman data
    if [ ! -d "talisman-paper/genesets/human" ]; then
        echo "Downloading Talisman data..."
        TMP_DIR=$(mktemp -d)
        git clone --filter=blob:none --no-checkout --depth 1 --sparse https://github.com/monarch-initiative/talisman-paper.git "$TMP_DIR"
        (cd "$TMP_DIR" && git sparse-checkout set --cone "genesets/human" && git checkout)
        mkdir -p talisman-paper/genesets
        mv "$TMP_DIR/genesets/human" talisman-paper/genesets/
        rm -rf "$TMP_DIR"
        echo " Talisman data downloaded"
    fi

    # Set proper ownership
    chown -R neo4j:neo4j /app/data

    echo " Dataset download completed successfully!"
    return 0
}

# Function to load database from dump
load_database() {
    echo "Loading biomedical knowledge graph from dump..."

    # Stop Neo4j if it's running
    neo4j stop || true

    # Load the database dump
    if [ -f "/var/lib/neo4j/import/biomedical-kg.dump" ]; then
        echo "Loading database dump..."
        neo4j-admin database load neo4j --from-path=/var/lib/neo4j/import --overwrite-destination=true

        # Create initialization marker
        touch /data/.kg_initialized
        echo "Database loaded successfully!"
    else
        echo "Warning: No database dump found at /var/lib/neo4j/import/biomedical-kg.dump"
        echo "Starting with empty database..."
        touch /data/.kg_initialized
    fi
}

# Function to start Neo4j in background and wait for it
start_neo4j_background() {
    echo "Starting Neo4j in background..."
    neo4j start
    wait_for_neo4j
}

# Function to run post-load setup (indexes, constraints, etc.)
setup_database() {
    echo "Setting up database indexes and constraints..."

    # Wait a bit more for database to be fully ready
    sleep 10

    # Run any post-load setup scripts
    if [ -f "/app/kg_scripts/post_load_setup.cypher" ]; then
        echo "Running post-load setup queries..."
        cypher-shell -u neo4j -p biomedical_kg_password -f /app/kg_scripts/post_load_setup.cypher || true
    fi

    echo "Database setup completed!"
}

# Main execution flow
main() {
    # Download required datasets first
    echo "Checking and downloading required datasets..."
    if ! download_datasets; then
        echo " Failed to download datasets, continuing anyway..."
    fi

    # For now, skip database loading and just start Neo4j
    echo "Starting Neo4j with empty database..."
    touch /data/.kg_initialized

    # Start Neo4j normally (foreground)
    echo "Starting Neo4j in foreground mode..."
    exec /startup/docker-entrypoint.sh "$@"
}

# Run main function
main "$@"