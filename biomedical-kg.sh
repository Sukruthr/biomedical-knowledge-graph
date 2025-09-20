#!/bin/bash

# Knowledge Graph Management Script with Multiple Options
# Supports building from scratch or loading from dump

set -e

# Function to clean up logs folder inside Docker container
cleanup_logs() {
    echo "Checking for logs folder in container..."

    if ! docker ps | grep -q biomedical-knowledge-graph; then
        echo " Container not running. Start it first with './biomedical-kg.sh start'"
        return 1
    fi

    if docker exec biomedical-knowledge-graph test -d /app/kg_scripts/logs; then
        echo " Found logs directory: /app/kg_scripts/logs"
        read -p " Remove logs folder? (y/n): " response

        case "$response" in
            [yY]|[yY][eE][sS])
                echo " Removing logs directory..."
                docker exec biomedical-knowledge-graph rm -rf /app/kg_scripts/logs
                echo " Logs directory removed."
                ;;
            *)
                echo " Logs directory kept."
                ;;
        esac
    else
        echo " No logs directory found in container."
    fi
}

# Function to wait for Neo4j
wait_for_neo4j() {
    echo "Waiting for Neo4j to be ready..."
    local max_attempts=36
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if docker exec biomedical-knowledge-graph curl -s http://localhost:7474/ > /dev/null 2>&1; then
            echo "Neo4j is ready!"
            return 0
        fi
        echo "   Attempt $attempt/$max_attempts..."
        sleep 2
        ((attempt++))
    done

    echo "Neo4j failed to start"
    return 1
}

# Function to check if KG already exists
kg_exists() {
    local count=$(docker exec biomedical-knowledge-graph cypher-shell -u neo4j -p password \
        "MATCH (n:GOTerm) RETURN count(n) as count" 2>/dev/null | tail -n +2 | head -n 1 || echo "0")
    [ "$count" -gt 0 ] 2>/dev/null
}

# Function to load from dump
load_from_dump() {
    echo "Loading knowledge graph from dump..."

    # Check if dump exists (check both possible names)
    if ! docker exec biomedical-knowledge-graph test -f /var/lib/neo4j/import/neo4j.dump; then
        if docker exec biomedical-knowledge-graph test -f /var/lib/neo4j/import/biomedical-kg.dump; then
            echo "Renaming dump file to expected format..."
            docker exec --user root biomedical-knowledge-graph cp /var/lib/neo4j/import/biomedical-kg.dump /var/lib/neo4j/import/neo4j.dump
            docker exec --user root biomedical-knowledge-graph chown neo4j:neo4j /var/lib/neo4j/import/neo4j.dump
        else
            echo "No dump file found at /var/lib/neo4j/import/"
            echo "   You can:"
            echo "   1. Place your dump file in ./kg_scripts/backups/"
            echo "   2. Or use './biomedical-kg.sh build-scratch' to build from source data"
            return 1
        fi
    fi

    # Stop containers completely for offline loading
    echo "Stopping containers for offline dump loading..."
    docker compose down

    echo "Loading dump file using offline method..."
    # Use the built image name (same pattern as docker-compose.yml)
    BUILT_IMAGE="knowledge_graph-biomedical-kg"

    # Start container in a way that we can run neo4j-admin without neo4j running
    docker run --rm \
        --volume knowledge_graph_neo4j_data:/data \
        --volume knowledge_graph_neo4j_logs:/logs \
        --volume knowledge_graph_neo4j_conf:/conf \
        --volume ./kg_scripts/backups:/var/lib/neo4j/import:ro \
        --env NEO4J_ACCEPT_LICENSE_AGREEMENT=eval \
        --user neo4j \
        $BUILT_IMAGE \
        neo4j-admin database load neo4j --from-path=/var/lib/neo4j/import --overwrite-destination=true

    echo "Starting containers..."
    docker compose up -d

    if ! wait_for_neo4j; then
        echo "Failed to start Neo4j after loading dump"
        return 1
    fi

    echo "Knowledge graph loaded from dump!"
}

# Function to build from scratch
build_from_scratch() {
    echo "Building knowledge graph from scratch..."
    echo "   This will take 30-60 minutes depending on your system..."

    # First, ensure all external data is downloaded
    echo "Downloading external datasets..."
    docker exec biomedical-knowledge-graph /app/kg_scripts/download_data.sh

    echo "Starting knowledge graph build..."
    echo "   Monitor progress: docker logs -f biomedical-knowledge-graph"

    docker exec biomedical-knowledge-graph python /app/kg_scripts/build_complete_biomedical_kg.py
    echo "Knowledge graph built from scratch!"
}

# Interactive mode for build choice
interactive_build() {
    echo "How would you like to set up the knowledge graph?"
    echo ""
    echo "1) Load from dump (fast, ~2-5 minutes)"
    echo "2) Build from scratch (slow, ~30-60 minutes)"
    echo "3) Cancel"
    echo ""
    read -p "Choose option (1-3): " choice

    case $choice in
        1)
            load_from_dump
            ;;
        2)
            build_from_scratch
            ;;
        3)
            echo "Cancelled"
            exit 0
            ;;
        *)
            echo "Invalid choice. Please select 1, 2, or 3."
            exit 1
            ;;
    esac
}

case "${1:-build}" in
    "start")
        echo "Starting containers..."
        docker compose up -d
        ;;

    "stop")
        echo " Stopping containers..."
        docker compose down
        ;;

    "build"|"")
        echo "  Setting up Knowledge Graph..."

        # Start if not running
        if ! docker ps | grep -q biomedical-knowledge-graph; then
            echo " Starting containers first..."
            docker compose up -d
        fi

        # Wait for Neo4j
        if ! wait_for_neo4j; then
            exit 1
        fi

        # Check if KG already exists
        if kg_exists; then
            echo " Knowledge graph already exists!"
            echo "   Use './biomedical-kg.sh rebuild' to rebuild or './biomedical-kg.sh status' to check data"
            exit 0
        fi

        # Interactive choice
        interactive_build
        echo " Neo4j Browser: http://localhost:7475"
        echo " Credentials: neo4j/password"
        ;;

    "load-dump")
        echo " Loading from dump..."

        if ! docker ps | grep -q biomedical-knowledge-graph; then
            echo " Starting containers first..."
            docker compose up -d
            wait_for_neo4j
        fi

        load_from_dump
        echo " Neo4j Browser: http://localhost:7475"
        ;;

    "build-scratch")
        echo "  Building from scratch..."

        if ! docker ps | grep -q biomedical-knowledge-graph; then
            echo " Starting containers first..."
            docker compose up -d
            wait_for_neo4j
        fi

        build_from_scratch
        echo " Neo4j Browser: http://localhost:7475"
        ;;

    "rebuild")
        echo " Rebuilding knowledge graph..."

        if ! docker ps | grep -q biomedical-knowledge-graph; then
            echo " Starting containers first..."
            docker compose up -d
            wait_for_neo4j
        fi

        # Clear existing data
        echo "  Clearing existing data..."
        docker exec biomedical-knowledge-graph cypher-shell -u neo4j -p password \
            "MATCH (n) DETACH DELETE n" 2>/dev/null || true

        interactive_build
        echo " Neo4j Browser: http://localhost:7475"
        ;;

    "status")
        docker ps | grep biomedical || echo " Container not running"
        if docker ps | grep -q biomedical-knowledge-graph; then
            echo " Database content:"
            docker exec biomedical-knowledge-graph cypher-shell -u neo4j -p password \
                "MATCH (n) RETURN labels(n)[0] as type, count(n) as count ORDER BY count DESC LIMIT 10" 2>/dev/null || echo " Database not ready"
        fi
        ;;

    "logs")
        docker logs -f biomedical-knowledge-graph
        ;;

    "cleanup-logs")
        cleanup_logs
        ;;

    "create-dump")
        echo " Creating database dump..."
        if ! docker ps | grep -q biomedical-knowledge-graph; then
            echo " Container not running. Start it first with './biomedical-kg.sh start'"
            exit 1
        fi

        timestamp=$(date +%Y%m%d_%H%M%S)
        dump_name="biomedical-kg-backup-${timestamp}.dump"

        docker exec biomedical-knowledge-graph neo4j stop
        docker exec biomedical-knowledge-graph neo4j-admin database dump neo4j \
            --to-path=/var/lib/neo4j/import --verbose
        docker exec biomedical-knowledge-graph mv /var/lib/neo4j/import/neo4j.dump \
            "/var/lib/neo4j/import/${dump_name}"
        docker exec biomedical-knowledge-graph neo4j start

        echo " Dump created: ${dump_name}"
        echo "   Location: ./kg_scripts/backups/${dump_name}"
        ;;

    "wipe"|"clean")
        echo " Wiping knowledge graph clean..."

        if ! docker ps | grep -q biomedical-knowledge-graph; then
            echo " Container not running. Start it first with './biomedical-kg.sh start'"
            exit 1
        fi

        # Confirm deletion
        echo "  WARNING: This will permanently delete ALL data in the knowledge graph!"
        echo "   This includes all nodes, relationships, and indexes."
        echo ""
        read -p "Are you sure you want to proceed? (yes/no): " confirm

        if [ "$confirm" != "yes" ]; then
            echo " Operation cancelled"
            exit 0
        fi

        echo "  Deleting all data..."

        # Delete all nodes and relationships
        echo "   Removing all nodes and relationships..."
        docker exec biomedical-knowledge-graph cypher-shell -u neo4j -p password \
            "MATCH (n) DETACH DELETE n" 2>/dev/null || true

        # Drop all indexes and constraints
        echo "   Dropping indexes and constraints..."
        docker exec biomedical-knowledge-graph cypher-shell -u neo4j -p password \
            "CALL apoc.schema.assert({},{}, true)" 2>/dev/null || true

        # Verify cleanup
        local count=$(docker exec biomedical-knowledge-graph cypher-shell -u neo4j -p password \
            "MATCH (n) RETURN count(n) as count" 2>/dev/null | tail -n +2 | head -n 1 || echo "0")

        if [ "$count" = "0" ]; then
            echo " Knowledge graph wiped clean!"
            echo "   Database is now empty and ready for new data"
            echo "   Use './biomedical-kg.sh load-dump' or './biomedical-kg.sh build-scratch' to reload data"
        else
            echo "  Cleanup may not be complete. $count nodes remaining."
            echo "   You may need to restart Neo4j: './biomedical-kg.sh stop && ./biomedical-kg.sh start'"
        fi
        ;;

    "reset")
        echo " Resetting knowledge graph (complete reset)..."

        echo "  WARNING: This will:"
        echo "   - Stop all containers"
        echo "   - Delete ALL data volumes (complete reset)"
        echo "   - Remove all Neo4j data, logs, and configuration"
        echo "   - You will need to reload data afterwards"
        echo ""
        read -p "Are you sure you want to proceed? (yes/no): " confirm

        if [ "$confirm" != "yes" ]; then
            echo " Operation cancelled"
            exit 0
        fi

        echo " Stopping containers..."
        docker compose down

        echo "  Removing data volumes..."
        docker volume rm knowledge_graph_neo4j_data knowledge_graph_neo4j_logs knowledge_graph_neo4j_conf 2>/dev/null || true

        echo " Starting fresh containers..."
        docker compose up -d

        if ! wait_for_neo4j; then
            echo " Failed to start Neo4j after reset"
            exit 1
        fi

        echo " Knowledge graph reset complete!"
        echo "   Fresh Neo4j instance ready with neo4j/password"
        echo "   Use './biomedical-kg.sh load-dump' or './biomedical-kg.sh build-scratch' to load data"
        ;;

    *)
        echo "Knowledge Graph Management Tool"
        echo "==============================="
        echo ""
        echo "Usage: $0 [command]"
        echo ""
        echo "Setup Commands:"
        echo "  build          - Interactive setup (choose dump vs scratch)"
        echo "  load-dump      - Load from existing dump file (fast)"
        echo "  build-scratch  - Build from source data (slow)"
        echo "  rebuild        - Clear and rebuild (interactive choice)"
        echo ""
        echo "Management Commands:"
        echo "  start          - Start containers only"
        echo "  stop           - Stop containers"
        echo "  status         - Show status and data summary"
        echo "  logs           - Follow container logs"
        echo "  cleanup-logs   - Remove logs folder from container"
        echo "  create-dump    - Create backup dump file"
        echo ""
        echo "Cleanup Commands:"
        echo "  wipe/clean     - Delete all data (keep container/config)"
        echo "  reset          - Complete reset (delete volumes, fresh start)"
        echo ""
        echo "Examples:"
        echo "  ./biomedical-kg.sh                    # Interactive setup"
        echo "  ./biomedical-kg.sh load-dump          # Quick load from dump"
        echo "  ./biomedical-kg.sh build-scratch      # Full build (slow)"
        echo "  ./biomedical-kg.sh status             # Check what's loaded"
        ;;
esac