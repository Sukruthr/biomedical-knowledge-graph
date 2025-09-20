#!/bin/bash

# Docker Commands for Knowledge Graph Management
# This script contains all essential Docker commands for the biomedical knowledge graph

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper function for colored output
log() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

header() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
}

case "${1:-help}" in

    # CONTAINER MANAGEMENT
    "start")
        header "Starting Knowledge Graph Containers"
        log "Starting containers with docker-compose..."
        docker compose up -d
        log "Containers started successfully!"
        ;;

    "stop")
        header "Stopping Knowledge Graph Containers"
        log "Stopping all containers..."
        docker compose down
        log "Containers stopped successfully!"
        ;;

    "restart")
        header "Restarting Knowledge Graph Containers"
        log "Stopping containers..."
        docker compose down
        log "Starting containers..."
        docker compose up -d
        log "Containers restarted successfully!"
        ;;

    "logs")
        header "Following Container Logs"
        log "Showing logs for biomedical-knowledge-graph..."
        docker logs -f biomedical-knowledge-graph
        ;;

    "logs-all")
        header "Following All Container Logs"
        log "Showing logs for all services..."
        docker compose logs -f
        ;;

    # IMAGE MANAGEMENT
    "build")
        header "Building Docker Images"
        log "Building images from scratch (no cache)..."
        docker compose build --no-cache
        log "Images built successfully!"
        ;;

    "build-cache")
        header "Building Docker Images (with cache)"
        log "Building images with cache..."
        docker compose build
        log "Images built successfully!"
        ;;

    "pull")
        header "Pulling Latest Base Images"
        log "Pulling latest Neo4j and micromamba images..."
        docker pull neo4j:5.21.2-enterprise
        docker pull mambaorg/micromamba:1.5.1
        log "Base images updated!"
        ;;

    # VOLUME MANAGEMENT
    "volumes")
        header "Docker Volumes Information"
        log "Knowledge Graph volumes:"
        docker volume ls | grep knowledge_graph || echo "No knowledge graph volumes found"
        echo ""
        log "Volume details:"
        docker volume inspect knowledge_graph_neo4j_data 2>/dev/null || echo "neo4j_data volume not found"
        ;;

    "volume-cleanup")
        header "Cleaning Up Volumes"
        warn "This will delete all Neo4j data!"
        read -p "Are you sure? (yes/no): " confirm
        if [ "$confirm" = "yes" ]; then
            log "Stopping containers first..."
            docker compose down
            log "Removing knowledge graph volumes..."
            docker volume rm knowledge_graph_neo4j_data knowledge_graph_neo4j_logs knowledge_graph_neo4j_conf 2>/dev/null || true
            log "Volumes cleaned up!"
        else
            log "Operation cancelled"
        fi
        ;;

    # SYSTEM CLEANUP
    "cleanup")
        header "Docker System Cleanup"
        warn "This will remove unused Docker resources"
        read -p "Proceed with cleanup? (yes/no): " confirm
        if [ "$confirm" = "yes" ]; then
            log "Cleaning up unused containers, networks, images..."
            docker system prune -f
            log "Basic cleanup completed!"
        else
            log "Cleanup cancelled"
        fi
        ;;

    "deep-cleanup")
        header "Deep Docker Cleanup"
        warn "This will remove ALL unused Docker resources including volumes!"
        read -p "Proceed with DEEP cleanup? (yes/no): " confirm
        if [ "$confirm" = "yes" ]; then
            log "Performing deep cleanup..."
            docker system prune -af --volumes
            log "Deep cleanup completed!"
        else
            log "Deep cleanup cancelled"
        fi
        ;;

    # CONTAINER INSPECTION
    "status")
        header "Container Status"
        log "Running containers:"
        docker ps | grep -E "(CONTAINER|biomedical|knowledge)" || echo "No knowledge graph containers running"
        echo ""
        log "All knowledge graph containers (including stopped):"
        docker ps -a | grep -E "(CONTAINER|biomedical|knowledge)" || echo "No knowledge graph containers found"
        ;;

    "inspect")
        header "Container Inspection"
        if docker ps | grep -q biomedical-knowledge-graph; then
            log "Container details:"
            docker inspect biomedical-knowledge-graph | jq '.[] | {Name: .Name, Status: .State.Status, Image: .Config.Image, Ports: .NetworkSettings.Ports}'
        else
            error "Container 'biomedical-knowledge-graph' not found or not running"
        fi
        ;;

    "stats")
        header "Container Resource Usage"
        log "Resource usage for running containers:"
        docker stats --no-stream | grep -E "(CONTAINER|biomedical|knowledge)" || echo "No knowledge graph containers running"
        ;;

    # NETWORK MANAGEMENT
    "networks")
        header "Docker Networks"
        log "Knowledge graph networks:"
        docker network ls | grep -E "(NETWORK|knowledge)" || echo "No knowledge graph networks found"
        ;;

    "network-inspect")
        header "Network Inspection"
        if docker network ls | grep -q knowledge_graph_kg_network; then
            log "Network details:"
            docker network inspect knowledge_graph_kg_network
        else
            error "Network 'knowledge_graph_kg_network' not found"
        fi
        ;;

    # CONTAINER SHELL ACCESS
    "shell")
        header "Container Shell Access"
        if docker ps | grep -q biomedical-knowledge-graph; then
            log "Opening bash shell in biomedical-knowledge-graph container..."
            docker exec -it biomedical-knowledge-graph bash
        else
            error "Container 'biomedical-knowledge-graph' not running"
        fi
        ;;

    "shell-root")
        header "Container Root Shell Access"
        if docker ps | grep -q biomedical-knowledge-graph; then
            log "Opening root bash shell in biomedical-knowledge-graph container..."
            docker exec -it --user root biomedical-knowledge-graph bash
        else
            error "Container 'biomedical-knowledge-graph' not running"
        fi
        ;;

    # NEO4J SPECIFIC COMMANDS
    "neo4j-shell")
        header "Neo4j Cypher Shell"
        if docker ps | grep -q biomedical-knowledge-graph; then
            log "Opening Neo4j cypher-shell..."
            docker exec -it biomedical-knowledge-graph cypher-shell -u neo4j -p password
        else
            error "Container 'biomedical-knowledge-graph' not running"
        fi
        ;;

    "neo4j-logs")
        header "Neo4j Specific Logs"
        if docker ps | grep -q biomedical-knowledge-graph; then
            log "Showing Neo4j logs..."
            docker exec biomedical-knowledge-graph tail -f /var/lib/neo4j/logs/neo4j.log
        else
            error "Container 'biomedical-knowledge-graph' not running"
        fi
        ;;

    # BACKUP AND RESTORE
    "backup-volumes")
        header "Backup Docker Volumes"
        timestamp=$(date +%Y%m%d_%H%M%S)
        backup_dir="./backups/volumes_${timestamp}"
        mkdir -p "$backup_dir"

        log "Creating volume backups in $backup_dir..."

        # Backup Neo4j data
        docker run --rm \
            -v knowledge_graph_neo4j_data:/data \
            -v "$(pwd)/$backup_dir":/backup \
            alpine \
            tar czf /backup/neo4j_data.tar.gz -C /data .

        # Backup Neo4j logs
        docker run --rm \
            -v knowledge_graph_neo4j_logs:/logs \
            -v "$(pwd)/$backup_dir":/backup \
            alpine \
            tar czf /backup/neo4j_logs.tar.gz -C /logs .

        # Backup Neo4j config
        docker run --rm \
            -v knowledge_graph_neo4j_conf:/conf \
            -v "$(pwd)/$backup_dir":/backup \
            alpine \
            tar czf /backup/neo4j_conf.tar.gz -C /conf .

        log "Volume backups created in $backup_dir/"
        ;;

    "restore-volumes")
        header "Restore Docker Volumes"
        if [ -z "$2" ]; then
            error "Please specify backup directory: $0 restore-volumes <backup_dir>"
            exit 1
        fi

        backup_dir="$2"
        if [ ! -d "$backup_dir" ]; then
            error "Backup directory not found: $backup_dir"
            exit 1
        fi

        warn "This will overwrite existing volume data!"
        read -p "Continue? (yes/no): " confirm
        if [ "$confirm" = "yes" ]; then
            log "Stopping containers..."
            docker compose down

            log "Restoring volumes from $backup_dir..."

            # Restore Neo4j data
            if [ -f "$backup_dir/neo4j_data.tar.gz" ]; then
                docker run --rm \
                    -v knowledge_graph_neo4j_data:/data \
                    -v "$(pwd)/$backup_dir":/backup \
                    alpine \
                    tar xzf /backup/neo4j_data.tar.gz -C /data
                log "Neo4j data restored"
            fi

            # Restore Neo4j logs
            if [ -f "$backup_dir/neo4j_logs.tar.gz" ]; then
                docker run --rm \
                    -v knowledge_graph_neo4j_logs:/logs \
                    -v "$(pwd)/$backup_dir":/backup \
                    alpine \
                    tar xzf /backup/neo4j_logs.tar.gz -C /logs
                log "Neo4j logs restored"
            fi

            # Restore Neo4j config
            if [ -f "$backup_dir/neo4j_conf.tar.gz" ]; then
                docker run --rm \
                    -v knowledge_graph_neo4j_conf:/conf \
                    -v "$(pwd)/$backup_dir":/backup \
                    alpine \
                    tar xzf /backup/neo4j_conf.tar.gz -C /conf
                log "Neo4j config restored"
            fi

            log "Starting containers..."
            docker compose up -d
            log "Volume restore completed!"
        else
            log "Restore cancelled"
        fi
        ;;

    # TROUBLESHOOTING
    "debug")
        header "Debug Information"
        log "Docker version:"
        docker --version
        echo ""
        log "Docker Compose version:"
        docker compose version
        echo ""
        log "System resources:"
        docker system df
        echo ""
        log "Container status:"
        docker ps -a | grep -E "(biomedical|knowledge)" || echo "No knowledge graph containers"
        echo ""
        log "Volume status:"
        docker volume ls | grep knowledge_graph || echo "No knowledge graph volumes"
        echo ""
        log "Network status:"
        docker network ls | grep knowledge_graph || echo "No knowledge graph networks"
        ;;

    "health")
        header "Health Check"
        if docker ps | grep -q biomedical-knowledge-graph; then
            log "Testing Neo4j connectivity..."
            if curl -s http://localhost:7475/ > /dev/null; then
                log " Neo4j HTTP interface accessible"
            else
                error " Neo4j HTTP interface not accessible"
            fi

            log "Testing database connection..."
            if docker exec biomedical-knowledge-graph cypher-shell -u neo4j -p password "RETURN 1" > /dev/null 2>&1; then
                log " Database connection working"
            else
                error " Database connection failed"
            fi
        else
            error "Container not running"
        fi
        ;;

    # COMPLETE RESET
    "nuclear")
        header "Nuclear Reset (Complete Cleanup)"
        warn "This will:"
        echo "  - Stop all containers"
        echo "  - Remove all containers"
        echo "  - Remove all volumes"
        echo "  - Remove all networks"
        echo "  - Remove all images"
        echo "  - Perform system cleanup"
        echo ""
        warn "This is irreversible!"
        read -p "Type 'NUCLEAR' to proceed: " confirm

        if [ "$confirm" = "NUCLEAR" ]; then
            log "Performing nuclear reset..."

            log "Stopping all containers..."
            docker compose down --remove-orphans

            log "Removing containers..."
            docker container rm -f $(docker container ls -aq --filter "name=biomedical" --filter "name=knowledge") 2>/dev/null || true

            log "Removing volumes..."
            docker volume rm knowledge_graph_neo4j_data knowledge_graph_neo4j_logs knowledge_graph_neo4j_conf knowledge_graph_kg_datasets 2>/dev/null || true

            log "Removing networks..."
            docker network rm knowledge_graph_kg_network 2>/dev/null || true

            log "Removing images..."
            docker image rm knowledge_graph-biomedical-kg knowledge_graph-kg-tools 2>/dev/null || true

            log "System cleanup..."
            docker system prune -af --volumes

            log "Nuclear reset completed! Everything has been wiped clean."
            log "Run './biomedical-kg.sh build' or 'docker compose up -d' to start fresh."
        else
            log "Nuclear reset cancelled"
        fi
        ;;

    # HELP
    *)
        header "Docker Commands for Knowledge Graph"
        echo ""
        echo "Container Management:"
        echo "  start           - Start containers"
        echo "  stop            - Stop containers"
        echo "  restart         - Restart containers"
        echo "  logs            - Follow main container logs"
        echo "  logs-all        - Follow all container logs"
        echo ""
        echo "Image Management:"
        echo "  build           - Build images (no cache)"
        echo "  build-cache     - Build images (with cache)"
        echo "  pull            - Pull latest base images"
        echo ""
        echo "Volume Management:"
        echo "  volumes         - Show volume information"
        echo "  volume-cleanup  - Delete all volumes"
        echo "  backup-volumes  - Backup volumes to files"
        echo "  restore-volumes - Restore volumes from backup"
        echo ""
        echo "System Cleanup:"
        echo "  cleanup         - Clean unused Docker resources"
        echo "  deep-cleanup    - Deep clean (includes volumes)"
        echo ""
        echo "Inspection:"
        echo "  status          - Show container status"
        echo "  inspect         - Detailed container inspection"
        echo "  stats           - Resource usage statistics"
        echo "  networks        - Show networks"
        echo "  network-inspect - Inspect network details"
        echo ""
        echo "Access:"
        echo "  shell           - Open bash shell in container"
        echo "  shell-root      - Open root shell in container"
        echo "  neo4j-shell     - Open Neo4j cypher shell"
        echo "  neo4j-logs      - Follow Neo4j logs"
        echo ""
        echo "Troubleshooting:"
        echo "  debug           - Show debug information"
        echo "  health          - Health check"
        echo ""
        echo "Nuclear Option:"
        echo "  nuclear         - Complete reset (removes everything)"
        echo ""
        echo "Examples:"
        echo "  $0 start                           # Start containers"
        echo "  $0 build                           # Build fresh images"
        echo "  $0 shell                           # Access container"
        echo "  $0 backup-volumes                  # Backup data"
        echo "  $0 restore-volumes ./backups/xxx   # Restore data"
        ;;
esac