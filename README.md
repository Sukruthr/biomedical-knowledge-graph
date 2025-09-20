# Biomedical Knowledge Graph - Docker Setup

A containerized biomedical knowledge graph containing **126,416+ nodes** and **3.5+ million relationships** covering genes, diseases, drugs, pathways, and biological processes.

**Note**: If you prefer to run without Docker, skip the Docker sections and refer to the README inside `kg_scripts/` for direct installation and usage instructions.

## Quick Start

### Prerequisites
- Docker & Docker Compose
- 8GB+ RAM (16GB+ recommended)
- 20GB+ free disk space

### One-Command Setup
```bash
# Clone and setup
git clone <repository-url>
cd knowledge_graph
chmod +x biomedical-kg.sh
./biomedical-kg.sh
```
Choose option 1 (Load from dump) for fastest setup (~5 minutes).

## What's Included

### Data Scale
- **47,658** GO Terms (Gene Ontology)
- **42,238** Genes (human)
- **10,347** Diseases
- **11,875** Drugs
- **2,674** Pathways
- **4,392** Viral entities
- **126,416+ total nodes**

### Relationships
- **2.7M+** Gene Annotations
- **234K+** Drug Perturbations
- **134K+** Disease Associations
- **128K+** Viral Infections
- **3.5M+ total relationships**

## Management Commands

### Basic Operations
```bash
./biomedical-kg.sh start          # Start containers
./biomedical-kg.sh stop           # Stop containers
./biomedical-kg.sh status         # Show data statistics
./biomedical-kg.sh logs           # Monitor logs
```

### Data Management
```bash
./biomedical-kg.sh load-dump      # Load from dump (fast ~5min)
./biomedical-kg.sh build-scratch  # Build from source (~60min)
./biomedical-kg.sh wipe           # Clear all data
./biomedical-kg.sh reset          # Complete reset
```

### Backup & Restore
```bash
./biomedical-kg.sh create-dump    # Create backup
./docker-commands.sh backup-volumes   # Backup everything
```

### Custom Database Import
To import your own Neo4j database dump:
1. Place your dump file at: `kg_scripts/backups/neo4j.dump`
2. The filename must be exactly `neo4j.dump`
3. Run: `./biomedical-kg.sh load-dump`

## Access Points

### Neo4j Browser
- **URL**: http://localhost:7475
- **Username**: `neo4j`
- **Password**: `password`

### API Access
- **Bolt**: `bolt://localhost:7688`
- **Connection String**: `neo4j://localhost:7688`

## Docker Architecture

### Container Services

#### Main Database Service (`biomedical-kg`)
- **Base**: Neo4j 5.21.2 Enterprise
- **Purpose**: Core knowledge graph database
- **Ports**: 7475 (browser), 7688 (bolt)
- **Features**: APOC plugins, Graph Data Science library

#### Management Scripts
- **biomedical-kg.sh**: Main management script for all operations
- **docker-commands.sh**: Advanced Docker operations and troubleshooting

### Docker Files Overview

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Service orchestration |
| `Dockerfile` | Main Neo4j container |
| `docker/entrypoint.sh` | Database initialization |
| `.dockerignore` | Docker build optimization |

### Data Persistence
All data is stored in Docker volumes:
- `neo4j_data`: Database files
- `neo4j_logs`: Log files
- `neo4j_conf`: Configuration
- `kg_datasets`: External datasets (cached)

## Knowledge Graph Scripts (`kg_scripts/`)

### Core Build Pipeline
```bash
# Main knowledge graph builder
kg_scripts/build_complete_biomedical_kg.py

# Individual component builders
kg_scripts/go_kg_builder.py                    # Gene Ontology
kg_scripts/omics_disease_integration.py        # Disease data
kg_scripts/omics_drug_integration.py           # Drug data
kg_scripts/omics_pathway_integration.py        # Pathway data
kg_scripts/talisman_integration_engine.py      # Core integration
```

### Data Management Scripts
```bash
kg_scripts/download_data.sh                    # Download external datasets
kg_scripts/biomedical_kg_metrics.py           # Database statistics
```

### Configuration
```bash
kg_scripts/config/neo4j_config.py             # Docker-compatible Neo4j config
kg_scripts/environment.yml                    # Python environment
```

### External Data Sources
Scripts automatically download from:
- **LLM Evaluation Data**: [idekerlab/llm_evaluation_for_gene_set_interpretation](https://github.com/idekerlab/llm_evaluation_for_gene_set_interpretation)
- **Talisman Genesets**: [monarch-initiative/talisman-paper](https://github.com/monarch-initiative/talisman-paper)

## Setup Options

### Option 1: Load from Dump (Recommended)
Fast setup using pre-built database dump:

```bash
./biomedical-kg.sh load-dump
# ~5-10 minutes
```

### Option 2: Build from Scratch
Complete build from source data:

```bash
# Start containers
docker compose up -d

# Run complete build pipeline
docker exec biomedical-knowledge-graph python /app/kg_scripts/build_complete_biomedical_kg.py

# Monitor progress
docker logs -f biomedical-knowledge-graph
```

**Note**: Fresh builds download external data and take 30-60 minutes.

## Example Queries

```cypher
// Find genes associated with diabetes
MATCH (g:Gene)-[:ASSOCIATED_WITH]->(d:Disease {name: "diabetes"})
RETURN g.symbol, g.name LIMIT 10;

// Drug-gene interactions
MATCH (d:Drug)-[:TARGETS]->(g:Gene)
WHERE d.name CONTAINS "aspirin"
RETURN d.name, g.symbol, g.name;

// Gene Ontology enrichment
MATCH (g:Gene)-[:ANNOTATED_WITH]->(go:GOTerm)
WHERE go.namespace = "biological_process"
RETURN go.name, count(g) as gene_count
ORDER BY gene_count DESC LIMIT 20;

// Pathway analysis
MATCH (g:Gene)-[:MEMBER_OF]->(p:Pathway)
WHERE g.symbol IN ["TP53", "BRCA1", "EGFR"]
RETURN p.name, collect(g.symbol) as genes;
```

## Advanced Usage

### Using the Container for Scripts
```bash
# Access container shell
docker exec -it biomedical-knowledge-graph bash

# Run health check
docker exec biomedical-knowledge-graph python -c "
from config.neo4j_config import test_connection
success, message = test_connection()
print(f'Status: {success}, Message: {message}')
"

# Run specific analysis scripts
docker exec biomedical-knowledge-graph python /app/kg_scripts/biomedical_kg_metrics.py
```

### Memory Configuration
Adjust memory in `docker-compose.yml`:
```yaml
environment:
  - NEO4J_dbms_memory_heap_max__size=8G    # For systems with more RAM
  - NEO4J_dbms_memory_pagecache_size=4G
```

### Environment Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `NEO4J_AUTH` | `neo4j/password` | Database credentials |
| `NEO4J_dbms_memory_heap_max__size` | `4G` | Maximum heap memory |
| `NEO4J_dbms_memory_pagecache_size` | `2G` | Page cache size |

## Troubleshooting

### Common Issues

**Container won't start**
```bash
# Check available memory
docker system df
# Increase Docker memory limit to 8GB minimum
```

**Database loading fails**
```bash
# Check logs
docker compose logs biomedical-kg
# Ensure dump file exists
docker compose exec biomedical-kg ls -la /data/backups/
```

**Connection refused**
```bash
# Wait for initialization
docker compose logs -f biomedical-kg
# Check Neo4j is listening
docker compose exec biomedical-kg netstat -tlnp | grep 7687
```


### Health Checks
```bash
# Check container health
docker compose ps

# View logs
docker compose logs biomedical-kg

# Monitor resource usage
docker stats

```

## Database Statistics

```bash
# Get comprehensive stats
./biomedical-kg.sh status

# Detailed node/relationship counts
docker exec biomedical-knowledge-graph python -c "
from neo4j import GraphDatabase
driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'))
with driver.session() as session:
    result = session.run('CALL db.stats.retrieve(\"GRAPH COUNTS\")')
    for record in result:
        print(record)
driver.close()
"
```
