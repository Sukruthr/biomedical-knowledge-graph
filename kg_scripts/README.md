# Biomedical Knowledge Graph Creator

Complete pipeline for building a comprehensive biomedical knowledge graph with 96K+ nodes and 3.6M+ relationships.

## Prerequisites

- **Neo4j Database**: Running locally on `http://localhost:7474/browser/` or `localhost:7687` 
- **Python**: 3.10 or higher
- **Data Sources**: External repositories (see Data Sources section)

## Environment Setup

### 1. Create Conda Environment

```bash
# Create and activate environment
conda env create -f environment.yml
conda activate knowledge_graph
```

### 2. Neo4j Database Setup

Install and configure Neo4j:

```bash
# Install Neo4j (Ubuntu/Debian)
wget -O - https://debian.neo4j.com/neotechnology.gpg.key | sudo apt-key add -
echo 'deb https://debian.neo4j.com stable 4.4' | sudo tee -a /etc/apt/sources.list.d/neo4j.list
sudo apt-get update
sudo apt-get install neo4j

# Start Neo4j service
sudo systemctl enable neo4j
sudo systemctl start neo4j

# Set initial password (default: neo4j/neo4j)
cypher-shell -u neo4j -p neo4j
ALTER USER neo4j SET PASSWORD 'password';
```

**Note**: Default configuration uses `http://localhost:7474/browser/` or `bolt://localhost:7687` with username `neo4j` and password `password`. Modify `config/neo4j_config.py` if needed.

### 3. Data Sources

**Option A: Automated Download (Recommended)**

Use the provided script to automatically download required data:

```bash
# Download data automatically (detects environment)
./download_data.sh
```

This script will:
- Automatically detect if running in Docker or locally
- Create the correct data directory structure (`../data/`)
- Download only the required folders using sparse checkout:
  - `llm_evaluation_for_gene_set_interpretation/data/`
  - `talisman-paper/genesets/human/`
- Skip downloads if data already exists

**Option B: Manual Clone**

Alternatively, clone the full repositories manually:

```bash
# Clone to parent directory (../data/)
cd ..
mkdir -p data
cd data

# LLM evaluation dataset
git clone https://github.com/idekerlab/llm_evaluation_for_gene_set_interpretation.git

# Talisman paper dataset
git clone https://github.com/monarch-initiative/talisman-paper.git

cd ../kg_scripts
```

## Quick Start

**Complete setup and build:**
```bash
# 1. Create environment
conda env create -f environment.yml
conda activate knowledge_graph

# 2. Download required data
./download_data.sh

# 3. Build complete knowledge graph
python3 build_complete_biomedical_kg.py

# 4. Check metrics and validation
python3 biomedical_kg_metrics.py
```

## What This Creates

- **GO Terms**: 47,658 (biological processes, molecular functions, cellular components)
- **Genes**: 42,238 with multi-modal annotations
- **Diseases**: 163 with gene associations
- **Drugs**: 132 with perturbation data  
- **Viruses**: 34 with infection data
- **Functional Modules**: 2,339 network clusters
- **Pathways**: 131 curated pathway modules
- **Literature Genesets**: 72 curated from research papers

## Main Scripts

| Script | Purpose |
|--------|---------|
| `build_complete_biomedical_kg.py` | **Main pipeline** - builds entire knowledge graph |
| `biomedical_kg_metrics.py` | **Metrics & validation** - generates comprehensive stats |

## Individual Components

### GO Knowledge Graph
- `go_kg_builder.py` - Core GO ontology integration
- `go_interconnector.py` - Cross-namespace connections  
- `go_branch_integrator.py` - External GO data integration

### OMICS Data Integration  
- `omics_schema_setup.py` - Database schema preparation
- `omics_disease_integration.py` - Disease-gene associations
- `omics_viral_integration.py` - Virus-gene interactions
- `omics_drug_integration.py` - Drug perturbation data
- `omics_nest_integration.py` - Hierarchical network modules
- `omics_pathway_integration.py` - Curated pathway data

### Talisman Literature Integration
- `talisman_schema_setup.py` - Schema for literature genesets
- `talisman_integration_engine.py` - Curated geneset integration
- `talisman_geneset_parser.py` - Parse YAML/JSON geneset files
- `talisman_gene_validator.py` - Validate genes against knowledge graph
- `talisman_quality_control.py` - Quality control reporting

## Configuration

- `config/neo4j_config.py` - Database connection settings
- Modify credentials and batch sizes as needed

## Troubleshooting

### Common Issues

**Neo4j Connection Errors:**
```bash
# Check if Neo4j is running
sudo systemctl status neo4j

# Restart if needed
sudo systemctl restart neo4j

# Check logs
sudo journalctl -u neo4j -f
```

**Memory Issues:**
- For large datasets, increase Neo4j heap memory in `/etc/neo4j/neo4j.conf`:
```
dbms.memory.heap.initial_size=2g
dbms.memory.heap.max_size=4g
```


## Project Structure

```
kg_scripts/
├── README.md                          # This file
├── environment.yml                    # Conda environment specification
├── config/                           # Configuration files
│   └── neo4j_config.py              # Database connection settings
├── neo4j_schema_outputs/             # Schema documentation
│   ├── NEO4J_SCHEMA_COMPLETE.md     # Complete schema guide
│   ├── *.txt                        # Schema export files
│   ├── *.json                       # Metrics and statistics
│   └── schema viz.png               # Visual schema diagram
├── backups/                         # Database backup files
├── build_complete_biomedical_kg.py  # Main pipeline script
├── biomedical_kg_metrics.py         # Metrics and validation
├── download_data.sh                 # Data download utility
└── [individual integration scripts] # Component-specific builders
```

## Output

- **Neo4j knowledge graph** with all integrated data
- **biomedical_kg_metrics.json** - Complete metrics and statistics
- **neo4j_schema_outputs/** - Schema documentation and visualization
- Individual integration reports for each data type

## Schema Documentation

The `neo4j_schema_outputs/` directory contains comprehensive documentation of the knowledge graph schema:

### Generated Files

| File | Description |
|------|-------------|
| `NEO4J_SCHEMA_COMPLETE.md` | **Complete schema guide** with usage examples and maintenance commands |
| `neo4j_schema_visualization.txt` | Schema overview from `CALL db.schema.visualization()` |
| `neo4j_node_properties.txt` | All node types and properties with data types |
| `neo4j_relationship_properties.txt` | All relationship types and properties |
| `neo4j_constraints.txt` | Database constraints (uniqueness, etc.) |
| `neo4j_indexes.txt` | All database indexes for performance |
| `biomedical_kg_metrics.json` | Complete statistics and node/relationship counts |
| `schema viz.png` | Visual schema diagram |

### Schema Quick Reference

- **11 Node Types**: Gene, GOTerm, Disease, Drug, Virus, FunctionalModule, PathwayModule, CuratedGeneset, GenesetCollection, AltGOMapping, Study
- **20 Relationship Types**: Including IS_A, ANNOTATED_WITH, ASSOCIATED_WITH_DISEASE, PERTURBED_BY, etc.
- **96,416+ Nodes**: Comprehensive biomedical entities
- **3.6M+ Relationships**: Multi-modal biological connections

### Accessing Schema Information

```cypher
-- Complete schema visualization
CALL db.schema.visualization()

-- Node properties with data types
CALL db.schema.nodeTypeProperties()

-- Relationship properties
CALL db.schema.relTypeProperties()

-- View all constraints and indexes
SHOW CONSTRAINTS
SHOW INDEXES
```

For detailed schema documentation and maintenance commands, see `neo4j_schema_outputs/NEO4J_SCHEMA_COMPLETE.md`.
