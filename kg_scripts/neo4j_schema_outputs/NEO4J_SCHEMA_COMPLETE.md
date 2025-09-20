# Neo4j Biomedical Knowledge Graph - Complete Schema Documentation

**Database**: `biomedical-kg`  
**Schema Extraction Method**: Native Neo4j procedures

## Quick Schema Commands

```cypher
-- Complete schema visualization (shows all nodes, relationships, indexes, constraints)
CALL db.schema.visualization()

-- Detailed node properties with data types
CALL db.schema.nodeTypeProperties()

-- Detailed relationship properties with data types  
CALL db.schema.relTypeProperties()

-- All constraints
SHOW CONSTRAINTS

-- All indexes
SHOW INDEXES
```

## Schema Files Generated

1. **`neo4j_schema_visualization.txt`** - Complete schema overview from `CALL db.schema.visualization()`
2. **`neo4j_node_properties.txt`** - All node properties and types from `CALL db.schema.nodeTypeProperties()`
3. **`neo4j_relationship_properties.txt`** - All relationship properties from `CALL db.schema.relTypeProperties()`
4. **`neo4j_constraints.txt`** - All uniqueness constraints from `SHOW CONSTRAINTS`
5. **`neo4j_indexes.txt`** - All database indexes from `SHOW INDEXES`

## Schema Overview

### Node Types (11 total)
From `CALL db.schema.visualization()`:

1. **Gene** - Core genes with multiple identifiers
   - Indexes: `["uniprot_id", "entrez_id", "symbol"]`
   - Constraints: None

2. **GOTerm** - Gene Ontology terms  
   - Indexes: `["go_id", "name", "namespace"]`
   - Constraints: None

3. **Disease** - Disease entities
   - Indexes: None
   - Constraints: `disease_name_unique (name)`

4. **Drug** - Drug compounds
   - Indexes: None  
   - Constraints: `drug_name_unique (name)`

5. **Virus** - Viral entities
   - Indexes: `["strain"]`
   - Constraints: `virus_name_unique (name)`

6. **FunctionalModule** - Network modules
   - Indexes: `["level"]`
   - Constraints: `module_id_unique (cluster_id)`, `functional_module_name_unique (name)`

7. **PathwayModule** - Pathway entities
   - Indexes: None
   - Constraints: `pathway_module_nest_id_unique (nest_id)`

8. **CuratedGeneset** - Literature genesets
   - Indexes: `["source_collection,name", "gene_count", "source_collection", "name"]`
   - Constraints: `curated_geneset_id_unique (geneset_id)`

9. **GenesetCollection** - Geneset collections
   - Indexes: `["collection_name"]`
   - Constraints: `geneset_collection_id_unique (collection_id)`

10. **AltGOMapping** - GO ID mappings
    - Indexes: `["obsolete_id"]`
    - Constraints: None

11. **Study** - Study metadata (currently empty)
    - Indexes: None
    - Constraints: `study_geo_unique (geo_id)`

### Relationship Types (20 total)

**GO Ontology Relationships:**
- `IS_A` - Hierarchical parent-child relationships
- `PART_OF` - Component relationships
- `REGULATES` - General regulation 
- `POSITIVELY_REGULATES` - Positive regulation
- `NEGATIVELY_REGULATES` - Negative regulation
- `OCCURS_IN` - Localization relationships
- `ENABLED_BY` - Molecular function enabling
- `HOSTS_FUNCTION` - Cellular component hosting
- `COLLAPSED_HIERARCHY` - Simplified hierarchies

**Gene Annotation Relationships:**
- `ANNOTATED_WITH` - Gene-GO term annotations (2.7M relationships)
- `ASSOCIATED_WITH_DISEASE` - Gene-disease associations  
- `INFECTED_BY` - Gene-virus infection data
- `PERTURBED_BY` - Gene-drug perturbation data

**Module & Network Relationships:**
- `BELONGS_TO_MODULE` - Gene-module membership
- `MEMBER_OF_PATHWAY` - Gene-pathway membership  
- `CONTAINS` - Module hierarchy containment

**Literature Integration Relationships:**
- `CURATED_MEMBER_OF` - Gene-geneset membership
- `PART_OF_COLLECTION` - Geneset-collection membership
- `ENRICHES_MODULE` - Geneset-pathway enrichment

**Utility Relationships:**
- `MAPS_TO` - Alternative GO ID mappings

## Data Types Summary

### Common Node Property Types:
- **String** - Names, IDs, descriptions
- **Long** - Counts, numeric IDs
- **Boolean** - Validation flags, status indicators
- **Double** - Scores, weights, sensitivity values
- **StringArray** - Lists of synonyms, references, conditions
- **DateTime** - Timestamps

### Common Relationship Property Types:
- **String** - IDs, sources, conditions, evidence codes
- **Double** - Weights, scores, z-scores, sensitivities  
- **Long** - Counts, study IDs
- **Boolean** - Validation flags, expression indicators
- **DateTime** - Creation dates, integration timestamps



## Usage Examples

```cypher
-- Get all Gene properties and types
CALL db.schema.nodeTypeProperties() 
WHERE nodeLabels = ["Gene"]

-- Get all ANNOTATED_WITH relationship properties
CALL db.schema.relTypeProperties()
WHERE relType = ":ANNOTATED_WITH"

-- Find all indexed properties
SHOW INDEXES WHERE type = "RANGE"

-- Find all uniqueness constraints
SHOW CONSTRAINTS WHERE type = "UNIQUENESS"
```

## Files Reference

All schema information is captured in these files for easy reference:
- **Schema visualization**: `neo4j_schema_visualization.txt`
- **Node properties**: `neo4j_node_properties.txt` 
- **Relationship properties**: `neo4j_relationship_properties.txt`
- **Constraints**: `neo4j_constraints.txt`
- **Indexes**: `neo4j_indexes.txt`

## Maintenance

To refresh schema documentation:
```bash
# Update all schema files
cypher-shell -u neo4j -p password -d biomedical-kg "CALL db.schema.visualization()" > neo4j_schema_visualization.txt
cypher-shell -u neo4j -p password -d biomedical-kg "CALL db.schema.nodeTypeProperties()" > neo4j_node_properties.txt
cypher-shell -u neo4j -p password -d biomedical-kg "CALL db.schema.relTypeProperties()" > neo4j_relationship_properties.txt
cypher-shell -u neo4j -p password -d biomedical-kg "SHOW CONSTRAINTS" > neo4j_constraints.txt
cypher-shell -u neo4j -p password -d biomedical-kg "SHOW INDEXES" > neo4j_indexes.txt
```

---

**Total Schema**: 96,416 nodes, 3,635,019 relationships across 11 node types and 20 relationship types 