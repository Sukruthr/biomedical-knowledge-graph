#!/usr/bin/env python3
"""
Talisman Schema Setup for CuratedGeneset Integration

This script creates the necessary constraints and indexes for integrating
talisman geneset data into the existing Neo4j knowledge graph.

"""

import logging
from typing import Dict, List, Any
from dataclasses import dataclass
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)


@dataclass
class ConstraintDefinition:
    """Definition of a Neo4j constraint."""
    name: str
    label: str
    properties: List[str]
    constraint_type: str  # 'UNIQUE', 'EXISTENCE'


@dataclass  
class IndexDefinition:
    """Definition of a Neo4j index."""
    name: str
    labels: List[str]
    properties: List[str]
    index_type: str  # 'BTREE', 'RANGE'


class TalismanSchemaSetup:
    """Setup schema extensions for talisman geneset integration."""
    
    def __init__(self, neo4j_driver):
        """Initialize schema setup with Neo4j driver."""
        self.driver = neo4j_driver
        
    def create_schema(self) -> Dict[str, Any]:
        """
        Create constraints and indexes for talisman integration.
        
        Creates:
        - CuratedGeneset unique constraint on geneset_id
        - GenesetCollection unique constraint on collection_id
        - Performance indexes for common query patterns
        
        Returns:
            Dictionary with creation results and any errors
        """
        results = {
            "constraints_created": 0,
            "indexes_created": 0,
            "errors": []
        }
        
        logger.info("Creating talisman integration schema extensions...")
        
        # Create constraints
        constraints = self._get_talisman_constraints()
        for constraint in constraints:
            try:
                self._create_constraint(constraint)
                results["constraints_created"] += 1
                logger.info(f"Created constraint: {constraint.name}")
            except Exception as e:
                error_msg = f"Failed to create constraint {constraint.name}: {e}"
                logger.error(error_msg)
                results["errors"].append(error_msg)
        
        # Create indexes
        indexes = self._get_talisman_indexes()
        for index in indexes:
            try:
                self._create_index(index)
                results["indexes_created"] += 1
                logger.info(f"Created index: {index.name}")
            except Exception as e:
                error_msg = f"Failed to create index {index.name}: {e}"
                logger.error(error_msg)
                results["errors"].append(error_msg)
        
        logger.info(f"Talisman schema creation completed: {results}")
        return results
    
    def _get_talisman_constraints(self) -> List[ConstraintDefinition]:
        """Get constraints required for talisman integration."""
        return [
            # Primary unique constraints
            ConstraintDefinition(
                "curated_geneset_id_unique", 
                "CuratedGeneset", 
                ["geneset_id"], 
                "UNIQUE"
            ),
            ConstraintDefinition(
                "geneset_collection_id_unique", 
                "GenesetCollection", 
                ["collection_id"], 
                "UNIQUE"
            )
        ]
    
    def _get_talisman_indexes(self) -> List[IndexDefinition]:
        """Get indexes required for talisman integration performance."""
        return [
            # Primary lookup indexes
            IndexDefinition(
                "curated_geneset_name_idx", 
                ["CuratedGeneset"], 
                ["name"], 
                "BTREE"
            ),
            IndexDefinition(
                "curated_geneset_source_idx", 
                ["CuratedGeneset"], 
                ["source_collection"], 
                "BTREE"
            ),
            IndexDefinition(
                "curated_geneset_gene_count_idx", 
                ["CuratedGeneset"], 
                ["gene_count"], 
                "BTREE"
            ),
            IndexDefinition(
                "geneset_collection_name_idx", 
                ["GenesetCollection"], 
                ["collection_name"], 
                "BTREE"
            ),
            
            # Composite indexes for complex queries
            IndexDefinition(
                "curated_geneset_source_name_idx", 
                ["CuratedGeneset"], 
                ["source_collection", "name"], 
                "BTREE"
            )
        ]
    
    def _create_constraint(self, constraint: ConstraintDefinition) -> None:
        """Create a single constraint."""
        if constraint.constraint_type == "UNIQUE":
            properties_str = ", ".join([f"n.{prop}" for prop in constraint.properties])
            query = f"""
            CREATE CONSTRAINT {constraint.name} IF NOT EXISTS
            FOR (n:{constraint.label})
            REQUIRE ({properties_str}) IS UNIQUE
            """
        elif constraint.constraint_type == "EXISTENCE":
            properties_str = ", ".join([f"n.{prop}" for prop in constraint.properties])
            query = f"""
            CREATE CONSTRAINT {constraint.name} IF NOT EXISTS
            FOR (n:{constraint.label})
            REQUIRE ({properties_str}) IS NOT NULL
            """
        else:
            raise ValueError(f"Unknown constraint type: {constraint.constraint_type}")
        
        with self.driver.session() as session:
            session.run(query)
    
    def _create_index(self, index: IndexDefinition) -> None:
        """Create a single index.""" 
        labels_str = ":".join(index.labels)
        properties_str = ", ".join([f"n.{prop}" for prop in index.properties])
        
        query = f"""
        CREATE INDEX {index.name} IF NOT EXISTS
        FOR (n:{labels_str})
        ON ({properties_str})
        """
        
        with self.driver.session() as session:
            session.run(query)
    
    def validate_schema(self) -> Dict[str, Any]:
        """
        Validate that the talisman schema extensions were created successfully.
        
        Returns:
            Dictionary with validation results
        """
        validation = {
            "constraints_found": [],
            "indexes_found": [],
            "missing_constraints": [],
            "missing_indexes": [],
            "overall_status": "UNKNOWN"
        }
        
        try:
            with self.driver.session() as session:
                # Check constraints
                constraints_query = "SHOW CONSTRAINTS"
                constraints_result = session.run(constraints_query)
                found_constraints = [c.get('name', '') for c in constraints_result]
                
                expected_constraints = [c.name for c in self._get_talisman_constraints()]
                for constraint_name in expected_constraints:
                    if constraint_name in found_constraints:
                        validation["constraints_found"].append(constraint_name)
                    else:
                        validation["missing_constraints"].append(constraint_name)
                
                # Check indexes
                indexes_query = "SHOW INDEXES"
                indexes_result = session.run(indexes_query)
                found_indexes = [i.get('name', '') for i in indexes_result]
                
                expected_indexes = [i.name for i in self._get_talisman_indexes()]
                for index_name in expected_indexes:
                    if index_name in found_indexes:
                        validation["indexes_found"].append(index_name)
                    else:
                        validation["missing_indexes"].append(index_name)
                
                # Overall status
                all_constraints_found = len(validation["missing_constraints"]) == 0
                all_indexes_found = len(validation["missing_indexes"]) == 0
                validation["overall_status"] = "PASS" if (all_constraints_found and all_indexes_found) else "FAIL"
            
        except Exception as e:
            validation["error"] = str(e)
            validation["overall_status"] = "ERROR"
        
        return validation


if __name__ == "__main__":
    # Example usage
    from neo4j import GraphDatabase
    from config.neo4j_config import NEO4J_CONFIG
    
    # Initialize connection
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'], 
        auth=(NEO4J_CONFIG['username'], NEO4J_CONFIG['password'])
    )
    
    try:
        # Create schema
        schema_setup = TalismanSchemaSetup(driver)
        results = schema_setup.create_schema()
        print(f"Schema creation results: {results}")
        
        # Validate schema
        validation = schema_setup.validate_schema()
        print(f"Schema validation: {validation}")
    finally:
        driver.close()