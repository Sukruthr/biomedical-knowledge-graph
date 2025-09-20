#!/usr/bin/env python3
"""
Talisman Integration Engine

Core integration engine that creates CuratedGeneset nodes, GenesetCollection nodes,
and all relationships in the Neo4j knowledge graph. Handles batch processing,
error recovery, and integration validation.

"""

import logging
import sys
import os
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from collections import defaultdict
from datetime import datetime
from neo4j import GraphDatabase

from config.neo4j_config import NEO4J_CONFIG, BATCH_CONFIG

from talisman_geneset_parser import ParsedGeneset
from talisman_gene_validator import GeneSymbolValidator, GeneValidationResult

logger = logging.getLogger(__name__)


@dataclass
class IntegrationResults:
    """Results from talisman integration process."""
    
    # Collection statistics
    collections_created: int = 0
    
    # Geneset statistics  
    genesets_created: int = 0
    genesets_updated: int = 0
    
    # Relationship statistics
    gene_relationships_created: int = 0
    collection_relationships_created: int = 0
    enrichment_relationships_created: int = 0
    
    # Error tracking
    failed_integrations: List[Dict] = None
    
    # Processing statistics
    processing_time_seconds: float = 0.0
    batches_processed: int = 0
    
    def __post_init__(self):
        if self.failed_integrations is None:
            self.failed_integrations = []


class TalismanIntegrationEngine:
    """Core integration engine for talisman geneset data."""
    
    def __init__(self):
        """
        Initialize integration engine with config-based Neo4j connection.
        """
        self.driver = GraphDatabase.driver(
            NEO4J_CONFIG['uri'], 
            auth=(NEO4J_CONFIG['username'], NEO4J_CONFIG['password'])
        )
        self.gene_validator = GeneSymbolValidator(self.driver)
        self.batch_size = BATCH_CONFIG['batch_size']
        
    def close(self):
        """Close Neo4j driver connection."""
        if self.driver:
            self.driver.close()
        
    def integrate_all_genesets(self, parsed_genesets: List[ParsedGeneset], 
                              dry_run: bool = False) -> IntegrationResults:
        """
        Main integration orchestrator - creates all nodes and relationships.
        
        Args:
            parsed_genesets: List of validated ParsedGeneset objects
            dry_run: If True, validate but don't create nodes/relationships
            
        Returns:
            IntegrationResults with comprehensive statistics
        """
        logger.info(f"Starting talisman integration: {len(parsed_genesets)} genesets, dry_run={dry_run}")
        
        import time
        start_time = time.time()
        
        results = IntegrationResults()
        
        try:
            # Step 1: Create schema if not exists
            if not dry_run:
                logger.info("Step 1: Creating schema extensions...")
                from talisman_schema_setup import TalismanSchemaSetup
                schema_setup = TalismanSchemaSetup(self.driver)
                schema_results = schema_setup.create_schema()
                logger.info(f"Schema setup: {schema_results}")
            
            # Step 2: Create geneset collections
            logger.info("Step 2: Creating geneset collections...")
            collection_results = self._create_collections(dry_run=dry_run)
            results.collections_created = collection_results
            
            # Step 3: Validate all genes first
            logger.info("Step 3: Validating all genes...")
            validation_results = self.gene_validator.validate_all_genesets(parsed_genesets)
            logger.info(f"Gene validation complete: {len(validation_results)} genesets validated")
            
            # Step 4: Create geneset nodes and relationships in batches
            logger.info("Step 4: Creating genesets and relationships in batches...")
            geneset_batches = self._create_geneset_batches(parsed_genesets)
            
            for batch_num, batch in enumerate(geneset_batches, 1):
                logger.info(f"Processing batch {batch_num}/{len(geneset_batches)} ({len(batch)} genesets)")
                
                batch_results = self._integrate_geneset_batch(batch, validation_results, dry_run=dry_run)
                
                results.genesets_created += batch_results['genesets_created']
                results.gene_relationships_created += batch_results['gene_relationships_created']
                results.collection_relationships_created += batch_results['collection_relationships_created']
                results.failed_integrations.extend(batch_results['failures'])
                results.batches_processed += 1
                
                if batch_num % 5 == 0:
                    logger.info(f"Progress: {batch_num}/{len(geneset_batches)} batches completed")
            
            # Step 5: Create enrichment relationships with existing modules
            logger.info("Step 5: Creating enrichment relationships with existing FunctionalModules...")
            if not dry_run:
                enrichment_count = self._create_enrichment_relationships()
                results.enrichment_relationships_created = enrichment_count
                logger.info(f"Created {enrichment_count} enrichment relationships")
            
            # Final timing
            results.processing_time_seconds = time.time() - start_time
            
            logger.info(f"Integration complete in {results.processing_time_seconds:.1f}s: "
                       f"{results.genesets_created} genesets, {results.gene_relationships_created} gene relationships")
            
            return results
            
        except Exception as e:
            logger.error(f"Integration failed: {e}")
            results.processing_time_seconds = time.time() - start_time
            raise
    
    def _create_collections(self, dry_run: bool = False) -> int:
        """
        Create GenesetCollection nodes.
        
        Args:
            dry_run: If True, don't actually create nodes
            
        Returns:
            Number of collections created
        """
        collections = [
            {
                'collection_id': 'HALLMARK',
                'collection_name': 'MSigDB Hallmark Collection', 
                'description': 'Hallmark gene sets summarize and represent specific well-defined biological states or processes',
                'source_authority': 'MSigDB',
                'total_genesets': 50  # Will be updated after integration
            },
            {
                'collection_id': 'BICLUSTER',
                'collection_name': 'RNAseqDB Bicluster Collection',
                'description': 'Co-expressed gene clusters from RNAseq database analysis',
                'source_authority': 'RNAseqDB', 
                'total_genesets': 3
            },
            {
                'collection_id': 'CUSTOM',
                'collection_name': 'Literature-Curated Research Sets',
                'description': 'Manually curated gene sets from research literature',
                'source_authority': 'Literature',
                'total_genesets': 20  # Will be updated after integration
            }
        ]
        
        if dry_run:
            logger.info(f"DRY RUN: Would create {len(collections)} collections")
            return len(collections)
        
        created_count = 0
        for collection in collections:
            query = """
            MERGE (gc:GenesetCollection {collection_id: $collection_id})
            SET gc.collection_name = $collection_name,
                gc.description = $description,
                gc.source_authority = $source_authority,
                gc.total_genesets = $total_genesets,
                gc.integration_date = datetime(),
                gc.last_updated = datetime()
            RETURN gc.collection_id as id
            """
            
            with self.driver.session() as session:
                result = session.run(query, collection)
                if result.single():
                    created_count += 1
                    logger.debug(f"Created/updated collection: {collection['collection_id']}")
        
        logger.info(f"Created/updated {created_count} geneset collections")
        return created_count
    
    def _create_geneset_batches(self, genesets: List[ParsedGeneset]) -> List[List[ParsedGeneset]]:
        """
        Create batches of genesets for efficient processing.
        
        Args:
            genesets: List of genesets to batch
            
        Returns:
            List of geneset batches
        """
        batches = []
        for i in range(0, len(genesets), self.batch_size):
            batch = genesets[i:i + self.batch_size]
            batches.append(batch)
        return batches
    
    def _integrate_geneset_batch(self, geneset_batch: List[ParsedGeneset], 
                                validation_results: Dict[str, GeneValidationResult],
                                dry_run: bool = False) -> Dict[str, Any]:
        """
        Integrate a batch of genesets into Neo4j.
        
        Args:
            geneset_batch: Batch of genesets to integrate
            validation_results: Gene validation results
            dry_run: If True, don't actually create nodes
            
        Returns:
            Batch integration results
        """
        batch_results = {
            'genesets_created': 0,
            'gene_relationships_created': 0, 
            'collection_relationships_created': 0,
            'failures': []
        }
        
        for geneset in geneset_batch:
            try:
                if dry_run:
                    logger.debug(f"DRY RUN: Would integrate {geneset.geneset_id}")
                    batch_results['genesets_created'] += 1
                    
                    # Count what relationships would be created
                    validation = validation_results.get(geneset.geneset_id)
                    if validation:
                        gene_count = len(validation.valid_genes) + len(validation.gene_ids_resolved)
                        batch_results['gene_relationships_created'] += gene_count
                    batch_results['collection_relationships_created'] += 1
                    continue
                
                # Create geneset node
                geneset_node_result = self._create_geneset_node(geneset)
                if geneset_node_result:
                    batch_results['genesets_created'] += 1
                
                # Create collection relationship
                collection_rel_result = self._create_collection_relationship(geneset)
                if collection_rel_result:
                    batch_results['collection_relationships_created'] += 1
                
                # Create gene relationships
                validation = validation_results.get(geneset.geneset_id)
                if validation:
                    gene_rel_count = self._create_gene_relationships(geneset, validation)
                    batch_results['gene_relationships_created'] += gene_rel_count
                
            except Exception as e:
                error_info = {
                    'geneset_id': geneset.geneset_id,
                    'source_file': geneset.source_file,
                    'error': str(e)
                }
                batch_results['failures'].append(error_info)
                logger.error(f"Failed to integrate {geneset.geneset_id}: {e}")
        
        return batch_results
    
    def _create_geneset_node(self, geneset: ParsedGeneset) -> bool:
        """
        Create a single CuratedGeneset node.
        
        Args:
            geneset: ParsedGeneset to create
            
        Returns:
            True if created successfully
        """
        # Prepare geneset properties
        properties = {
            'geneset_id': geneset.geneset_id,
            'name': geneset.name,
            'source_collection': geneset.source_collection,
            'source_file': geneset.source_file,
            'taxon': geneset.taxon,
            'gene_count': len(geneset.gene_symbols) + len(geneset.gene_ids),
            'integration_date': datetime.now().isoformat(),
            'validation_status': 'VALIDATED'
        }
        
        # Add optional fields
        if geneset.description:
            properties['description'] = geneset.description
        
        # Add metadata for MSigDB entries
        if 'pmid' in geneset.metadata and geneset.metadata['pmid']:
            properties['pmid'] = geneset.metadata['pmid']
        
        if 'systematic_name' in geneset.metadata and geneset.metadata['systematic_name']:
            properties['systematic_name'] = geneset.metadata['systematic_name']
        
        if 'msigdb_url' in geneset.metadata and geneset.metadata['msigdb_url']:
            properties['msigdb_url'] = geneset.metadata['msigdb_url']
        
        # Create the node
        query = """
        MERGE (cg:CuratedGeneset {geneset_id: $geneset_id})
        SET cg += $properties
        RETURN cg.geneset_id as id
        """
        
        with self.driver.session() as session:
            result = session.run(query, {
                'geneset_id': geneset.geneset_id,
                'properties': properties
            })
            return len(list(result)) > 0
    
    def _create_collection_relationship(self, geneset: ParsedGeneset) -> bool:
        """
        Create PART_OF_COLLECTION relationship.
        
        Args:
            geneset: ParsedGeneset to link to collection
            
        Returns:
            True if relationship created
        """
        query = """
        MATCH (cg:CuratedGeneset {geneset_id: $geneset_id})
        MATCH (gc:GenesetCollection {collection_id: $collection_id})
        MERGE (cg)-[r:PART_OF_COLLECTION]->(gc)
        SET r.integration_date = datetime()
        RETURN count(r) as count
        """
        
        with self.driver.session() as session:
            result = session.run(query, {
                'geneset_id': geneset.geneset_id,
                'collection_id': geneset.source_collection
            })
            record = result.single()
            return record and record['count'] > 0
    
    def _create_gene_relationships(self, geneset: ParsedGeneset, 
                                 validation: GeneValidationResult) -> int:
        """
        Create CURATED_MEMBER_OF relationships between genes and geneset.
        
        Args:
            geneset: ParsedGeneset to link genes to
            validation: Gene validation results
            
        Returns:
            Number of relationships created
        """
        # Collect all valid genes
        valid_genes = validation.valid_genes + validation.gene_ids_resolved
        
        if not valid_genes:
            logger.warning(f"No valid genes found for geneset {geneset.geneset_id}")
            return 0
        
        # Create relationships in batches for performance
        gene_batches = [valid_genes[i:i+500] for i in range(0, len(valid_genes), 500)]
        total_created = 0
        
        for gene_batch in gene_batches:
            query = """
            MATCH (cg:CuratedGeneset {geneset_id: $geneset_id})
            UNWIND $gene_symbols as gene_symbol
            MATCH (g:Gene {symbol: gene_symbol})
            MERGE (g)-[r:CURATED_MEMBER_OF]->(cg)
            SET r.integration_date = datetime(),
                r.validation_status = 'VALIDATED',
                r.source_file = $source_file
            RETURN count(r) as relationships_created
            """
            
            with self.driver.session() as session:
                result = session.run(query, {
                    'geneset_id': geneset.geneset_id,
                    'gene_symbols': gene_batch,
                    'source_file': geneset.source_file
                })
            
                record = result.single()
                if record:
                    batch_count = record['relationships_created']
                    total_created += batch_count
                    logger.debug(f"Created {batch_count} gene relationships for {geneset.geneset_id}")
        
        return total_created
    
    def _create_enrichment_relationships(self, overlap_threshold: float = 0.3) -> int:
        """
        Create ENRICHES_MODULE relationships for significant overlaps with FunctionalModules.
        
        Args:
            overlap_threshold: Minimum overlap ratio to create relationship
            
        Returns:
            Number of enrichment relationships created
        """
        logger.info(f"Analyzing overlaps with existing FunctionalModules (threshold: {overlap_threshold})")
        
        query = """
        MATCH (cg:CuratedGeneset)<-[:CURATED_MEMBER_OF]-(g:Gene)-[:BELONGS_TO_MODULE]->(fm:FunctionalModule)
        WITH cg, fm, count(g) as overlap_count, 
             size([(cg)<-[:CURATED_MEMBER_OF]-(all_curated:Gene) | all_curated]) as curated_size,
             size([(fm)<-[:BELONGS_TO_MODULE]-(all_module:Gene) | all_module]) as module_size
        WITH cg, fm, overlap_count, curated_size, module_size,
             (overlap_count * 1.0 / curated_size) as curated_coverage,
             (overlap_count * 1.0 / module_size) as module_coverage
        WHERE curated_coverage >= $threshold OR module_coverage >= $threshold
        MERGE (cg)-[r:ENRICHES_MODULE]->(fm)
        SET r.overlap_count = overlap_count,
            r.curated_coverage = curated_coverage,
            r.module_coverage = module_coverage,
            r.enrichment_score = (curated_coverage + module_coverage) / 2,
            r.analysis_date = datetime()
        RETURN count(r) as relationships_created
        """
        
        with self.driver.session() as session:
            result = session.run(query, {'threshold': overlap_threshold})
            record = result.single()
            if record:
                count = record['relationships_created']
                logger.info(f"Created {count} enrichment relationships with existing modules")
                return count
        
        return 0
    
    def validate_integration(self) -> Dict[str, Any]:
        """
        Validate the completed integration.
        
        Returns:
            Dictionary with validation results
        """
        logger.info("Validating talisman integration...")
        
        validation_queries = {
            'curated_genesets': "MATCH (cg:CuratedGeneset) RETURN count(cg) as count",
            'geneset_collections': "MATCH (gc:GenesetCollection) RETURN count(gc) as count",
            'curated_memberships': "MATCH ()-[r:CURATED_MEMBER_OF]->() RETURN count(r) as count",
            'collection_relationships': "MATCH ()-[r:PART_OF_COLLECTION]->() RETURN count(r) as count",
            'enrichment_relationships': "MATCH ()-[r:ENRICHES_MODULE]->() RETURN count(r) as count",
        }
        
        validation_results = {}
        with self.driver.session() as session:
            for metric_name, query in validation_queries.items():
                result = session.run(query)
                record = result.single()
                validation_results[metric_name] = record['count'] if record else 0
            
            # Get sample genesets with statistics
            sample_query = """
            MATCH (cg:CuratedGeneset)
            OPTIONAL MATCH (cg)<-[:CURATED_MEMBER_OF]-(g:Gene)
            RETURN cg.geneset_id as geneset_id, 
                   cg.name as name,
                   cg.source_collection as collection,
                   count(g) as gene_count
            ORDER BY gene_count DESC
            LIMIT 10
            """
            
            sample_result = session.run(sample_query)
            validation_results['top_genesets'] = [dict(record) for record in sample_result]
            
            # Check for any orphaned nodes
            orphan_query = """
            MATCH (cg:CuratedGeneset)
            WHERE NOT (cg)-[:PART_OF_COLLECTION]->()
            RETURN count(cg) as orphaned_genesets
            """
            
            orphan_result = session.run(orphan_query)
            orphan_record = orphan_result.single()
            validation_results['orphaned_genesets'] = orphan_record['orphaned_genesets'] if orphan_record else 0
        
        logger.info(f"Integration validation complete: {validation_results}")
        return validation_results


def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description='Talisman Integration Engine')
    parser.add_argument('--data-dir', required=True, help='Path to data directory')
    args = parser.parse_args()

    # Setup
    import logging
    import sys
    from pathlib import Path

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("=== Starting Talisman Geneset Integration ===")

    try:
        sys.path.append(str(Path(__file__).parent.parent))

        from talisman_geneset_parser import TalismanGenesetParser

        # Initialize with data directory path
        geneset_dir = f"{args.data_dir}/talisman-paper/genesets/human"
        parser = TalismanGenesetParser(geneset_dir)
        engine = TalismanIntegrationEngine()

        # Parse genesets
        logger.info("Step 1: Parsing genesets...")
        genesets = parser.parse_all_genesets()
        logger.info(f"Parsed {len(genesets)} genesets")

        # Run actual integration
        logger.info("Step 2: Running geneset integration...")
        results = engine.integrate_all_genesets(genesets, dry_run=False)
        logger.info(f"Integration completed: {results}")

        # Validate
        logger.info("Step 3: Validating integration...")
        validation = engine.validate_integration()
        logger.info(f"Validation results: {validation}")

        logger.info("Talisman geneset integration completed successfully!")
        print("Talisman geneset integration completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Error during talisman integration: {str(e)}")
        print(f"Talisman integration failed: {e}")
        return False

    finally:
        if 'engine' in locals():
            engine.close()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)