#!/usr/bin/env python3
"""
Complete NeST Hierarchical Network Integration Script
Integrates hierarchical biological network structure into Neo4j knowledge graph.

This script processes NeST network data to create:
- FunctionalModule nodes representing biological clusters
- Hierarchical relationships between modules (CONTAINS, PART_OF)
- Gene-Module membership relationships (BELONGS_TO_MODULE)
- Multi-resolution network analysis capabilities

"""

import pandas as pd
import numpy as np
from neo4j import GraphDatabase
import os
import json
from datetime import datetime
import logging
from collections import defaultdict
import re

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NestNetworkProcessor:
    def __init__(self, data_dir, neo4j_uri="bolt://localhost:7687", user="neo4j", password="password"):
        self.data_dir = data_dir
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(user, password))
        self.stats = defaultdict(int)
        
    def close(self):
        self.driver.close()
    
    def parse_nest_network(self):
        """Parse NeST hierarchical network file"""
        logger.info("Parsing NeST hierarchical network...")
        
        file_path = os.path.join(self.data_dir, 'NeST__IAS_clixo_hidef_Nov17.edges')
        
        # Read the file
        df = pd.read_csv(file_path, sep='\t', header=None, names=['source', 'target', 'edge_type'])
        
        logger.info(f"Parsed {len(df)} total edges")
        logger.info(f"Edge types: {df['edge_type'].value_counts().to_dict()}")
        
        # Split into two types
        gene_edges = df[df['edge_type'] == 'gene'].copy()
        cluster_edges = df[df['edge_type'] == 'default'].copy()
        
        logger.info(f"Found {len(gene_edges)} gene-cluster assignments")
        logger.info(f"Found {len(cluster_edges)} cluster-cluster hierarchical relationships")
        logger.info(f"Found {gene_edges['target'].nunique()} unique genes")
        logger.info(f"Found {len(set(gene_edges['source'].unique()) | set(cluster_edges['source'].unique()) | set(cluster_edges['target'].unique()))} unique clusters")
        
        return gene_edges, cluster_edges
    
    def extract_cluster_metadata(self, gene_edges, cluster_edges):
        """Extract metadata about clusters and hierarchy"""
        logger.info("Extracting cluster metadata...")
        
        cluster_metadata = {}
        
        # Get all unique clusters
        all_clusters = set(gene_edges['source'].unique()) | set(cluster_edges['source'].unique()) | set(cluster_edges['target'].unique())
        
        for cluster in all_clusters:
            # Parse cluster level and ID
            match = re.match(r'Cluster(\d+)-(\d+)', cluster)
            if match:
                level = int(match.group(1))
                cluster_id = int(match.group(2))
            else:
                level = -1
                cluster_id = -1
                logger.warning(f"Could not parse cluster: {cluster}")
            
            # Count genes in this cluster
            gene_count = len(gene_edges[gene_edges['source'] == cluster])
            
            # Count child clusters
            child_count = len(cluster_edges[cluster_edges['source'] == cluster])
            
            # Count parent clusters  
            parent_count = len(cluster_edges[cluster_edges['target'] == cluster])
            
            cluster_metadata[cluster] = {
                'cluster_name': cluster,
                'hierarchy_level': level,
                'cluster_id': cluster_id,
                'gene_count': gene_count,
                'child_cluster_count': child_count,
                'parent_cluster_count': parent_count,
                'is_leaf': child_count == 0,
                'is_root': parent_count == 0
            }
        
        logger.info(f"Processed metadata for {len(cluster_metadata)} clusters")
        
        # Find hierarchy statistics
        levels = [meta['hierarchy_level'] for meta in cluster_metadata.values() if meta['hierarchy_level'] >= 0]
        if levels:
            logger.info(f"Hierarchy depth: {min(levels)} to {max(levels)}")
            logger.info(f"Level distribution: {pd.Series(levels).value_counts().sort_index().to_dict()}")
        
        return cluster_metadata
    
    def create_functional_modules(self, cluster_metadata):
        """Create FunctionalModule nodes in Neo4j"""
        logger.info("Creating functional module nodes...")
        
        with self.driver.session() as session:
            # Create constraint for functional modules
            session.run("""
                CREATE CONSTRAINT functional_module_name_unique IF NOT EXISTS 
                FOR (fm:FunctionalModule) REQUIRE fm.name IS UNIQUE
            """)
            
            # Prepare module data for batch creation
            module_data = []
            for cluster_name, metadata in cluster_metadata.items():
                module_data.append({
                    'name': cluster_name,
                    'hierarchy_level': metadata['hierarchy_level'],
                    'gene_count': metadata['gene_count'],
                    'child_cluster_count': metadata['child_cluster_count'],
                    'parent_cluster_count': metadata['parent_cluster_count'],
                    'is_leaf': metadata['is_leaf'],
                    'is_root': metadata['is_root'],
                    'source': 'nest_network'
                })
            
            # Create modules in batches
            batch_size = 1000
            for i in range(0, len(module_data), batch_size):
                batch = module_data[i:i + batch_size]
                
                result = session.run("""
                    UNWIND $modules AS module
                    MERGE (fm:FunctionalModule {name: module.name})
                    SET fm.hierarchy_level = module.hierarchy_level,
                        fm.gene_count = module.gene_count,
                        fm.child_cluster_count = module.child_cluster_count,
                        fm.parent_cluster_count = module.parent_cluster_count,
                        fm.is_leaf = module.is_leaf,
                        fm.is_root = module.is_root,
                        fm.source = module.source
                """, modules=batch)
                
                summary = result.consume()
                self.stats['modules_created'] += summary.counters.nodes_created
                self.stats['module_properties_set'] += summary.counters.properties_set
        
        logger.info(f"Created {len(cluster_metadata)} functional modules")
        return self.stats
    
    def create_gene_module_relationships(self, gene_edges):
        """Create Gene-Module relationships"""
        logger.info("Creating gene-module relationships...")
        
        # Prepare gene-module data
        gene_module_data = []
        for _, edge in gene_edges.iterrows():
            gene_module_data.append({
                'gene_symbol': edge['target'],
                'module_name': edge['source']
            })
        
        with self.driver.session() as session:
            # Process in batches
            batch_size = 1000
            for i in range(0, len(gene_module_data), batch_size):
                batch = gene_module_data[i:i + batch_size]
                
                result = session.run("""
                    UNWIND $edges AS edge
                    MERGE (g:Gene {symbol: edge.gene_symbol})
                    MERGE (fm:FunctionalModule {name: edge.module_name})
                    MERGE (g)-[r:BELONGS_TO_MODULE]->(fm)
                    SET r.source = "nest_network"
                """, edges=batch)
                
                summary = result.consume()
                self.stats['gene_module_relationships_created'] += summary.counters.relationships_created
                self.stats['genes_processed'] += len(batch)
                
                if (i // batch_size + 1) % 10 == 0:
                    logger.info(f"Processed batch {i//batch_size + 1}/{(len(gene_module_data)-1)//batch_size + 1}")
        
        logger.info(f"Created {self.stats['gene_module_relationships_created']} gene-module relationships")
        return self.stats
    
    def create_module_hierarchy_relationships(self, cluster_edges):
        """Create hierarchical relationships between modules"""
        logger.info("Creating module hierarchy relationships...")
        
        # Prepare hierarchy data
        hierarchy_data = []
        for _, edge in cluster_edges.iterrows():
            hierarchy_data.append({
                'parent_module': edge['source'],
                'child_module': edge['target']
            })
        
        with self.driver.session() as session:
            # Process in batches
            batch_size = 1000
            for i in range(0, len(hierarchy_data), batch_size):
                batch = hierarchy_data[i:i + batch_size]
                
                result = session.run("""
                    UNWIND $edges AS edge
                    MERGE (parent:FunctionalModule {name: edge.parent_module})
                    MERGE (child:FunctionalModule {name: edge.child_module})
                    MERGE (parent)-[r:CONTAINS]->(child)
                    MERGE (child)-[r2:PART_OF]->(parent)
                    SET r.source = "nest_network",
                        r2.source = "nest_network"
                """, edges=batch)
                
                summary = result.consume()
                self.stats['hierarchy_relationships_created'] += summary.counters.relationships_created
                
                if (i // batch_size + 1) % 10 == 0:
                    logger.info(f"Processed batch {i//batch_size + 1}/{(len(hierarchy_data)-1)//batch_size + 1}")
        
        logger.info(f"Created {self.stats['hierarchy_relationships_created']} hierarchy relationships")
        return self.stats
    
    def validate_integration(self):
        """Validate the NeST network integration"""
        logger.info("Validating NeST network integration...")
        
        with self.driver.session() as session:
            # Count functional modules
            result = session.run("MATCH (fm:FunctionalModule) RETURN count(fm) as module_count")
            module_count = result.single()['module_count']
            
            # Count gene-module relationships
            result = session.run("MATCH ()-[r:BELONGS_TO_MODULE]->() RETURN count(r) as gene_module_rels")
            gene_module_rels = result.single()['gene_module_rels']
            
            # Count hierarchy relationships
            result = session.run("MATCH ()-[r:CONTAINS]->() RETURN count(r) as hierarchy_rels")
            hierarchy_rels = result.single()['hierarchy_rels']
            
            # Get sample modules with statistics
            result = session.run("""
                MATCH (fm:FunctionalModule)
                RETURN fm.name as module, fm.hierarchy_level as level,
                       fm.gene_count as genes, fm.is_root as is_root,
                       fm.is_leaf as is_leaf
                ORDER BY fm.gene_count DESC
                LIMIT 10
            """)
            top_modules = list(result)
            
            # Check gene overlap with existing data
            result = session.run("""
                MATCH (g:Gene)-[:BELONGS_TO_MODULE]->(fm:FunctionalModule)
                WHERE exists((g)-[:PERTURBED_BY]->())
                RETURN count(DISTINCT g) as genes_with_drug_and_module
            """)
            genes_with_drug_and_module = result.single()['genes_with_drug_and_module']
            
            result = session.run("""
                MATCH (g:Gene)-[:BELONGS_TO_MODULE]->(fm:FunctionalModule)
                WHERE exists((g)-[:ASSOCIATED_WITH_DISEASE]->())
                RETURN count(DISTINCT g) as genes_with_disease_and_module
            """)
            genes_with_disease_and_module = result.single()['genes_with_disease_and_module']
            
            validation_results = {
                'functional_modules': module_count,
                'gene_module_relationships': gene_module_rels,
                'hierarchy_relationships': hierarchy_rels,
                'top_modules_by_gene_count': top_modules,
                'genes_with_drug_and_module': genes_with_drug_and_module,
                'genes_with_disease_and_module': genes_with_disease_and_module
            }
            
            logger.info(f"Validation complete: {module_count} modules, {gene_module_rels} gene-module rels, {hierarchy_rels} hierarchy rels")
            
            return validation_results

def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description='NeST Network Integration Script')
    parser.add_argument('--data-dir', required=True, help='Path to data directory')
    args = parser.parse_args()

    logger.info("=== Starting Phase 4b: NeST Network Integration ===")

    # Configuration
    omics_data_dir = f"{args.data_dir}/llm_evaluation_for_gene_set_interpretation/data/Omics_data"
    
    # Initialize processor
    processor = NestNetworkProcessor(omics_data_dir)
    
    try:
        # Step 1: Parse NeST network data
        logger.info("Step 1: Parsing NeST network data...")
        gene_edges, cluster_edges = processor.parse_nest_network()
        
        # Step 2: Extract cluster metadata
        logger.info("Step 2: Extracting cluster metadata...")
        cluster_metadata = processor.extract_cluster_metadata(gene_edges, cluster_edges)
        
        # Step 3: Create functional modules
        logger.info("Step 3: Creating functional modules...")
        module_stats = processor.create_functional_modules(cluster_metadata)
        
        # Step 4: Create gene-module relationships
        logger.info("Step 4: Creating gene-module relationships...")
        gene_module_stats = processor.create_gene_module_relationships(gene_edges)
        
        # Step 5: Create module hierarchy relationships  
        logger.info("Step 5: Creating module hierarchy relationships...")
        hierarchy_stats = processor.create_module_hierarchy_relationships(cluster_edges)
        
        # Step 6: Validate integration
        logger.info("Step 6: Validating integration...")
        validation_results = processor.validate_integration()
        
        # Step 7: Generate report
        logger.info("Step 7: Generating integration report...")
        
        # Save results
        results = {
            'timestamp': datetime.now().isoformat(),
            'phase': 'Phase 4b - NeST Network Integration',
            'input_files': {
                'nest_file': 'NeST__IAS_clixo_hidef_Nov17.edges'
            },
            'parsing_stats': {
                'total_edges': len(gene_edges) + len(cluster_edges),
                'gene_edges': len(gene_edges),
                'cluster_edges': len(cluster_edges),
                'unique_genes': gene_edges['target'].nunique(),
                'unique_clusters': len(cluster_metadata)
            },
            'integration_stats': processor.stats,
            'validation_results': validation_results
        }
        
        # Save to file
        with open('phase4b_nest_integration_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        # Print summary
        print("\n" + "="*70)
        print("PHASE 4b NeST NETWORK INTEGRATION COMPLETE")
        print("="*70)
        print(f"Functional Modules Created: {validation_results['functional_modules']}")
        print(f"Gene-Module Relationships: {validation_results['gene_module_relationships']:,}")
        print(f"Hierarchy Relationships: {validation_results['hierarchy_relationships']:,}")
        print(f"Genes with Drug & Module: {validation_results['genes_with_drug_and_module']:,}")
        print(f"Genes with Disease & Module: {validation_results['genes_with_disease_and_module']:,}")
        print("\nTop Modules by Gene Count:")
        for module in validation_results['top_modules_by_gene_count'][:5]:
            level_info = f"L{module['level']}" if module['level'] >= 0 else "L?"
            root_leaf = []
            if module['is_root']: root_leaf.append("ROOT")
            if module['is_leaf']: root_leaf.append("LEAF")
            status = f"({', '.join(root_leaf)})" if root_leaf else ""
            print(f"  {module['module']} [{level_info}]: {module['genes']} genes {status}")
        print("="*70)
        
        logger.info("Phase 4b NeST network integration completed successfully!")
        print(" NeST network integration completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Error during NeST network integration: {str(e)}")
        print(f" NeST network integration failed: {e}")
        return False

    finally:
        processor.close()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)