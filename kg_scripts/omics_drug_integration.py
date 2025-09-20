#!/usr/bin/env python3
"""
Complete Small Molecule Drug Integration Script
Integrates drug perturbation data into Neo4j knowledge graph.

"""

import pandas as pd
import numpy as np
from neo4j import GraphDatabase
import os
import json
from datetime import datetime
import logging
from collections import defaultdict

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DrugIntegrationProcessor:
    def __init__(self, data_dir, neo4j_uri="bolt://localhost:7687", user="neo4j", password="password"):
        self.data_dir = data_dir
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(user, password))
        self.stats = defaultdict(int)
        
    def close(self):
        self.driver.close()
    
    def parse_drug_edges(self):
        """Parse small molecule drug edges file"""
        logger.info("Parsing small molecule drug edges...")
        
        file_path = os.path.join(self.data_dir, 'Small_molecule __gene_attribute_edges.txt')
        
        # Read with proper header handling (skip GeneSym header row)
        df = pd.read_csv(file_path, sep='\t', skiprows=1, low_memory=False)
        
        # Rename columns to match expected structure
        df.columns = ['gene_symbol', 'source_desc', 'gene_id', 'drug_condition_full', 'drug_name', 'target_id', 'weight']
        
        logger.info(f"Parsed {len(df)} drug gene-drug edges")
        logger.info(f"Found {df['gene_symbol'].nunique()} unique genes")
        logger.info(f"Found {df['drug_name'].nunique()} unique drugs")
        logger.info(f"Found {df['drug_condition_full'].nunique()} unique drug conditions")
        
        return df
    
    def extract_experimental_context(self, drug_condition_full):
        """Extract experimental context from drug condition string"""
        # Parse condition like "fluoxetine_mus musculus_gpl1261 _gds2803"
        parts = drug_condition_full.split('_')
        
        context = {
            'organism': 'unknown',
            'platform': 'unknown', 
            'study': 'unknown'
        }
        
        if len(parts) >= 3:
            # Try to identify organism
            for part in parts:
                if 'musculus' in part.lower() or 'mus' in part.lower():
                    context['organism'] = 'Mus musculus'
                elif 'sapiens' in part.lower() or 'homo' in part.lower() or 'human' in part.lower():
                    context['organism'] = 'Homo sapiens'
                elif 'rattus' in part.lower():
                    context['organism'] = 'Rattus norvegicus'
                
                # Try to identify platform/study IDs
                if part.lower().startswith('gpl'):
                    context['platform'] = part
                elif part.lower().startswith('gds') or part.lower().startswith('gse'):
                    context['study'] = part
        
        return context
    
    def integrate_drug_data(self, edges_df):
        """Process drug edges and prepare for integration"""
        logger.info("Processing drug edges for integration...")
        
        integrated_data = []
        drug_metadata = {}
        
        for _, edge in edges_df.iterrows():
            gene_symbol = edge['gene_symbol']
            drug_condition_full = edge['drug_condition_full']
            drug_name = edge['drug_name']
            
            # Extract experimental context
            context = self.extract_experimental_context(drug_condition_full)
            
            # Collect drug metadata
            if drug_name not in drug_metadata:
                drug_metadata[drug_name] = {
                    'conditions': set(),
                    'organisms': set(),
                    'platforms': set(),
                    'studies': set()
                }
            
            drug_metadata[drug_name]['conditions'].add(drug_condition_full)
            drug_metadata[drug_name]['organisms'].add(context['organism'])
            drug_metadata[drug_name]['platforms'].add(context['platform'])
            drug_metadata[drug_name]['studies'].add(context['study'])
            
            integrated_data.append({
                'gene_symbol': gene_symbol,
                'gene_id': edge['gene_id'],
                'drug_name': drug_name,
                'drug_condition_full': drug_condition_full,
                'weight': edge['weight'],
                'organism': context['organism'],
                'platform': context['platform'],
                'study': context['study']
            })
        
        # Convert sets to lists and add counts
        for drug_name in drug_metadata:
            for key in ['conditions', 'organisms', 'platforms', 'studies']:
                drug_metadata[drug_name][key] = list(drug_metadata[drug_name][key])
            
            drug_metadata[drug_name]['condition_count'] = len(drug_metadata[drug_name]['conditions'])
            drug_metadata[drug_name]['organism_count'] = len(drug_metadata[drug_name]['organisms'])
            drug_metadata[drug_name]['platform_count'] = len(drug_metadata[drug_name]['platforms'])
            drug_metadata[drug_name]['study_count'] = len(drug_metadata[drug_name]['studies'])
        
        logger.info(f"Processed {len(integrated_data)} gene-drug relationships")
        logger.info(f"Found {len(drug_metadata)} unique drugs")
        
        return integrated_data, drug_metadata
    
    def create_drug_relationships(self, integrated_data, drug_metadata):
        """Create drug nodes and gene-drug relationships in Neo4j"""
        logger.info("Creating drug relationships in Neo4j...")
        
        # Prepare data for batch processing
        batch_data = []
        
        for data in integrated_data:
            batch_data.append({
                'gene_symbol': data['gene_symbol'],
                'gene_id': str(data['gene_id']),
                'drug_name': data['drug_name'],
                'drug_condition_full': data['drug_condition_full'],
                'weight': float(data['weight']),
                'organism': data['organism'],
                'platform': data['platform'],
                'study': data['study']
            })
        
        logger.info(f"Processing {len(batch_data)} relationships for {len(drug_metadata)} drugs")
        
        # Create nodes and relationships in batches
        with self.driver.session() as session:
            # Create drug nodes with metadata
            for drug_name, metadata in drug_metadata.items():
                session.run("""
                    MERGE (d:Drug {name: $drug_name})
                    SET d.condition_count = $condition_count,
                        d.organism_count = $organism_count,
                        d.platform_count = $platform_count,
                        d.study_count = $study_count,
                        d.conditions = $conditions,
                        d.organisms = $organisms,
                        d.platforms = $platforms,
                        d.studies = $studies,
                        d.source = "omics_drug"
                """, drug_name=drug_name, **metadata)
            
            # Process relationships in batches of 1000
            batch_size = 1000
            for i in range(0, len(batch_data), batch_size):
                batch = batch_data[i:i + batch_size]
                
                result = session.run("""
                    UNWIND $edges AS edge
                    MERGE (g:Gene {symbol: edge.gene_symbol})
                    ON CREATE SET g.entrez_id = edge.gene_id
                    ON MATCH SET g.entrez_id = COALESCE(g.entrez_id, edge.gene_id)
                    
                    MERGE (d:Drug {name: edge.drug_name})
                    MERGE (g)-[r:PERTURBED_BY]->(d)
                    SET r.weight = edge.weight,
                        r.drug_condition_full = edge.drug_condition_full,
                        r.organism = edge.organism,
                        r.platform = edge.platform,
                        r.study = edge.study,
                        r.source = "omics"
                """, edges=batch)
                
                summary = result.consume()
                counters = summary.counters
                
                self.stats['nodes_created'] += counters.nodes_created
                self.stats['relationships_created'] += counters.relationships_created
                self.stats['properties_set'] += counters.properties_set
                
                logger.info(f"Processed batch {i//batch_size + 1}/{(len(batch_data)-1)//batch_size + 1}")
        
        logger.info(f"Created {self.stats['relationships_created']} drug relationships")
        return self.stats
    
    def validate_integration(self):
        """Validate the drug integration"""
        logger.info("Validating drug integration...")
        
        with self.driver.session() as session:
            # Count drug nodes
            result = session.run("MATCH (d:Drug) RETURN count(d) as drug_count")
            drug_count = result.single()['drug_count']
            
            # Count drug relationships
            result = session.run("MATCH ()-[r:PERTURBED_BY]->() RETURN count(r) as rel_count")
            rel_count = result.single()['rel_count']
            
            # Get sample relationships
            result = session.run("""
                MATCH (g:Gene)-[r:PERTURBED_BY]->(d:Drug)
                RETURN g.symbol as gene, d.name as drug, 
                       r.weight as weight, r.organism as organism,
                       r.platform as platform
                LIMIT 5
            """)
            samples = [[r['gene'], r['drug'], r['weight'], r['organism'], r['platform']] 
                      for r in result]
            
            # Get drug statistics
            result = session.run("""
                MATCH (d:Drug)
                RETURN d.name as drug, d.condition_count as conditions,
                       d.organism_count as organisms, d.study_count as studies
                ORDER BY d.condition_count DESC
                LIMIT 10
            """)
            top_drugs = list(result)
            
            validation_results = {
                'drug_nodes': drug_count,
                'drug_relationships': rel_count,
                'sample_relationships': samples,
                'top_drugs_by_conditions': top_drugs
            }
            
            logger.info(f"Validation complete: {drug_count} drugs, {rel_count} relationships")
            
            return validation_results

def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description='Drug Integration Script')
    parser.add_argument('--data-dir', required=True, help='Path to data directory')
    args = parser.parse_args()

    logger.info("=== Starting Phase 4a: Drug Integration ===")

    # Configuration
    omics_data_dir = f"{args.data_dir}/llm_evaluation_for_gene_set_interpretation/data/Omics_data"
    
    # Initialize processor
    processor = DrugIntegrationProcessor(omics_data_dir)
    
    try:
        # Step 1: Parse drug data
        logger.info("Step 1: Parsing drug data files...")
        edges_df = processor.parse_drug_edges()
        
        # Step 2: Integrate data
        logger.info("Step 2: Processing drug data...")
        integrated_data, drug_metadata = processor.integrate_drug_data(edges_df)
        
        # Step 3: Create Neo4j relationships
        logger.info("Step 3: Creating Neo4j relationships...")
        creation_stats = processor.create_drug_relationships(integrated_data, drug_metadata)
        
        # Step 4: Validate integration
        logger.info("Step 4: Validating integration...")
        validation_results = processor.validate_integration()
        
        # Step 5: Generate report
        logger.info("Step 5: Generating integration report...")
        
        # Save results
        results = {
            'timestamp': datetime.now().isoformat(),
            'phase': 'Phase 4a - Drug Integration',
            'input_files': {
                'edges_file': 'Small_molecule __gene_attribute_edges.txt'
            },
            'parsing_stats': {
                'total_edges': len(edges_df),
                'unique_genes': edges_df['gene_symbol'].nunique(),
                'unique_drugs': edges_df['drug_name'].nunique(),
                'unique_conditions': edges_df['drug_condition_full'].nunique()
            },
            'integration_stats': {
                'integrated_relationships': len(integrated_data),
                'unique_drugs': len(drug_metadata)
            },
            'neo4j_stats': creation_stats,
            'validation_results': validation_results
        }
        
        # Save to file
        with open('phase4a_drug_integration_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        # Print summary
        print("\n" + "="*60)
        print("PHASE 4a DRUG INTEGRATION COMPLETE")
        print("="*60)
        print(f"Drug Nodes Created: {validation_results['drug_nodes']}")
        print(f"Drug Relationships: {validation_results['drug_relationships']:,}")
        print("\nSample Relationships:")
        for sample in validation_results['sample_relationships']:
            print(f"  {sample[0]} -[PERTURBED_BY]-> {sample[1]}")
            print(f"    Weight: {sample[2]}, Organism: {sample[3]}, Platform: {sample[4]}")
        print("\nTop Drugs by Condition Count:")
        for drug in validation_results['top_drugs_by_conditions'][:5]:
            print(f"  {drug['drug']}: {drug['conditions']} conditions, {drug['organisms']} organisms")
        print("="*60)
        
        logger.info("Phase 4a drug integration completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during drug integration: {str(e)}")
        raise
    
    finally:
        processor.close()

if __name__ == "__main__":
    main()