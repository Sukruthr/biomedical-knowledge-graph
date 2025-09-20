#!/usr/bin/env python3
"""
 Complete Viral Integration Script
Integrates viral infection data with quantitative expression profiles into Neo4j knowledge graph.

This script processes viral infection edges and expression matrix to create:
- Virus nodes with standardized names and metadata
- Gene-Virus relationships with quantitative expression weights
- Comprehensive study context and temporal information

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

class ViralIntegrationProcessor:
    def __init__(self, data_dir, neo4j_uri="bolt://localhost:7687", user="neo4j", password="password"):
        self.data_dir = data_dir
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(user, password))
        self.stats = defaultdict(int)
        
    def close(self):
        self.driver.close()
    
    def parse_viral_edges(self):
        """Parse viral infection edges file with corrected header handling"""
        logger.info("Parsing viral infection edges...")
        
        file_path = os.path.join(self.data_dir, 'Viral_Infections__gene_attribute_edges.txt')
        
        # Read with proper header handling (skip GeneSym header row)
        df = pd.read_csv(file_path, sep='\t', skiprows=1, low_memory=False)
        
        # Rename columns to match expected structure
        df.columns = ['gene_symbol', 'source_desc', 'gene_id', 'viral_condition_full', 'viral_condition', 'study_id', 'weight']
        
        logger.info(f"Parsed {len(df)} viral gene-virus edges")
        logger.info(f"Found {df['gene_symbol'].nunique()} unique genes")
        logger.info(f"Found {df['viral_condition_full'].nunique()} unique viral conditions")
        
        return df
    
    def parse_viral_matrix(self):
        """Parse viral expression matrix with corrected header handling"""
        logger.info("Parsing viral expression matrix...")
        
        file_path = os.path.join(self.data_dir, 'Viral_Infections_gene_attribute_matrix_standardized.txt')
        
        # Read with proper header handling - use line 1 as header, skip line 2, use line 3 as index
        df = pd.read_csv(file_path, sep='\t', header=0, skiprows=[1], index_col=0, low_memory=False)
        
        # Drop metadata columns (first two columns after gene symbol)
        df = df.iloc[:, 2:]
        
        # Remove the GeneSym row that got included as data
        if 'GeneSym' in df.index:
            df = df.drop('GeneSym')
        
        logger.info(f"Parsed expression matrix: {df.shape[0]} genes × {df.shape[1]} viral conditions")
        
        return df
    
    def standardize_viral_name(self, viral_condition):
        """Extract standardized virus name from condition string"""
        # Extract virus name from complex condition strings
        if 'HCMV' in viral_condition:
            return 'Human Cytomegalovirus (HCMV)'
        elif 'SARS-CoV' in viral_condition or 'icSARS' in viral_condition or 'cSARS' in viral_condition:
            if 'SARS-BatSRBD' in viral_condition:
                return 'SARS-CoV Bat SRBD'
            elif 'MA15' in viral_condition:
                return 'SARS-CoV MA15'
            elif 'NSP16' in viral_condition:
                return 'SARS-CoV NSP16'
            elif 'icSARS' in viral_condition:
                return 'icSARS-CoV'
            elif 'cSARS' in viral_condition:
                return 'cSARS Bat SRBD'
            else:
                return 'SARS-CoV'
        elif 'A-CA-04-2009' in viral_condition or 'A_CA_04_2009' in viral_condition:
            return 'Influenza A H1N1 (CA/04/2009)'
        elif 'A-Vietnam-1203' in viral_condition:
            return 'Influenza A H5N1 (Vietnam/1203/2004)'
        elif 'A-Netherlands-602' in viral_condition:
            return 'Influenza A H1N1 (Netherlands/602/2009)'
        elif 'PR8(H1N1)' in viral_condition:
            return 'Influenza A H1N1 (PR8)'
        elif 'VN(H5N1)' in viral_condition:
            return 'Influenza A H5N1 (VN)'
        elif 'X31(H3N2)' in viral_condition:
            return 'Influenza A H3N2 (X31)'
        elif 'RSV' in viral_condition:
            return 'Respiratory Syncytial Virus (RSV)'
        elif 'Rabies' in viral_condition:
            return 'Rabies Virus (CVS-11)'
        elif 'Ebolavirus' in viral_condition or 'EBOV' in viral_condition or 'ZEBOV' in viral_condition:
            return 'Ebola Virus'
        elif 'HCV' in viral_condition:
            return 'Hepatitis C Virus (HCV)'
        elif 'HCoV-EMC2012' in viral_condition:
            return 'MERS Coronavirus (HCoV-EMC)'
        elif 'HIV' in viral_condition:
            return 'Human Immunodeficiency Virus (HIV)'
        elif 'HHV' in viral_condition:
            return 'Human Herpesvirus 8 (HHV-8)'
        elif 'CVB3' in viral_condition:
            return 'Coxsackievirus B3 (CVB3)'
        elif 'Enterovirus 71' in viral_condition:
            return 'Enterovirus 71'
        elif 'Lassa' in viral_condition or 'LASV' in viral_condition:
            return 'Lassa Fever Virus'
        elif 'Dhori' in viral_condition:
            return 'Dhori Virus'
        elif 'hMPV' in viral_condition:
            return 'Human Metapneumovirus (hMPV)'
        elif 'HEV' in viral_condition:
            return 'Hepatitis E Virus (HEV)'
        elif 'Measles' in viral_condition:
            return 'Measles Virus'
        elif 'Epstein-Barr' in viral_condition:
            return 'Epstein-Barr Virus (EBV)'
        elif 'Norwalk' in viral_condition:
            return 'Norwalk Virus'
        elif 'RV16' in viral_condition:
            return 'Human Rhinovirus 16 (RV16)'
        else:
            return viral_condition.split('_')[0]  # Fallback to first part
    
    def integrate_viral_data(self, edges_df, matrix_df):
        """Integrate viral edges with quantitative expression data"""
        logger.info("Integrating viral edges with expression matrix...")
        
        # Get available matrix columns for direct matching
        available_conditions = set(matrix_df.columns)
        logger.info(f"Matrix contains {len(available_conditions)} viral conditions")
        
        integrated_data = []
        genes_with_expression = set()
        conditions_matched = set()
        
        for _, edge in edges_df.iterrows():
            gene_symbol = edge['gene_symbol']
            viral_condition_full = edge['viral_condition_full']
            
            # Get expression weight from matrix using direct column matching
            expression_weight = None
            if viral_condition_full in available_conditions:
                if gene_symbol in matrix_df.index:
                    expression_weight = matrix_df.loc[gene_symbol, viral_condition_full]
                    genes_with_expression.add(gene_symbol)
                    conditions_matched.add(viral_condition_full)
            
            integrated_data.append({
                'gene_symbol': gene_symbol,
                'gene_id': edge['gene_id'],
                'viral_condition_full': viral_condition_full,
                'viral_condition': edge['viral_condition'],
                'virus_name': self.standardize_viral_name(viral_condition_full),
                'study_id': edge['study_id'],
                'edge_weight': edge['weight'],
                'expression_weight': expression_weight
            })
        
        logger.info(f"Integrated {len(integrated_data)} gene-virus relationships")
        logger.info(f"Matched {len(conditions_matched)} conditions directly")
        logger.info(f"Expression data available for {len(genes_with_expression)} genes "
                   f"({len(genes_with_expression)/edges_df['gene_symbol'].nunique()*100:.1f}%)")
        
        return integrated_data
    
    
    def create_viral_relationships(self, integrated_data):
        """Create viral nodes and gene-virus relationships in Neo4j"""
        logger.info("Creating viral relationships in Neo4j...")
        
        # Prepare data for batch processing
        batch_data = []
        virus_metadata = {}
        
        for data in integrated_data:
            # Collect virus metadata
            virus_name = data['virus_name']
            if virus_name not in virus_metadata:
                virus_metadata[virus_name] = {
                    'conditions': set(),
                    'studies': set()
                }
            
            virus_metadata[virus_name]['conditions'].add(data['viral_condition_full'])
            virus_metadata[virus_name]['studies'].add(str(data['study_id']))
            
            # Prepare relationship data
            batch_data.append({
                'gene_symbol': data['gene_symbol'],
                'gene_id': str(data['gene_id']),
                'virus_name': virus_name,
                'viral_condition': data['viral_condition'],
                'viral_condition_full': data['viral_condition_full'],
                'study_id': str(data['study_id']),
                'edge_weight': float(data['edge_weight']),
                'expression_weight': float(data['expression_weight']) if data['expression_weight'] is not None else None,
                'has_expression': data['expression_weight'] is not None
            })
        
        # Convert sets to lists for JSON serialization
        for virus_name in virus_metadata:
            virus_metadata[virus_name]['conditions'] = list(virus_metadata[virus_name]['conditions'])
            virus_metadata[virus_name]['studies'] = list(virus_metadata[virus_name]['studies'])
            virus_metadata[virus_name]['condition_count'] = len(virus_metadata[virus_name]['conditions'])
            virus_metadata[virus_name]['study_count'] = len(virus_metadata[virus_name]['studies'])
        
        logger.info(f"Processing {len(batch_data)} relationships for {len(virus_metadata)} viruses")
        
        # Create nodes and relationships in batches
        with self.driver.session() as session:
            # Create virus nodes with metadata
            for virus_name, metadata in virus_metadata.items():
                session.run("""
                    MERGE (v:Virus {name: $virus_name})
                    SET v.condition_count = $condition_count,
                        v.study_count = $study_count,
                        v.conditions = $conditions,
                        v.studies = $studies,
                        v.source = "omics_viral"
                """, virus_name=virus_name, **metadata)
            
            # Process relationships in batches of 1000
            batch_size = 1000
            for i in range(0, len(batch_data), batch_size):
                batch = batch_data[i:i + batch_size]
                
                result = session.run("""
                    UNWIND $edges AS edge
                    MERGE (g:Gene {symbol: edge.gene_symbol})
                    ON CREATE SET g.entrez_id = edge.gene_id
                    MERGE (v:Virus {name: edge.virus_name})
                    MERGE (g)-[r:INFECTED_BY]->(v)
                    SET r.edge_weight = edge.edge_weight,
                        r.expression_weight = edge.expression_weight,
                        r.has_expression = edge.has_expression,
                        r.viral_condition = edge.viral_condition,
                        r.viral_condition_full = edge.viral_condition_full,
                        r.study_id = edge.study_id,
                        r.source = "omics"
                """, edges=batch)
                
                summary = result.consume()
                counters = summary.counters
                
                self.stats['nodes_created'] += counters.nodes_created
                self.stats['relationships_created'] += counters.relationships_created
                self.stats['properties_set'] += counters.properties_set
                
                logger.info(f"Processed batch {i//batch_size + 1}/{(len(batch_data)-1)//batch_size + 1}")
        
        logger.info(f"Created {self.stats['relationships_created']} viral relationships")
        return self.stats
    
    def validate_integration(self):
        """Validate the viral integration"""
        logger.info("Validating viral integration...")
        
        with self.driver.session() as session:
            # Count virus nodes
            result = session.run("MATCH (v:Virus) RETURN count(v) as virus_count")
            virus_count = result.single()['virus_count']
            
            # Count viral relationships
            result = session.run("MATCH ()-[r:INFECTED_BY]->() RETURN count(r) as rel_count")
            rel_count = result.single()['rel_count']
            
            # Count relationships with expression data
            result = session.run("""
                MATCH ()-[r:INFECTED_BY]->() 
                WHERE r.has_expression = true 
                RETURN count(r) as expr_count
            """)
            expr_count = result.single()['expr_count']
            
            # Get sample relationships
            result = session.run("""
                MATCH (g:Gene)-[r:INFECTED_BY]->(v:Virus)
                WHERE r.has_expression = true
                RETURN g.symbol as gene, v.name as virus, 
                       r.expression_weight as expr_weight,
                       r.viral_condition as condition
                LIMIT 5
            """)
            samples = list(result)
            
            validation_results = {
                'virus_nodes': virus_count,
                'viral_relationships': rel_count,
                'relationships_with_expression': expr_count,
                'expression_coverage_percent': round(expr_count/rel_count*100, 1) if rel_count > 0 else 0,
                'sample_relationships': samples
            }
            
            logger.info(f"Validation complete: {virus_count} viruses, {rel_count} relationships")
            logger.info(f"Expression coverage: {validation_results['expression_coverage_percent']}%")
            
            return validation_results

def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description='Viral Integration Script')
    parser.add_argument('--data-dir', required=True, help='Path to data directory')
    args = parser.parse_args()

    logger.info("=== Starting Phase 3: Viral Integration ===")

    # Configuration
    omics_data_dir = f"{args.data_dir}/llm_evaluation_for_gene_set_interpretation/data/Omics_data"
    
    # Initialize processor
    processor = ViralIntegrationProcessor(omics_data_dir)
    
    try:
        # Step 1: Parse viral data
        logger.info("Step 1: Parsing viral data files...")
        edges_df = processor.parse_viral_edges()
        matrix_df = processor.parse_viral_matrix()
        
        # Step 2: Integrate data
        logger.info("Step 2: Integrating viral data...")
        integrated_data = processor.integrate_viral_data(edges_df, matrix_df)
        
        # Step 3: Create Neo4j relationships
        logger.info("Step 3: Creating Neo4j relationships...")
        creation_stats = processor.create_viral_relationships(integrated_data)
        
        # Step 4: Validate integration
        logger.info("Step 4: Validating integration...")
        validation_results = processor.validate_integration()
        
        # Step 5: Generate report
        logger.info("Step 5: Generating integration report...")
        
        # Save results
        results = {
            'timestamp': datetime.now().isoformat(),
            'phase': 'Phase 3 - Viral Integration',
            'input_files': {
                'edges_file': 'Viral_Infections__gene_attribute_edges.txt',
                'matrix_file': 'Viral_Infections_gene_attribute_matrix_standardized.txt'
            },
            'parsing_stats': {
                'total_edges': len(edges_df),
                'unique_genes': edges_df['gene_symbol'].nunique(),
                'unique_viral_conditions': edges_df['viral_condition_full'].nunique(),
                'matrix_genes': matrix_df.shape[0],
                'matrix_conditions': matrix_df.shape[1]
            },
            'integration_stats': {
                'integrated_relationships': len(integrated_data),
                'genes_with_expression': len([d for d in integrated_data if d['expression_weight'] is not None]),
                'unique_viruses': len(set(d['virus_name'] for d in integrated_data))
            },
            'neo4j_stats': creation_stats,
            'validation_results': validation_results
        }
        
        # Save to file
        with open('phase3_viral_integration_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        # Print summary
        print("\n" + "="*60)
        print("PHASE 3 VIRAL INTEGRATION COMPLETE")
        print("="*60)
        print(f"Virus Nodes Created: {validation_results['virus_nodes']}")
        print(f"Viral Relationships: {validation_results['viral_relationships']:,}")
        print(f"With Expression Data: {validation_results['relationships_with_expression']:,} "
              f"({validation_results['expression_coverage_percent']}%)")
        print("\nSample Relationships:")
        for sample in validation_results['sample_relationships']:
            print(f"  {sample['gene']} -[INFECTED_BY]-> {sample['virus']}")
            print(f"    Expression: {sample['expr_weight']:.3f}, Condition: {sample['condition']}")
        print("="*60)
        
        logger.info("Phase 3 viral integration completed successfully!")
        print(" Viral integration completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Error during viral integration: {str(e)}")
        print(f" Viral integration failed: {e}")
        return False

    finally:
        processor.close()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)