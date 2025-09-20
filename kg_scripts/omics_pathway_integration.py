#!/usr/bin/env python3
"""
Complete Semantic Pathway Integration Script
Integrates curated pathway annotations from NeST_table_All.csv into Neo4j knowledge graph.

This script processes NeST pathway data to create:
- PathwayModule nodes with semantic pathway descriptions
- Gene-Pathway membership relationships (MEMBER_OF_PATHWAY)
- Separation from structural FunctionalModule hierarchy

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

class PathwayIntegrationProcessor:
    def __init__(self, data_dir, neo4j_uri="bolt://localhost:7687", user="neo4j", password="password"):
        self.data_dir = data_dir
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(user, password))
        self.stats = defaultdict(int)
        
    def close(self):
        self.driver.close()
    
    def parse_pathway_data(self):
        """Parse NeST pathway table"""
        logger.info("Parsing NeST pathway table...")
        
        file_path = os.path.join(self.data_dir, 'NeST_table_All.csv')
        
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        logger.info(f"Parsed {len(df)} pathway modules")
        logger.info(f"Columns: {df.columns.tolist()}")
        
        # Process pathway data
        pathways = []
        for idx, row in df.iterrows():
            if pd.notna(row['All_Genes']) and pd.notna(row['name']):
                # Parse gene list
                genes = [g.strip() for g in str(row['All_Genes']).split(',') if g.strip()]
                
                pathway_data = {
                    'nest_id': str(row['NEST ID']).strip(),
                    'pathway_name': str(row['name']).strip(),
                    'genes': genes,
                    'gene_count': len(genes),
                    'size_all': row['Size_All'] if pd.notna(row['Size_All']) else len(genes)
                }
                
                # Add optional fields
                if 'name_new' in df.columns and pd.notna(row['name_new']):
                    pathway_data['pathway_description'] = str(row['name_new']).strip()
                
                # Add drug sensitivity data
                drug_columns = ['Camptothecin', 'CD437', 'Cisplatin', 'Etoposide', 'Gemcitabine', 'Olaparib']
                for drug in drug_columns:
                    if drug in df.columns and pd.notna(row[drug]):
                        pathway_data[f'{drug.lower()}_sensitivity'] = float(row[drug])
                
                # Add metadata fields
                if 'selected' in df.columns and pd.notna(row['selected']):
                    pathway_data['is_selected'] = bool(row['selected'])
                if 'name_show' in df.columns and pd.notna(row['name_show']):
                    pathway_data['display_priority'] = int(row['name_show'])
                if 'sum' in df.columns and pd.notna(row['sum']):
                    pathway_data['aggregate_score'] = int(row['sum'])
                
                pathways.append(pathway_data)
        
        logger.info(f"Processed {len(pathways)} valid pathways")
        unique_genes = set()
        for pathway in pathways:
            unique_genes.update(pathway['genes'])
        logger.info(f"Found {len(unique_genes)} unique genes across all pathways")
        
        return pathways
    
    def create_pathway_modules(self, pathways):
        """Create PathwayModule nodes in Neo4j"""
        logger.info("Creating pathway module nodes...")
        
        with self.driver.session() as session:
            # Create constraint for pathway modules
            session.run("""
                CREATE CONSTRAINT pathway_module_nest_id_unique IF NOT EXISTS 
                FOR (pm:PathwayModule) REQUIRE pm.nest_id IS UNIQUE
            """)
            
            # Prepare pathway data for batch creation
            pathway_data = []
            for pathway in pathways:
                pathway_record = {
                    'nest_id': pathway['nest_id'],
                    'pathway_name': pathway['pathway_name'],
                    'gene_count': pathway['gene_count'],
                    'size_all': pathway['size_all'],
                    'pathway_description': pathway.get('pathway_description', pathway['pathway_name']),
                    'source': 'nest_table'
                }
                
                # Add drug sensitivity scores
                drug_columns = ['camptothecin_sensitivity', 'cd437_sensitivity', 'cisplatin_sensitivity', 
                               'etoposide_sensitivity', 'gemcitabine_sensitivity', 'olaparib_sensitivity']
                for drug_col in drug_columns:
                    if drug_col in pathway:
                        pathway_record[drug_col] = pathway[drug_col]
                
                # Add metadata
                for meta_field in ['is_selected', 'display_priority', 'aggregate_score']:
                    if meta_field in pathway:
                        pathway_record[meta_field] = pathway[meta_field]
                
                pathway_data.append(pathway_record)
            
            # Create pathway modules in batches
            batch_size = 100
            for i in range(0, len(pathway_data), batch_size):
                batch = pathway_data[i:i + batch_size]
                
                result = session.run("""
                    UNWIND $pathways AS pathway
                    MERGE (pm:PathwayModule {nest_id: pathway.nest_id})
                    SET pm.pathway_name = pathway.pathway_name,
                        pm.pathway_description = pathway.pathway_description,
                        pm.gene_count = pathway.gene_count,
                        pm.size_all = pathway.size_all,
                        pm.source = pathway.source,
                        pm.camptothecin_sensitivity = pathway.camptothecin_sensitivity,
                        pm.cd437_sensitivity = pathway.cd437_sensitivity,
                        pm.cisplatin_sensitivity = pathway.cisplatin_sensitivity,
                        pm.etoposide_sensitivity = pathway.etoposide_sensitivity,
                        pm.gemcitabine_sensitivity = pathway.gemcitabine_sensitivity,
                        pm.olaparib_sensitivity = pathway.olaparib_sensitivity,
                        pm.is_selected = pathway.is_selected,
                        pm.display_priority = pathway.display_priority,
                        pm.aggregate_score = pathway.aggregate_score
                """, pathways=batch)
                
                summary = result.consume()
                self.stats['pathway_modules_created'] += summary.counters.nodes_created
                self.stats['pathway_properties_set'] += summary.counters.properties_set
        
        logger.info(f"Created {len(pathways)} pathway modules")
        return self.stats
    
    def create_gene_pathway_relationships(self, pathways):
        """Create Gene-Pathway relationships"""
        logger.info("Creating gene-pathway relationships...")
        
        # Prepare gene-pathway data
        gene_pathway_data = []
        for pathway in pathways:
            for gene_symbol in pathway['genes']:
                gene_pathway_data.append({
                    'gene_symbol': gene_symbol,
                    'nest_id': pathway['nest_id'],
                    'pathway_name': pathway['pathway_name']
                })
        
        logger.info(f"Preparing {len(gene_pathway_data)} gene-pathway relationships")
        
        with self.driver.session() as session:
            # Process in batches
            batch_size = 1000
            for i in range(0, len(gene_pathway_data), batch_size):
                batch = gene_pathway_data[i:i + batch_size]
                
                result = session.run("""
                    UNWIND $edges AS edge
                    MERGE (g:Gene {symbol: edge.gene_symbol})
                    MERGE (pm:PathwayModule {nest_id: edge.nest_id})
                    MERGE (g)-[r:MEMBER_OF_PATHWAY]->(pm)
                    SET r.source = "nest_table",
                        r.pathway_name = edge.pathway_name
                """, edges=batch)
                
                summary = result.consume()
                self.stats['gene_pathway_relationships_created'] += summary.counters.relationships_created
                self.stats['genes_processed'] += len(batch)
                
                if (i // batch_size + 1) % 5 == 0:
                    logger.info(f"Processed batch {i//batch_size + 1}/{(len(gene_pathway_data)-1)//batch_size + 1}")
        
        logger.info(f"Created {self.stats['gene_pathway_relationships_created']} gene-pathway relationships")
        return self.stats
    
    def validate_integration(self):
        """Validate the pathway integration"""
        logger.info("Validating pathway integration...")
        
        with self.driver.session() as session:
            # Count pathway modules
            result = session.run("MATCH (pm:PathwayModule) RETURN count(pm) as pathway_count")
            pathway_count = result.single()['pathway_count']
            
            # Count gene-pathway relationships
            result = session.run("MATCH ()-[r:MEMBER_OF_PATHWAY]->() RETURN count(r) as pathway_rel_count")
            pathway_rel_count = result.single()['pathway_rel_count']
            
            # Get sample pathways with gene counts
            result = session.run("""
                MATCH (pm:PathwayModule)<-[r:MEMBER_OF_PATHWAY]-(g:Gene)
                WITH pm, count(g) AS actual_gene_count
                RETURN pm.nest_id as nest_id,
                       pm.pathway_name as pathway_name,
                       pm.gene_count as expected_gene_count,
                       actual_gene_count,
                       (pm.gene_count = actual_gene_count) as count_matches
                ORDER BY actual_gene_count DESC
                LIMIT 10
            """)
            top_pathways = list(result)
            
            # Check gene overlap with existing data types
            result = session.run("""
                MATCH (g:Gene)-[:MEMBER_OF_PATHWAY]->(pm:PathwayModule)
                WHERE exists((g)-[:PERTURBED_BY]->())
                RETURN count(DISTINCT g) as genes_with_drug_and_pathway
            """)
            genes_with_drug_and_pathway = result.single()['genes_with_drug_and_pathway']
            
            result = session.run("""
                MATCH (g:Gene)-[:MEMBER_OF_PATHWAY]->(pm:PathwayModule)
                WHERE exists((g)-[:ASSOCIATED_WITH_DISEASE]->())
                RETURN count(DISTINCT g) as genes_with_disease_and_pathway
            """)
            genes_with_disease_and_pathway = result.single()['genes_with_disease_and_pathway']
            
            result = session.run("""
                MATCH (g:Gene)-[:MEMBER_OF_PATHWAY]->(pm:PathwayModule)
                WHERE exists((g)-[:BELONGS_TO_MODULE]->())
                RETURN count(DISTINCT g) as genes_with_module_and_pathway
            """)
            genes_with_module_and_pathway = result.single()['genes_with_module_and_pathway']
            
            # Check for genes with all data types
            result = session.run("""
                MATCH (g:Gene)-[:MEMBER_OF_PATHWAY]->(pm:PathwayModule)
                WHERE exists((g)-[:ANNOTATED_WITH]->())
                  AND exists((g)-[:ASSOCIATED_WITH_DISEASE]->())
                  AND exists((g)-[:INFECTED_BY]->())
                  AND exists((g)-[:PERTURBED_BY]->())
                  AND exists((g)-[:BELONGS_TO_MODULE]->())
                RETURN count(DISTINCT g) as genes_with_all_six_types
            """)
            genes_with_all_six_types = result.single()['genes_with_all_six_types']
            
            # Get sample pathways with drug sensitivity data
            result = session.run("""
                MATCH (pm:PathwayModule)
                WHERE pm.camptothecin_sensitivity IS NOT NULL
                RETURN pm.nest_id as nest_id,
                       pm.pathway_name as pathway_name,
                       pm.camptothecin_sensitivity as camptothecin,
                       pm.cisplatin_sensitivity as cisplatin,
                       pm.olaparib_sensitivity as olaparib,
                       pm.display_priority as priority
                ORDER BY pm.camptothecin_sensitivity DESC
                LIMIT 5
            """)
            top_drug_sensitive_pathways = list(result)
            
            validation_results = {
                'pathway_modules': pathway_count,
                'gene_pathway_relationships': pathway_rel_count,
                'top_pathways_by_gene_count': top_pathways,
                'genes_with_drug_and_pathway': genes_with_drug_and_pathway,
                'genes_with_disease_and_pathway': genes_with_disease_and_pathway,
                'genes_with_module_and_pathway': genes_with_module_and_pathway,
                'genes_with_all_six_types': genes_with_all_six_types,
                'top_drug_sensitive_pathways': top_drug_sensitive_pathways
            }
            
            logger.info(f"Validation complete: {pathway_count} pathways, {pathway_rel_count} gene-pathway relationships")
            
            return validation_results

def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description='Pathway Integration Script')
    parser.add_argument('--data-dir', required=True, help='Path to data directory')
    args = parser.parse_args()

    logger.info("=== Starting Phase 4c: Semantic Pathway Integration ===")

    # Configuration
    pathway_data_dir = f"{args.data_dir}/llm_evaluation_for_gene_set_interpretation/data"
    
    # Initialize processor
    processor = PathwayIntegrationProcessor(pathway_data_dir)
    
    try:
        # Step 1: Parse pathway data
        logger.info("Step 1: Parsing pathway data...")
        pathways = processor.parse_pathway_data()
        
        # Step 2: Create pathway modules
        logger.info("Step 2: Creating pathway modules...")
        module_stats = processor.create_pathway_modules(pathways)
        
        # Step 3: Create gene-pathway relationships
        logger.info("Step 3: Creating gene-pathway relationships...")
        relationship_stats = processor.create_gene_pathway_relationships(pathways)
        
        # Step 4: Validate integration
        logger.info("Step 4: Validating integration...")
        validation_results = processor.validate_integration()
        
        # Step 5: Generate report
        logger.info("Step 5: Generating integration report...")
        
        # Save results
        results = {
            'timestamp': datetime.now().isoformat(),
            'phase': 'Phase 4c - Semantic Pathway Integration',
            'input_files': {
                'pathway_file': 'NeST_table_All.csv'
            },
            'parsing_stats': {
                'total_pathways': len(pathways),
                'unique_genes': len(set(gene for pathway in pathways for gene in pathway['genes']))
            },
            'integration_stats': processor.stats,
            'validation_results': validation_results
        }
        
        # Save to file
        with open('phase4c_pathway_integration_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        # Print summary
        print("\n" + "="*70)
        print("PHASE 4c SEMANTIC PATHWAY INTEGRATION COMPLETE")
        print("="*70)
        print(f"Pathway Modules Created: {validation_results['pathway_modules']}")
        print(f"Gene-Pathway Relationships: {validation_results['gene_pathway_relationships']:,}")
        print(f"Genes with Drug & Pathway: {validation_results['genes_with_drug_and_pathway']:,}")
        print(f"Genes with Disease & Pathway: {validation_results['genes_with_disease_and_pathway']:,}")
        print(f"Genes with Module & Pathway: {validation_results['genes_with_module_and_pathway']:,}")
        print(f"Genes with ALL 6 Data Types: {validation_results['genes_with_all_six_types']:,}")
        print("\nTop Pathways by Gene Count:")
        for pathway in validation_results['top_pathways_by_gene_count'][:5]:
            match_status = "PASS" if pathway['count_matches'] else "FAIL"
            print(f"  {match_status} {pathway['nest_id']}: {pathway['pathway_name']}")
            print(f"    Expected: {pathway['expected_gene_count']}, Actual: {pathway['actual_gene_count']} genes")
        
        print("\nTop Drug-Sensitive Pathways:")
        for pathway in validation_results['top_drug_sensitive_pathways']:
            print(f"  {pathway['nest_id']}: {pathway['pathway_name']}")
            print(f"    Camptothecin: {pathway['camptothecin']:.3f}, Cisplatin: {pathway['cisplatin']:.3f}, Olaparib: {pathway['olaparib']:.3f}")
            print(f"    Display Priority: {pathway['priority']}")
        print("="*70)
        
        logger.info("Phase 4c semantic pathway integration completed successfully!")
        print(" Pathway integration completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Error during pathway integration: {str(e)}")
        print(f" Pathway integration failed: {e}")
        return False

    finally:
        processor.close()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)