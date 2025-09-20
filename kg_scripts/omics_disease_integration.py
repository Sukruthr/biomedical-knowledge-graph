"""
Complete Disease Integration Script
Integrates disease-gene associations with quantitative expression data into the knowledge graph
"""
from neo4j import GraphDatabase
import pandas as pd
import os
import argparse


def parse_disease_edges(data_dir):
    """Parse disease-gene associations"""
    file_path = os.path.join(data_dir, 'Disease__gene_attribute_edges.txt')
    df = pd.read_csv(file_path, sep='\t', skiprows=1, low_memory=False)  # Skip header row
    return df

def parse_disease_matrix(data_dir):
    """Parse disease expression matrix"""  
    file_path = os.path.join(data_dir, 'Disease_gene_attribute_matrix_standardized.txt')
    # Use line 1 as header (condition names), skip line 2 (disease names), use line 3 as index
    df = pd.read_csv(file_path, sep='\t', header=0, skiprows=[1], index_col=0, low_memory=False)
    # Drop the metadata columns (first two columns after gene symbol)
    df = df.iloc[:, 2:]  # Skip first two metadata columns (#.1 and Disease_Tissue_GEO Accession)
    # Remove the header row that got included as data (GeneSym row)
    if 'GeneSym' in df.index:
        df = df.drop('GeneSym')
    return df

def integrate_disease_data_complete(data_dir):
    """Complete disease integration with expression data"""

    print("PHASE 2: COMPLETE DISEASE INTEGRATION")
    print("=" * 60)

    # Parse omics data
    print("Loading disease data...")
    omics_data_dir = f'{data_dir}/llm_evaluation_for_gene_set_interpretation/data/Omics_data/'
    edges_df = parse_disease_edges(omics_data_dir)
    matrix_df = parse_disease_matrix(omics_data_dir)

    print(f"Disease associations: {len(edges_df):,}")
    print(f"Expression matrix: {matrix_df.shape[0]:,} genes × {matrix_df.shape[1]} conditions")

    # Connect to Neo4j
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

    with driver.session(database="neo4j") as session:

        print("\nIntegrating disease-gene associations...")

        # Process disease edges in batches
        batch_size = 1000
        processed = 0

        for i in range(0, len(edges_df), batch_size):
            batch = edges_df.iloc[i:i+batch_size]
            
            # Create nodes and relationships
            query = """
            UNWIND $edges AS edge
            MERGE (g:Gene {symbol: edge.gene_symbol})
            ON CREATE SET g.entrez_id = edge.gene_id
            MERGE (d:Disease {name: edge.disease_name})
            ON CREATE SET d.full_condition = edge.full_condition,
                          d.tissue_context = split(edge.full_condition, '_')[1],
                          d.study_id = edge.study_id
            MERGE (g)-[r:ASSOCIATED_WITH_DISEASE]->(d)
            SET r.weight = edge.weight,
                r.source = "omics",
                r.study_geo_id = edge.study_id
            """
            
            # Prepare batch data
            edges_data = []
            for _, row in batch.iterrows():
                edges_data.append({
                    'gene_symbol': row['GeneSym'],
                    'gene_id': row['GeneID'],
                    'full_condition': row['Disease_Tissue_GEO Accession'],
                    'disease_name': row['Disease'],
                    'study_id': row['GSE'],
                    'weight': row['weight']
                })
            
            session.run(query, edges=edges_data)
            processed += len(batch)
            
            if processed % 10000 == 0:
                print(f"   Processed {processed:,}/{len(edges_df):,} associations")
        
        print(f"Integrated {processed:,} disease associations")
        
        print("\nAdding quantitative expression data...")
        
        # Get relationships that need expression data
        existing_rels = list(session.run("""
        MATCH (g:Gene)-[r:ASSOCIATED_WITH_DISEASE]->(d:Disease)
        WHERE r.source = "omics" AND r.expression_zscore IS NULL
        RETURN g.symbol as gene, d.full_condition as condition
        """))
        
        print(f"   Found {len(existing_rels):,} relationships for expression data")
        
        # Add expression data in batches
        expression_updates = 0
        
        for i in range(0, len(existing_rels), batch_size):
            batch = existing_rels[i:i+batch_size]
            
            batch_updates = []
            for record in batch:
                gene = record['gene']
                condition = record['condition']
                
                if gene in matrix_df.index and condition in matrix_df.columns:
                    expression_value = matrix_df.loc[gene, condition]
                    
                    if pd.notna(expression_value) and float(expression_value) != 0:
                        regulation = "upregulated" if float(expression_value) > 0 else "downregulated"
                        batch_updates.append({
                            'gene': gene,
                            'condition': condition,
                            'zscore': float(expression_value),
                            'regulation': regulation
                        })
            
            # Apply batch updates
            if batch_updates:
                update_query = """
                UNWIND $updates AS update
                MATCH (g:Gene {symbol: update.gene})-[r:ASSOCIATED_WITH_DISEASE]->(d:Disease)
                WHERE d.full_condition = update.condition
                SET r.expression_zscore = update.zscore,
                    r.regulation = update.regulation
                """
                
                result = session.run(update_query, updates=batch_updates)
                batch_expression_updates = result.consume().counters.properties_set / 2
                expression_updates += int(batch_expression_updates)
                
                if expression_updates % 20000 == 0:
                    print(f"   Added expression data to {expression_updates:,} relationships")
        
        print(f"Added expression data to {expression_updates:,} relationships")
        
        print("\nFinal validation...")
        
        # Get final statistics
        disease_count = session.run("MATCH (d:Disease) RETURN count(d) as count").single()['count']
        
        total_rels = session.run("MATCH ()-[r:ASSOCIATED_WITH_DISEASE]->() WHERE r.source = 'omics' RETURN count(r) as count").single()['count']
        
        expr_rels = session.run("MATCH ()-[r:ASSOCIATED_WITH_DISEASE]->() WHERE r.source = 'omics' AND r.expression_zscore IS NOT NULL RETURN count(r) as count").single()['count']
        
        upregulated = session.run("MATCH ()-[r:ASSOCIATED_WITH_DISEASE]->() WHERE r.regulation = 'upregulated' RETURN count(r) as count").single()['count']
        
        downregulated = session.run("MATCH ()-[r:ASSOCIATED_WITH_DISEASE]->() WHERE r.regulation = 'downregulated' RETURN count(r) as count").single()['count']
        
        print(f"\nPHASE 2 INTEGRATION COMPLETE!")
        print(f"=" * 50)
        print(f"Disease nodes: {disease_count:,}")
        print(f"Disease associations: {total_rels:,}")
        print(f"With expression data: {expr_rels:,} ({expr_rels/total_rels*100:.1f}%)")
        print(f"Upregulated: {upregulated:,}")
        print(f"Downregulated: {downregulated:,}")
        
        # Quick validation query
        print(f"\nSample validation:")
        sample = list(session.run("""
        MATCH (g:Gene)-[r:ASSOCIATED_WITH_DISEASE]->(d:Disease)
        WHERE r.source = 'omics' AND r.expression_zscore IS NOT NULL
        RETURN g.symbol, d.name, d.tissue_context, r.expression_zscore, r.regulation
        ORDER BY r.expression_zscore DESC
        LIMIT 3
        """))
        
        for record in sample:
            print(f"   {record['g.symbol']} -> {record['d.name']} ({record['d.tissue_context']})")
            print(f"      Expression: {record['r.expression_zscore']:.3f} ({record['r.regulation']})")
    
    driver.close()
    print(f"\nPhase 2 ready for sharing - all disease data successfully integrated!")
    
    return {
        'diseases': disease_count,
        'associations': total_rels,
        'with_expression': expr_rels,
        'upregulated': upregulated,
        'downregulated': downregulated
    }

def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description='Disease Integration Script')
    parser.add_argument('--data-dir', required=True, help='Path to data directory')
    args = parser.parse_args()

    try:
        results = integrate_disease_data_complete(args.data_dir)
        print(f"\nIntegration Summary:")
        print(f"   {results['diseases']} diseases")
        print(f"   {results['associations']:,} associations")
        print(f"   {results['with_expression']:,} with expression data")
        print(f"    Disease integration completed successfully!")
        return True
    except Exception as e:
        print(f" Disease integration failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)