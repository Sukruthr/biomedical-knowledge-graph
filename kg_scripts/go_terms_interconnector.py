#!/usr/bin/env python3
"""
GO Namespace Interconnector

Creates essential cross-namespace connections between BP, CC, and MF knowledge graphs
based on shared gene annotations. 

Connections created:
- BP → CC: Processes occur in cellular components (OCCURS_IN)
- BP → MF: Processes are enabled by molecular functions (ENABLED_BY) 
- CC → MF: Cellular components host molecular functions (HOSTS_FUNCTION)
"""

import logging
import time
from neo4j import GraphDatabase
from pathlib import Path
import sys

# Configure logging with fallback to console-only if file logging fails

log_path = Path(__file__).parent / 'logs' / 'go_interconnector.log'
handlers = [logging.StreamHandler(sys.stdout)]

try:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handlers.append(logging.FileHandler(log_path))
except (PermissionError, OSError):
    print(f"Warning: Could not create log file at {log_path}, logging to console only")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=handlers
)
logger = logging.getLogger(__name__)


class GOInterconnector:
    """Simple GO namespace interconnector"""
    
    def __init__(self, data_dir=None, uri="bolt://localhost:7687", user="neo4j", password="password"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.min_shared_genes = 3      # Minimum for any connection
        self.high_confidence = 50      # High confidence threshold  
        self.medium_confidence = 10    # Medium confidence threshold
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            self.driver.close()
    
    def validate_prerequisites(self):
        """Validate all three namespaces exist with sufficient data"""
        logger.info("Validating prerequisites...")
        
        with self.driver.session() as session:
            # Check namespace counts
            result = session.run("""
            MATCH (go:GOTerm) 
            WHERE go.namespace IN ['biological_process', 'cellular_component', 'molecular_function']
            RETURN go.namespace as ns, count(go) as count
            ORDER BY ns
            """)
            
            namespace_counts = {}
            for record in result:
                ns = record['ns']
                count = record['count']
                namespace_counts[ns] = count
                logger.info(f"   {ns}: {count:,} terms")
            
            # Validate minimums
            required = ['biological_process', 'cellular_component', 'molecular_function']
            minimums = {'biological_process': 25000, 'cellular_component': 3000, 'molecular_function': 10000}
            
            for ns in required:
                if ns not in namespace_counts:
                    logger.error(f" Missing namespace: {ns}")
                    return False
                if namespace_counts[ns] < minimums[ns]:
                    logger.error(f" {ns}: only {namespace_counts[ns]:,} terms (need >{minimums[ns]:,})")
                    return False
            
            # Check genes with cross-namespace annotations
            multi_ns_result = session.run("""
            MATCH (g:Gene)-[:ANNOTATED_WITH]->(go:GOTerm)
            WITH g, collect(DISTINCT go.namespace) as namespaces
            WHERE size(namespaces) > 1
            RETURN count(g) as multi_namespace_genes
            """)
            
            multi_genes = multi_ns_result.single()['multi_namespace_genes']
            if multi_genes < 10000:
                logger.error(f" Only {multi_genes:,} multi-namespace genes (need >10,000)")
                return False
            
            logger.info(f" Prerequisites validated: {multi_genes:,} multi-namespace genes")
            return True
    
    def create_bp_cc_connections(self, session):
        """Create BP → CC connections (processes occur in components)"""
        logger.info(" Creating BP → CC connections...")
        
        query = """
        MATCH (bp:GOTerm {namespace: 'biological_process'})-[:ANNOTATED_WITH]-(g:Gene)
              -[:ANNOTATED_WITH]-(cc:GOTerm {namespace: 'cellular_component'})
        WHERE bp <> cc
        AND NOT EXISTS((bp)-[:OCCURS_IN]->(cc))
        
        WITH bp, cc, count(DISTINCT g) as shared_genes
        WHERE shared_genes >= $min_genes
        
        CREATE (bp)-[r:OCCURS_IN {
            shared_gene_count: shared_genes,
            confidence: CASE 
                WHEN shared_genes >= $high_conf THEN 'high'
                WHEN shared_genes >= $med_conf THEN 'medium' 
                ELSE 'low' 
            END,
            created_by: 'go_interconnector',
            created_date: datetime()
        }]->(cc)
        
        RETURN count(r) as connections
        """
        
        result = session.run(query, 
                           min_genes=self.min_shared_genes,
                           high_conf=self.high_confidence,
                           med_conf=self.medium_confidence)
        
        return result.single()['connections']
    
    def create_bp_mf_connections(self, session):
        """Create BP → MF connections (processes enabled by functions)"""
        logger.info(" Creating BP → MF connections...")
        
        query = """
        MATCH (bp:GOTerm {namespace: 'biological_process'})-[:ANNOTATED_WITH]-(g:Gene)
              -[:ANNOTATED_WITH]-(mf:GOTerm {namespace: 'molecular_function'})
        WHERE bp <> mf
        AND NOT EXISTS((bp)-[:ENABLED_BY]->(mf))
        
        WITH bp, mf, count(DISTINCT g) as shared_genes
        WHERE shared_genes >= $min_genes
        
        CREATE (bp)-[r:ENABLED_BY {
            shared_gene_count: shared_genes,
            confidence: CASE 
                WHEN shared_genes >= $high_conf THEN 'high'
                WHEN shared_genes >= $med_conf THEN 'medium' 
                ELSE 'low' 
            END,
            created_by: 'go_interconnector',
            created_date: datetime()
        }]->(mf)
        
        RETURN count(r) as connections
        """
        
        result = session.run(query,
                           min_genes=self.min_shared_genes,
                           high_conf=self.high_confidence,
                           med_conf=self.medium_confidence)
        
        return result.single()['connections']
    
    def create_cc_mf_connections(self, session):
        """Create CC → MF connections (components host functions)"""
        logger.info(" Creating CC → MF connections...")
        
        query = """
        MATCH (cc:GOTerm {namespace: 'cellular_component'})-[:ANNOTATED_WITH]-(g:Gene)
              -[:ANNOTATED_WITH]-(mf:GOTerm {namespace: 'molecular_function'})
        WHERE cc <> mf
        AND NOT EXISTS((cc)-[:HOSTS_FUNCTION]->(mf))
        
        WITH cc, mf, count(DISTINCT g) as shared_genes
        WHERE shared_genes >= $min_genes
        
        CREATE (cc)-[r:HOSTS_FUNCTION {
            shared_gene_count: shared_genes,
            confidence: CASE 
                WHEN shared_genes >= $high_conf THEN 'high'
                WHEN shared_genes >= $med_conf THEN 'medium' 
                ELSE 'low' 
            END,
            created_by: 'go_interconnector',
            created_date: datetime()
        }]->(mf)
        
        RETURN count(r) as connections
        """
        
        result = session.run(query,
                           min_genes=self.min_shared_genes,
                           high_conf=self.high_confidence,
                           med_conf=self.medium_confidence)
        
        return result.single()['connections']
    
    def create_interconnections(self):
        """Create all cross-namespace connections"""
        logger.info("=" * 60)
        logger.info(" GO NAMESPACE INTERCONNECTION")
        logger.info("=" * 60)
        
        start_time = time.time()
        
        try:
            if not self.validate_prerequisites():
                return False
            
            with self.driver.session() as session:
                # Create all connections
                bp_cc_count = self.create_bp_cc_connections(session)
                bp_mf_count = self.create_bp_mf_connections(session)
                cc_mf_count = self.create_cc_mf_connections(session)
                
                total_connections = bp_cc_count + bp_mf_count + cc_mf_count
                elapsed_time = time.time() - start_time
                
                # Summary
                logger.info("\n" + "=" * 60)
                logger.info(" INTERCONNECTION COMPLETE")
                logger.info("=" * 60)
                logger.info(f"  Time: {elapsed_time:.2f} seconds")
                logger.info(f" Total connections: {total_connections:,}")
                logger.info(f"   • BP → CC (occurs in): {bp_cc_count:,}")
                logger.info(f"   • BP → MF (enabled by): {bp_mf_count:,}")
                logger.info(f"   • CC → MF (hosts function): {cc_mf_count:,}")
                logger.info("=" * 60)
                
                return True
                
        except Exception as e:
            logger.error(f" Interconnection failed: {str(e)}")
            return False


if __name__ == "__main__":
    with GOInterconnector() as interconnector:
        success = interconnector.create_interconnections()
        exit(0 if success else 1)