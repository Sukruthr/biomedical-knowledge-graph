#!/usr/bin/env python3
"""
GO Branch Integration Engine
Core integration logic for adding external GO branch data and enhancing gene-GO term associations.
"""

import csv
import logging
import sys
import os
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from neo4j import GraphDatabase
from datetime import datetime

# Import Neo4j configuration
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config.neo4j_config import NEO4J_CONFIG, BATCH_CONFIG


def get_data_dir():
    """Get data directory path - works both in Docker and locally."""
    if os.path.exists('/app/data'):
        return '/app/data'  # Docker environment
    else:
        # Local environment - find project root
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        return os.path.join(project_root, 'data')


# Integration configuration - inline
INTEGRATION_CONFIG = {
    # 'data_sources': {
    #     'bp_branch': f"{get_data_dir()}/llm_evaluation_for_gene_set_interpretation/data/go_terms.csv",
    #     'cc_branch': f"{get_data_dir()}/llm_evaluation_for_gene_set_interpretation/data/GO_term_analysis/CC_MF_branch/CC_go_terms.csv",
    #     'mf_branch': f"{get_data_dir()}/llm_evaluation_for_gene_set_interpretation/data/GO_term_analysis/CC_MF_branch/MF_go_terms.csv"
    # },
    'integration': {
        'batch_size': BATCH_CONFIG['batch_size'],
        'max_retries': 3,
        'timeout_seconds': 300,
        'preserve_existing': True,
        'enable_validation': True
    },
    'validation': {
        'min_gene_symbol_length': 1,
        'max_gene_symbol_length': 20,
        'go_id_pattern': "^GO:\\d{7}$",
        'required_fields': ["GO", "Genes", "Gene_Count", "Term_Description"]
    },
    'enhancement': {
        'source_type_new': "external_branch_enhanced",
        'source_type_existing': "both_direct_and_branch", 
        'source_type_original': "direct_go",
        'enhancement_method': "branch_data_integration",
        'evidence_code_external': "EXTERNAL_BRANCH_DERIVED"
    }
}

class GOBranchIntegrator:
    def __init__(self, data_dir=None):
        """Initialize integrator with configuration."""
        self.setup_logging()
        
        # Neo4j connection from config
        self.driver = GraphDatabase.driver(
            NEO4J_CONFIG['uri'],
            auth=(NEO4J_CONFIG['username'], NEO4J_CONFIG['password'])
        )
        
        # Use provided data_dir or fall back to automatic detection
        if data_dir is None:
            data_dir = get_data_dir()

        # Update data sources with actual data directory
        self.data_sources = {
            'bp_branch': f"{data_dir}/llm_evaluation_for_gene_set_interpretation/data/go_terms.csv",
            'cc_branch': f"{data_dir}/llm_evaluation_for_gene_set_interpretation/data/GO_term_analysis/CC_MF_branch/CC_go_terms.csv",
            'mf_branch': f"{data_dir}/llm_evaluation_for_gene_set_interpretation/data/GO_term_analysis/CC_MF_branch/MF_go_terms.csv"
        }

        # Integration statistics
        self.stats = {
            'bp_branch': {'processed': 0, 'new_associations': 0, 'enhanced_existing': 0, 'errors': 0},
            'cc_branch': {'processed': 0, 'new_associations': 0, 'enhanced_existing': 0, 'errors': 0},
            'mf_branch': {'processed': 0, 'new_associations': 0, 'enhanced_existing': 0, 'errors': 0}
        }
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
            
    def setup_logging(self):
        """Set up logging configuration with fallback."""
        log_path = Path(__file__).parent / 'logs' / 'go_branch_integration.log'
        handlers = [logging.StreamHandler(sys.stdout)]

        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            handlers.append(logging.FileHandler(log_path))
        except (PermissionError, OSError):
            print(f"Warning: Could not create log file at {log_path}, logging to console only")

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=handlers
        )
        self.logger = logging.getLogger(__name__)
        
    def get_existing_genes(self) -> Set[str]:
        """Get all existing gene symbols from knowledge graph."""
        with self.driver.session() as session:
            result = session.run("MATCH (g:Gene) RETURN g.symbol as symbol")
            return {record["symbol"] for record in result if record["symbol"]}
            
    def get_existing_go_terms(self) -> Set[str]:
        """Get all existing GO terms from knowledge graph."""
        with self.driver.session() as session:
            result = session.run("MATCH (go:GOTerm) RETURN go.go_id as go_id")
            return {record["go_id"] for record in result if record["go_id"]}
            
    def create_missing_genes(self, gene_symbols: Set[str]) -> int:
        """Create gene nodes for missing gene symbols."""
        existing_genes = self.get_existing_genes()
        missing_genes = gene_symbols - existing_genes
        
        if not missing_genes:
            return 0
            
        self.logger.info(f"Creating {len(missing_genes)} missing gene nodes...")
        
        created_count = 0
        batch_size = INTEGRATION_CONFIG['integration']['batch_size']
        
        gene_batches = [list(missing_genes)[i:i+batch_size] 
                       for i in range(0, len(missing_genes), batch_size)]
        
        for batch in gene_batches:
            with self.driver.session() as session:
                try:
                    query = """
                    UNWIND $gene_symbols as gene_symbol
                    MERGE (g:Gene {symbol: gene_symbol})
                    SET g.import_timestamp = datetime(),
                        g.source = 'go_branch_integration'
                    RETURN count(g) as created
                    """
                    result = session.run(query, gene_symbols=batch)
                    batch_created = result.single()["created"]
                    created_count += batch_created
                    
                except Exception as e:
                    self.logger.error(f"Error creating gene batch: {e}")
                    
        self.logger.info(f"Created {created_count} new gene nodes")
        return created_count
        
    def create_missing_go_terms(self, go_term_data: List[Dict]) -> int:
        """Create GO term nodes for missing GO terms."""
        existing_go_terms = self.get_existing_go_terms()
        missing_terms = [term for term in go_term_data 
                        if term['GO_ID'] not in existing_go_terms]
        
        if not missing_terms:
            return 0
            
        self.logger.info(f"Creating {len(missing_terms)} missing GO term nodes...")
        
        created_count = 0
        batch_size = INTEGRATION_CONFIG['integration']['batch_size']
        
        term_batches = [missing_terms[i:i+batch_size] 
                       for i in range(0, len(missing_terms), batch_size)]
        
        for batch in term_batches:
            with self.driver.session() as session:
                try:
                    query = """
                    UNWIND $go_terms as term_data
                    MERGE (go:GOTerm {go_id: term_data.GO_ID})
                    SET go.name = term_data.term_description,
                        go.import_timestamp = datetime(),
                        go.source = 'go_branch_integration'
                    RETURN count(go) as created
                    """
                    result = session.run(query, go_terms=batch)
                    batch_created = result.single()["created"]
                    created_count += batch_created
                    
                except Exception as e:
                    self.logger.error(f"Error creating GO term batch: {e}")
                    
        self.logger.info(f"Created {created_count} new GO term nodes")
        return created_count
        
    def integrate_gene_go_associations(self, go_id: str, gene_symbols: List[str], branch_name: str) -> Tuple[int, int]:
        """Integrate gene-GO term associations using MERGE strategy with idempotency."""
        new_associations = 0
        enhanced_existing = 0
        
        batch_size = INTEGRATION_CONFIG['integration']['batch_size']
        gene_batches = [gene_symbols[i:i+batch_size] 
                       for i in range(0, len(gene_symbols), batch_size)]
        
        for batch in gene_batches:
            with self.driver.session() as session:
                try:
                    # Enhanced MERGE query with idempotency check
                    query = """
                    MATCH (go:GOTerm {go_id: $go_id})
                    UNWIND $gene_symbols as gene_symbol
                    MATCH (g:Gene {symbol: gene_symbol})
                    OPTIONAL MATCH (g)-[existing:ANNOTATED_WITH]->(go)
                    
                    // Skip if already processed by GO branch integration
                    WHERE existing IS NULL OR existing.branch_confirmed IS NULL OR existing.branch_confirmed <> true
                    
                    MERGE (g)-[r:ANNOTATED_WITH]->(go)
                    SET r.branch_confirmed = true,
                        r.enhancement_method = $enhancement_method,
                        r.source_type = CASE 
                            WHEN existing IS NOT NULL THEN $source_type_existing
                            ELSE $source_type_new
                        END,
                        r.original_association = CASE WHEN existing IS NOT NULL THEN true ELSE false END,
                        r.integration_timestamp = CASE 
                            WHEN existing IS NULL THEN datetime()
                            ELSE coalesce(existing.integration_timestamp, datetime())
                        END,
                        r.evidence_code = CASE 
                            WHEN existing IS NOT NULL THEN existing.evidence_code
                            ELSE $evidence_code_external
                        END
                    WITH r, existing
                    RETURN 
                        count(CASE WHEN existing IS NULL THEN 1 END) as new_count,
                        count(CASE WHEN existing IS NOT NULL THEN 1 END) as enhanced_count
                    """
                    
                    result = session.run(query, 
                        go_id=go_id,
                        gene_symbols=batch,
                        enhancement_method=INTEGRATION_CONFIG['enhancement']['enhancement_method'],
                        source_type_existing=INTEGRATION_CONFIG['enhancement']['source_type_existing'],
                        source_type_new=INTEGRATION_CONFIG['enhancement']['source_type_new'],
                        evidence_code_external=INTEGRATION_CONFIG['enhancement']['evidence_code_external']
                    )
                    
                    record = result.single()
                    new_associations += record["new_count"] or 0
                    enhanced_existing += record["enhanced_count"] or 0
                    
                except Exception as e:
                    self.logger.error(f"Error integrating associations for {go_id}: {e}")
                    self.stats[branch_name]['errors'] += 1
                    
        return new_associations, enhanced_existing
        
    def process_branch_file(self, file_path: str, branch_name: str) -> bool:
        """Process a single branch data file."""
        self.logger.info(f"Processing {branch_name}: {file_path}")
        
        if not os.path.exists(file_path):
            self.logger.error(f"File not found: {file_path}")
            return False
            
        # Read and parse data
        go_term_data = []
        all_gene_symbols = set()
        
        try:
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    go_id = row.get('GO', '').strip()
                    genes_str = row.get('Genes', '').strip()
                    term_description = row.get('Term_Description', '').strip()
                    
                    if not go_id or not genes_str:
                        continue
                        
                    gene_symbols = [g.strip() for g in genes_str.split() if g.strip()]
                    
                    go_term_data.append({
                        'GO_ID': go_id,
                        'term_description': term_description,
                        'gene_symbols': gene_symbols
                    })
                    
                    all_gene_symbols.update(gene_symbols)
                    
        except Exception as e:
            self.logger.error(f"Error reading {file_path}: {e}")
            return False
            
        self.logger.info(f"Loaded {len(go_term_data)} GO terms with {len(all_gene_symbols)} unique genes")
        
        # Create missing nodes
        self.create_missing_genes(all_gene_symbols)
        self.create_missing_go_terms(go_term_data)
        
        # Process associations
        for term_data in go_term_data:
            try:
                new_assoc, enhanced_assoc = self.integrate_gene_go_associations(
                    term_data['GO_ID'], 
                    term_data['gene_symbols'], 
                    branch_name
                )
                
                self.stats[branch_name]['processed'] += 1
                self.stats[branch_name]['new_associations'] += new_assoc
                self.stats[branch_name]['enhanced_existing'] += enhanced_assoc
                
                # Progress logging
                if self.stats[branch_name]['processed'] % 100 == 0:
                    self.logger.info(f"{branch_name}: Processed {self.stats[branch_name]['processed']} terms")
                    
            except Exception as e:
                self.logger.error(f"Error processing term {term_data['GO_ID']}: {e}")
                self.stats[branch_name]['errors'] += 1
                
        self.logger.info(f"Completed {branch_name}: "
                        f"processed={self.stats[branch_name]['processed']}, "
                        f"new={self.stats[branch_name]['new_associations']}, "
                        f"enhanced={self.stats[branch_name]['enhanced_existing']}, "
                        f"errors={self.stats[branch_name]['errors']}")
        
        return True
        
    def run_integration(self) -> bool:
        """Run the complete integration process."""
        self.logger.info("Starting GO Branch Integration...")
        start_time = time.time()
        
        branches = [
            ('bp_branch', 'BP Branch'),
            ('cc_branch', 'CC Branch'), 
            ('mf_branch', 'MF Branch')
        ]
        
        success = True
        
        for branch_key, branch_name in branches:
            file_path = self.data_sources[branch_key]
            
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"INTEGRATING {branch_name.upper()}")
            self.logger.info(f"{'='*60}")
            
            branch_success = self.process_branch_file(file_path, branch_key)
            if not branch_success:
                self.logger.error(f"Failed to integrate {branch_name}")
                success = False
            else:
                self.logger.info(f" {branch_name} integration completed successfully")
                
        # Final summary
        total_time = time.time() - start_time
        self.logger.info(f"\n{'='*60}")
        self.logger.info("INTEGRATION SUMMARY")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Total Runtime: {total_time:.2f} seconds")
        
        total_processed = sum(stats['processed'] for stats in self.stats.values())
        total_new = sum(stats['new_associations'] for stats in self.stats.values())
        total_enhanced = sum(stats['enhanced_existing'] for stats in self.stats.values())
        total_errors = sum(stats['errors'] for stats in self.stats.values())
        
        self.logger.info(f"Total Terms Processed: {total_processed}")
        self.logger.info(f"New Associations: {total_new}")
        self.logger.info(f"Enhanced Existing: {total_enhanced}")
        self.logger.info(f"Total Errors: {total_errors}")
        
        if success and total_errors == 0:
            self.logger.info(" INTEGRATION COMPLETED SUCCESSFULLY!")
        elif success:
            self.logger.warning(f"  INTEGRATION COMPLETED WITH {total_errors} ERRORS")
        else:
            self.logger.error(" INTEGRATION FAILED")
            
        return success and total_errors == 0
        
    def generate_integration_report(self) -> Dict:
        """Generate comprehensive integration report."""
        return {
            'timestamp': datetime.now().isoformat(),
            'integration_stats': self.stats,
            'summary': {
                'total_processed': sum(stats['processed'] for stats in self.stats.values()),
                'total_new_associations': sum(stats['new_associations'] for stats in self.stats.values()),
                'total_enhanced_existing': sum(stats['enhanced_existing'] for stats in self.stats.values()),
                'total_errors': sum(stats['errors'] for stats in self.stats.values())
            }
        }
        
    def save_report(self, report_path: str):
        """Save integration report to file."""
        report = self.generate_integration_report()
        
        import json
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
            
        self.logger.info(f"Integration report saved to: {report_path}")
        
    def close(self):
        """Close database connection."""
        if self.driver:
            self.driver.close()

def main():
    """Main integration function."""
    integrator = GOBranchIntegrator()
    
    try:
        # Run integration
        success = integrator.run_integration()
        
        # Generate and save report
        report_path = Path(__file__).parent / 'reports' / 'go_branch_integration_report.json'
        integrator.save_report(str(report_path))
        
        if not success:
            sys.exit(1)
            
    finally:
        integrator.close()

if __name__ == "__main__":
    main()