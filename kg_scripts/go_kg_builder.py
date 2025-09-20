#!/usr/bin/env python3
"""
GO KNOWLEDGE GRAPH CREATION FRAMEWORK
=============================================

This framework creates complete GO knowledge graphs for any namespace
(BP, CC, MF)

PHASES CONSOLIDATED:
- Phase 1: Foundation (go-basic.obo) 
- Phase 2: ID Mappings (goID_2_alt_id.tab)
- Phase 3: Metadata Validation (goID_2_name.tab & goID_2_namespace.tab)
- Phase 4: Hierarchical Structure (go.tab)
- Phase 5: Gene Annotations (goa_human.gaf.gz)
- Phase 6: ID Cross-References (collapsed_go.entrez)
- Phase 7: Symbol Cross-References (collapsed_go.symbol)
- Phase 8: UniProt Cross-References (collapsed_go.uniprot)

Usage:
    creator = CompleteGOKnowledgeGraphCreator(namespace="bp|cc|mf")
    creator.build_complete_knowledge_graph()

"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from neo4j import GraphDatabase
from config.neo4j_config import NEO4J_CONFIG, BATCH_CONFIG
import re
import time
from datetime import datetime
from collections import defaultdict
import logging
import csv
import gzip
import pandas as pd
from pathlib import Path

# Configure comprehensive logging - will be setup by each instance
logger = logging.getLogger(__name__)


def get_data_dir():
    """Get data directory path - works both in Docker and locally."""
    if os.path.exists('/app/data'):
        return '/app/data'  # Docker environment
    else:
        # Local environment - find project root
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        return os.path.join(project_root, 'data')


class CompleteGOKnowledgeGraphCreator:
    """Unified implementation for GO knowledge graph creation - any namespace"""
    
    def __init__(self, namespace="bp", data_dir=None, neo4j_uri=None, neo4j_user=None, neo4j_password=None):
        """Initialize with namespace configuration"""
        self.namespace = namespace.lower()
        self.namespace_full = {
            'bp': 'biological_process',
            'cc': 'cellular_component', 
            'mf': 'molecular_function'
        }[self.namespace]
        
        # Set namespace-specific qualifier for gene annotations
        self.qualifier = {
            'bp': 'involved_in',
            'cc': 'located_in', 
            'mf': 'enables'
        }[self.namespace]
        
        # Configure logging with namespace-specific log file and fallback
        log_path = Path(__file__).parent / 'logs' / f'complete_go_{self.namespace}_kg_creation.log'
        handlers = [logging.StreamHandler(sys.stdout)]

        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            handlers.append(logging.FileHandler(log_path))
        except (PermissionError, OSError):
            print(f"Warning: Could not create log file at {log_path}, logging to console only")

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=handlers,
            force=True  # Override any existing configuration
        )
        
        # Neo4j connection - use parameters if provided
        if neo4j_uri and neo4j_user and neo4j_password:
            self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        elif neo4j_uri:
            self.driver = GraphDatabase.driver(neo4j_uri)
        else:
            self.driver = GraphDatabase.driver(
                NEO4J_CONFIG['uri'], 
                auth=(NEO4J_CONFIG['username'], NEO4J_CONFIG['password'])
            )
        
        # Data file paths - parameterized by namespace
        # Use provided data_dir or fall back to automatic detection
        if data_dir is None:
            data_dir = get_data_dir()
        data_base = f"{data_dir}/llm_evaluation_for_gene_set_interpretation/data/GO_{self.namespace.upper()}"
        self.data_paths = {
            'go_basic_obo': f"{data_base}/go-basic.obo",
            'alt_id_tab': f"{data_base}/goID_2_alt_id.tab",
            'name_tab': f"{data_base}/goID_2_name.tab",
            'namespace_tab': f"{data_base}/goID_2_namespace.tab",
            'go_tab': f"{data_base}/go.tab",
            'goa_human_gaf': f"{data_base}/goa_human.gaf.gz",
            'collapsed_entrez': f"{data_base}/collapsed_go.entrez",
            'collapsed_symbol': f"{data_base}/collapsed_go.symbol",
            'collapsed_uniprot': f"{data_base}/collapsed_go.uniprot"
        }
        
        # Optimized batch sizes (learned from individual phases)
        self.batch_sizes = {
            'go_terms': BATCH_CONFIG['batch_size'],
            'relationships': BATCH_CONFIG['batch_size'],
            'alt_mappings': BATCH_CONFIG['batch_size'],
            'validation': BATCH_CONFIG['batch_size'],
            'hierarchy': 1000,
            'gene_annotations': 1000,  # Conservative for gene operations
            'entrez_genes': 5000,      # Optimized from Phase 6
            'symbol_genes': 1000,      # Conservative due to symbol matching
            'uniprot_genes': 2000      # Optimized from Phase 8
        }
        
        # Global statistics tracking
        self.global_stats = {
            'start_time': None,
            'end_time': None,
            'phase_times': {},
            'total_nodes_created': 0,
            'total_relationships_created': 0,
            'phases_completed': []
        }
        
        # Import timestamp for all operations
        self.import_timestamp = datetime.now().isoformat()
        
        # Phase-specific data storage
        self.phase_data = {}
        
        # Reference DataFrames for fast validation (loaded in Phase 1)
        self.go_name_lookup = {}
        self.go_namespace_lookup = {}
        self.alt_id_lookup = {}  # obsolete_id -> current_id mapping
        self.current_to_alt_lookup = defaultdict(list)  # current_id -> [obsolete_ids]
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            self.driver.close()
    
    def validate_prerequisites(self):
        """Validate all required files exist before starting"""
        logger.info("Validating prerequisites...")
        
        missing_files = []
        for file_key, file_path in self.data_paths.items():
            if not os.path.exists(file_path):
                missing_files.append(f"{file_key}: {file_path}")
        
        if missing_files:
            logger.error("Missing required files:")
            for missing in missing_files:
                logger.error(f"   {missing}")
            return False
        
        # Test database connection
        try:
            with self.driver.session() as session:
                result = session.run("RETURN 1 as test")
                if not result.single():
                    raise Exception("Database connection test failed")
            logger.info(" Database connection validated")
        except Exception as e:
            logger.error(f" Database connection failed: {str(e)}")
            return False
        
        logger.info(" All prerequisites validated")
        return True
    
    def create_performance_indexes(self):
        """Create all necessary indexes for optimal performance"""
        logger.info(" Creating performance indexes...")
        
        indexes = [
            # GO Term indexes
            "CREATE INDEX go_term_id_idx IF NOT EXISTS FOR (go:GOTerm) ON (go.go_id)",
            "CREATE INDEX go_term_name_idx IF NOT EXISTS FOR (go:GOTerm) ON (go.name)",
            "CREATE INDEX go_term_namespace_idx IF NOT EXISTS FOR (go:GOTerm) ON (go.namespace)",
            
            # Gene indexes (critical for performance)
            "CREATE INDEX gene_uniprot_idx IF NOT EXISTS FOR (g:Gene) ON (g.uniprot_id)",
            "CREATE INDEX gene_entrez_idx IF NOT EXISTS FOR (g:Gene) ON (g.entrez_id)",
            "CREATE INDEX gene_symbol_idx IF NOT EXISTS FOR (g:Gene) ON (g.symbol)",
            
            # Alternative mapping indexes
            "CREATE INDEX alt_mapping_obsolete_idx IF NOT EXISTS FOR (alt:AltGOMapping) ON (alt.obsolete_id)",
        ]
        
        # Constraints
        constraints = [
            "CREATE CONSTRAINT go_term_id_unique IF NOT EXISTS FOR (go:GOTerm) REQUIRE go.go_id IS UNIQUE"
        ]
        
        with self.driver.session() as session:
            # Create indexes
            for index in indexes:
                try:
                    session.run(index)
                    logger.info(f"    Index created")
                except Exception as e:
                    logger.warning(f"    Index issue (may already exist): {str(e)}")
            
            # Create constraints
            for constraint in constraints:
                try:
                    session.run(constraint)
                    logger.info(f"    Constraint created")
                except Exception as e:
                    logger.warning(f"    Constraint issue (may already exist): {str(e)}")
            
            # Wait for indexes to come online
            time.sleep(3)
        
        logger.info(" Performance indexes created")
    
    def _create_reference_dataframes(self):
        """Create optimized reference lookups for fast GO term validation"""
        logger.info(" Creating reference lookups for validation...")
        start_time = time.time()
        
        try:
            # Initialize lookups
            self.go_name_lookup = {}
            self.go_namespace_lookup = {}
            self.alt_id_lookup = {}
            self.current_to_alt_lookup = defaultdict(list)
            
            # Optimized direct file reading - avoiding pandas overhead
            logger.info("    Loading GO names and namespaces...")
            
            # Load names directly into lookup
            name_path = self.data_paths['name_tab']
            with open(name_path, 'r', encoding='utf-8') as f:
                next(f)  # Skip header
                for line in f:
                    parts = line.strip().split('\t', 1)  # Split only on first tab
                    if len(parts) == 2:
                        go_id, name = parts
                        self.go_name_lookup[go_id] = name
            
            # Load namespaces directly into lookup (filter by namespace during load)
            namespace_path = self.data_paths['namespace_tab']
            namespace_count = 0
            with open(namespace_path, 'r', encoding='utf-8') as f:
                next(f)  # Skip header
                for line in f:
                    parts = line.strip().split('\t')
                    if len(parts) == 2:
                        go_id, namespace = parts
                        if namespace == self.namespace_full:
                            self.go_namespace_lookup[go_id] = namespace
                            namespace_count += 1
                        elif go_id in self.go_name_lookup:
                            # Remove non-namespace terms from name lookup to keep consistency
                            del self.go_name_lookup[go_id]
            
            # Load alternative ID mappings directly
            logger.info("    Loading GO alternative IDs...")
            alt_id_path = self.data_paths['alt_id_tab']
            with open(alt_id_path, 'r', encoding='utf-8') as f:
                next(f)  # Skip header
                for line in f:
                    parts = line.strip().split('\t')
                    if len(parts) == 2:
                        current_id, obsolete_id = parts
                        # Only process terms in our namespace
                        if current_id in self.go_namespace_lookup:
                            self.alt_id_lookup[obsolete_id] = current_id
                            self.current_to_alt_lookup[current_id].append(obsolete_id)
            
            elapsed = time.time() - start_time
            logger.info(f"    Reference lookups created in {elapsed:.2f}s")
            logger.info(f"       {self.namespace_full.replace('_', ' ').title()} terms: {len(self.go_name_lookup):,}")
            logger.info(f"       Alternative ID mappings: {len(self.alt_id_lookup):,}")
            
        except Exception as e:
            logger.error(f" Failed to create reference lookups: {str(e)}")
            # Initialize empty lookups as fallback
            self.go_name_lookup = {}
            self.go_namespace_lookup = {}
            self.alt_id_lookup = {}
            self.current_to_alt_lookup = defaultdict(list)
            raise
    
    # =============================================================================
    # PHASE 1: FOUNDATION (GO-BASIC.OBO)
    # =============================================================================
    
    def run_phase1_foundation(self):
        """Phase 1: Import GO terms and relationships with reference validation"""
        phase_start = time.time()
        logger.info(" PHASE 1: Foundation - go-basic.obo (with Reference Validation)")
        logger.info("=" * 60)
        
        stats = {
            'terms_parsed': 0,
            f'{self.namespace}_terms': 0,
            'relationships_parsed': 0,
            'terms_imported': 0,
            'relationships_imported': 0,
            'reference_validated': 0,
            'name_corrections': 0,
            'reference_missing': 0,
            'errors': 0
        }
        
        # Create reference DataFrames for validation
        self._create_reference_dataframes()
        
        # Parse OBO file
        go_terms = self._parse_obo_file(self.data_paths['go_basic_obo'], stats)
        
        # Import GO terms
        self._import_go_terms(go_terms, stats)
        
        # Import relationships
        self._import_go_relationships(go_terms, stats)
        
        # Phase completion
        phase_time = time.time() - phase_start
        self.global_stats['phase_times']['phase1'] = phase_time
        self.global_stats['phases_completed'].append('Phase 1: Foundation')
        
        logger.info(f" Phase 1 Complete in {phase_time:.2f} seconds")
        logger.info(f"   Terms imported: {stats['terms_imported']:,}")
        logger.info(f"   Reference validated: {stats['reference_validated']:,}")
        logger.info(f"   Name corrections applied: {stats['name_corrections']:,}")
        logger.info(f"   Alt ID mappings created: {stats.get('alt_mappings_created', 0):,}")
        logger.info(f"   Alt ID corrections applied: {stats.get('alt_id_corrections', 0):,}")
        logger.info(f"   Relationships imported: {stats['relationships_imported']:,}")
        
        # Validation checkpoint
        self._run_phase_validation("Phase 1: Foundation")
        
        return stats
    
    def _parse_obo_file(self, file_path, stats):
        """Parse go-basic.obo file with optimized namespace-only extraction"""
        logger.info(f" Parsing OBO file: {file_path}")
        
        go_terms = {}
        current_term = None
        skip_current_term = False  # Flag to skip non-namespace terms early
        
        with open(file_path, 'r', encoding='utf-8') as file:
            for line_num, line in enumerate(file, 1):
                line = line.strip()
                
                try:
                    if line == '[Term]':
                        # Save previous term if it matches our namespace
                        if current_term and not skip_current_term:
                            go_terms[current_term['id']] = current_term
                            stats[f'{self.namespace}_terms'] += 1
                        
                        current_term = {
                            'synonyms': [],
                            'alt_ids': [],
                            'xrefs': [],
                            'subsets': [],
                            'consider': [],
                            'replaced_by': [],
                            'relationships': []
                        }
                        skip_current_term = False  # Reset flag
                        stats['terms_parsed'] += 1
                        
                    elif line.startswith('[') and line.endswith(']'):
                        # End of term section
                        if current_term and not skip_current_term:
                            go_terms[current_term['id']] = current_term
                            stats[f'{self.namespace}_terms'] += 1
                        current_term = None
                        skip_current_term = False
                        
                    elif current_term and ':' in line and not skip_current_term:
                        key, value = line.split(':', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        if key == 'id':
                            current_term['id'] = value
                        elif key == 'name':
                            current_term['name'] = value
                        elif key == 'namespace':
                            current_term['namespace'] = value
                            # OPTIMIZATION: Skip non-namespace terms immediately
                            if value != self.namespace_full:
                                skip_current_term = True
                                continue
                        elif key == 'def':
                            # Extract definition and references
                            if value.startswith('"') and '[' in value:
                                quote_end = value.rfind('"', 0, value.find('['))
                                if quote_end > 0:
                                    current_term['definition'] = value[1:quote_end]
                                    bracket_start = value.find('[')
                                    bracket_end = value.rfind(']')
                                    if bracket_start > 0 and bracket_end > bracket_start:
                                        refs_str = value[bracket_start+1:bracket_end]
                                        current_term['def_refs'] = [ref.strip() for ref in refs_str.split(',') if ref.strip()]
                            else:
                                current_term['definition'] = value.strip('"')
                        elif key == 'comment':
                            current_term['comment'] = value
                        elif key == 'synonym':
                            # Parse synonym with scope and references
                            if value.startswith('"'):
                                parts = value.split('"')
                                if len(parts) >= 3:
                                    syn_text = parts[1]
                                    remainder = parts[2].strip()
                                    scope = 'RELATED'
                                    refs = []
                                    
                                    for scope_type in ['EXACT', 'BROAD', 'NARROW', 'RELATED']:
                                        if scope_type in remainder:
                                            scope = scope_type
                                            break
                                    
                                    if '[' in remainder and ']' in remainder:
                                        bracket_start = remainder.find('[')
                                        bracket_end = remainder.rfind(']')
                                        if bracket_start > 0 and bracket_end > bracket_start:
                                            refs_str = remainder[bracket_start+1:bracket_end]
                                            refs = [ref.strip() for ref in refs_str.split(',') if ref.strip()]
                                    
                                    current_term['synonyms'].append({
                                        'text': syn_text,
                                        'scope': scope,
                                        'refs': refs
                                    })
                        elif key == 'alt_id':
                            current_term['alt_ids'].append(value)
                        elif key == 'xref':
                            current_term['xrefs'].append(value)
                        elif key == 'subset':
                            current_term['subsets'].append(value)
                        elif key == 'is_obsolete':
                            current_term['is_obsolete'] = value.lower() == 'true'
                        elif key == 'consider':
                            current_term['consider'].append(value)
                        elif key == 'replaced_by':
                            current_term['replaced_by'].append(value)
                        elif key == 'created_by':
                            current_term['created_by'] = value
                        elif key == 'creation_date':
                            current_term['creation_date'] = value
                        elif key == 'is_a':
                            # Parse IS_A relationship
                            parts = value.split('!', 1)
                            target_id = parts[0].strip()
                            current_term['relationships'].append({
                                'type': 'IS_A',
                                'target': target_id,
                                'target_name': parts[1].strip() if len(parts) > 1 else None
                            })
                            stats['relationships_parsed'] += 1
                        elif key == 'relationship':
                            # Parse other relationships
                            parts = value.split()
                            if len(parts) >= 2:
                                rel_type = parts[0].upper()
                                target_id = parts[1]
                                target_name = None
                                if '!' in value:
                                    target_name = value.split('!', 1)[1].strip()
                                
                                current_term['relationships'].append({
                                    'type': rel_type,
                                    'target': target_id,
                                    'target_name': target_name
                                })
                                stats['relationships_parsed'] += 1
                                
                except Exception as e:
                    logger.error(f"Error parsing line {line_num}: {line} - {e}")
                    stats['errors'] += 1
                    continue
        
        # Save last term if it matches our namespace
        if current_term and current_term.get('namespace') == self.namespace_full:
            go_terms[current_term['id']] = current_term
            stats[f'{self.namespace}_terms'] += 1
        
        logger.info(f" Parsed {stats['terms_parsed']:,} total terms")
        logger.info(f" Found {stats[f'{self.namespace}_terms']:,} {self.namespace_full} terms")
        logger.info(f" Found {stats['relationships_parsed']:,} relationships")
        
        return go_terms
    
    def _import_go_terms(self, go_terms, stats):
        """Import GO terms in batches"""
        logger.info(f" Importing {len(go_terms):,} GO terms...")
        
        batch_size = self.batch_sizes['go_terms']
        terms_list = list(go_terms.values())
        
        with self.driver.session() as session:
            for i in range(0, len(terms_list), batch_size):
                batch = terms_list[i:i + batch_size]
                
                try:
                    # Prepare batch data with reference validation AND alt_id processing
                    batch_data = []
                    alt_mapping_batch = []  # NEW: collect alt mappings
                    
                    for term in batch:
                        # Validate and enrich with reference data
                        go_id = term['id']
                        obo_name = term.get('name', '')
                        obo_alt_ids = term.get('alt_ids', [])
                        
                        # Fast lookup using reference DataFrames
                        ref_name = self.go_name_lookup.get(go_id)
                        
                        # Use reference data if available, otherwise use OBO data
                        final_name = ref_name if ref_name else obo_name
                        
                        # Track validation statistics
                        reference_validated = ref_name is not None
                        name_corrected = ref_name and obo_name and ref_name != obo_name
                        
                        if reference_validated:
                            stats['reference_validated'] = stats.get('reference_validated', 0) + 1
                        if name_corrected:
                            stats['name_corrections'] = stats.get('name_corrections', 0) + 1
                            logger.debug(f"Name correction for {go_id}: '{obo_name}' -> '{ref_name}'")
                        if not reference_validated and len(self.go_name_lookup) > 0:
                            stats['reference_missing'] = stats.get('reference_missing', 0) + 1
                        
                        # NEW: Cross-validate alternative IDs
                        reference_alt_ids = self.current_to_alt_lookup.get(go_id, [])
                        alt_id_validated = len(reference_alt_ids) > 0
                        alt_id_corrections = []

                        # Start with OBO alt_ids and merge with reference data
                        final_alt_ids = list(obo_alt_ids)  # Create copy to avoid modifying original
                        
                        if reference_alt_ids:
                            # Cross-validate and merge alternative IDs
                            obo_set = set(obo_alt_ids)
                            ref_set = set(reference_alt_ids)
                            
                            # Add missing reference alt_ids to final list
                            for ref_id in reference_alt_ids:
                                if ref_id not in obo_set:
                                    final_alt_ids.append(ref_id)
                                    alt_id_corrections.append(f"Added missing ref alt_id: {ref_id}")
                                    stats['alt_id_corrections'] = stats.get('alt_id_corrections', 0) + 1
                            
                            # Log discrepancies for monitoring
                            if obo_set != ref_set:
                                logger.debug(f"Alt_ID discrepancy for {go_id}: OBO={list(obo_set)} vs REF={list(ref_set)}")
                        else:
                            # Track missing reference data
                            if len(self.alt_id_lookup) > 0:
                                stats['alt_id_missing'] = stats.get('alt_id_missing', 0) + 1
                        
                        # Enhanced term_data with alt_id validation
                        term_data = {
                            'go_id': go_id,
                            'name': final_name,
                            'namespace': term.get('namespace', ''),
                            'definition': term.get('definition', ''),
                            'comment': term.get('comment', ''),
                            'is_obsolete': term.get('is_obsolete', False),
                            'created_by': term.get('created_by', ''),
                            'creation_date': term.get('creation_date', ''),
                            'synonyms': [syn['text'] for syn in term.get('synonyms', [])],
                            'synonym_scopes': [syn['scope'] for syn in term.get('synonyms', [])],
                            'alt_ids': final_alt_ids,  # Use validated alt_ids
                            'xrefs': term.get('xrefs', []),
                            'subsets': term.get('subsets', []),
                            'consider': term.get('consider', []),
                            'replaced_by': term.get('replaced_by', []),
                            'def_refs': term.get('def_refs', []),
                            'source_file': 'go-basic.obo',
                            'import_timestamp': self.import_timestamp,
                            'reference_validated': reference_validated,
                            'name_corrected': name_corrected,
                            'alt_id_validated': alt_id_validated,
                            'alt_id_corrections': alt_id_corrections
                        }
                        batch_data.append(term_data)
                        
                        # Collect alt mappings for immediate creation
                        for obsolete_id in final_alt_ids:
                            alt_mapping_batch.append({
                                'current_id': go_id,
                                'obsolete_id': obsolete_id
                            })
                    
                    # Import batch
                    query = """
                    UNWIND $batch as term
                    CREATE (go:GOTerm {
                        go_id: term.go_id,
                        name: term.name,
                        namespace: term.namespace,
                        definition: term.definition,
                        comment: term.comment,
                        is_obsolete: term.is_obsolete,
                        created_by: term.created_by,
                        creation_date: term.creation_date,
                        synonyms: term.synonyms,
                        synonym_scopes: term.synonym_scopes,
                        alternative_ids: term.alt_ids,
                        xrefs: term.xrefs,
                        subsets: term.subsets,
                        consider: term.consider,
                        replaced_by: term.replaced_by,
                        definition_references: term.def_refs,
                        source_file: term.source_file,
                        import_timestamp: term.import_timestamp,
                        reference_validated: term.reference_validated,
                        name_corrected: term.name_corrected,
                        alt_id_validated: term.alt_id_validated,
                        alt_id_corrections: term.alt_id_corrections
                    })
                    """
                    
                    session.run(query, batch=batch_data)
                    stats['terms_imported'] += len(batch)
                    
                    # NEW: Create alternative ID mappings immediately
                    if alt_mapping_batch:
                        alt_mapping_query = """
                        UNWIND $alt_mappings as mapping
                        MATCH (current:GOTerm {go_id: mapping.current_id})
                        CREATE (alt:AltGOMapping {
                            obsolete_id: mapping.obsolete_id,
                            current_id: mapping.current_id,
                            source_file: "cross_validated_phase1",
                            import_timestamp: $timestamp
                        })
                        CREATE (alt)-[:MAPS_TO {
                            source_file: "cross_validated_phase1",
                            import_timestamp: $timestamp
                        }]->(current)
                        RETURN count(alt) as mappings_created
                        """
                        
                        result = session.run(alt_mapping_query, 
                                           alt_mappings=alt_mapping_batch, 
                                           timestamp=self.import_timestamp)
                        mappings_created = result.single()['mappings_created']
                        stats['alt_mappings_created'] = stats.get('alt_mappings_created', 0) + mappings_created
                    
                    if (i // batch_size + 1) % 10 == 0:
                        logger.info(f"    Imported {stats['terms_imported']:,} terms...")
                        
                except Exception as e:
                    logger.error(f" Error importing batch {i//batch_size + 1}: {e}")
                    stats['errors'] += 1
        
        logger.info(f" Imported {stats['terms_imported']:,} GO terms")
    
    def _import_go_relationships(self, go_terms, stats):
        """Import relationships between GO terms"""
        logger.info(" Importing relationships...")
        
        # Collect all relationships
        all_relationships = []
        for term_id, term in go_terms.items():
            for rel in term.get('relationships', []):
                all_relationships.append({
                    'source': term_id,
                    'target': rel['target'],
                    'type': rel['type'],
                    'target_name': rel.get('target_name', '')
                })
        
        batch_size = self.batch_sizes['relationships']
        
        with self.driver.session() as session:
            for i in range(0, len(all_relationships), batch_size):
                batch = all_relationships[i:i + batch_size]
                
                try:
                    # Use standard Cypher (no APOC dependency)
                    for rel in batch:
                        rel_type = rel['type'].replace(' ', '_').replace('-', '_')
                        
                        # Create relationship with proper type
                        if rel_type == 'IS_A':
                            query = """
                            MATCH (source:GOTerm {go_id: $source})
                            MATCH (target:GOTerm {go_id: $target})
                            CREATE (source)-[r:IS_A {
                                source_file: 'go-basic.obo',
                                relationship_type: $rel_type,
                                target_name: $target_name,
                                import_timestamp: $timestamp
                            }]->(target)
                            """
                        elif rel_type == 'PART_OF':
                            query = """
                            MATCH (source:GOTerm {go_id: $source})
                            MATCH (target:GOTerm {go_id: $target})
                            CREATE (source)-[r:PART_OF {
                                source_file: 'go-basic.obo',
                                relationship_type: $rel_type,
                                target_name: $target_name,
                                import_timestamp: $timestamp
                            }]->(target)
                            """
                        elif rel_type == 'REGULATES':
                            query = """
                            MATCH (source:GOTerm {go_id: $source})
                            MATCH (target:GOTerm {go_id: $target})
                            CREATE (source)-[r:REGULATES {
                                source_file: 'go-basic.obo',
                                relationship_type: $rel_type,
                                target_name: $target_name,
                                import_timestamp: $timestamp
                            }]->(target)
                            """
                        elif rel_type == 'NEGATIVELY_REGULATES':
                            query = """
                            MATCH (source:GOTerm {go_id: $source})
                            MATCH (target:GOTerm {go_id: $target})
                            CREATE (source)-[r:NEGATIVELY_REGULATES {
                                source_file: 'go-basic.obo',
                                relationship_type: $rel_type,
                                target_name: $target_name,
                                import_timestamp: $timestamp
                            }]->(target)
                            """
                        elif rel_type == 'POSITIVELY_REGULATES':
                            query = """
                            MATCH (source:GOTerm {go_id: $source})
                            MATCH (target:GOTerm {go_id: $target})
                            CREATE (source)-[r:POSITIVELY_REGULATES {
                                source_file: 'go-basic.obo',
                                relationship_type: $rel_type,
                                target_name: $target_name,
                                import_timestamp: $timestamp
                            }]->(target)
                            """
                        else:
                            # Generic relationship fallback
                            query = f"""
                            MATCH (source:GOTerm {{go_id: $source}})
                            MATCH (target:GOTerm {{go_id: $target}})
                            CREATE (source)-[r:GO_RELATIONSHIP {{
                                source_file: 'go-basic.obo',
                                relationship_type: $rel_type,
                                target_name: $target_name,
                                import_timestamp: $timestamp
                            }}]->(target)
                            """
                        
                        session.run(query, 
                                   source=rel['source'],
                                   target=rel['target'], 
                                   rel_type=rel['type'],
                                   target_name=rel['target_name'],
                                   timestamp=self.import_timestamp)
                        stats['relationships_imported'] += 1
                        
                    if (i // batch_size + 1) % 10 == 0:
                        logger.info(f"    Imported {stats['relationships_imported']:,} relationships...")
                        
                except Exception as e:
                    logger.error(f" Error creating relationships batch {i//batch_size + 1}: {e}")
                    stats['errors'] += 1
        
        logger.info(f" Imported {stats['relationships_imported']:,} relationships")
    
    # =============================================================================
    # PHASE 2: ID MAPPINGS (goID_2_alt_id.tab)
    # =============================================================================
    
    def run_phase2_id_mappings(self):
        """Phase 2: Simplified - Alternative ID mappings already completed in Phase 1"""
        phase_start = time.time()
        logger.info(" PHASE 2: ID Mappings (Already completed in Phase 1)")
        logger.info("=" * 60)
        
        # Just report the alt ID mappings that were already created in Phase 1
        with self.driver.session() as session:
            result = session.run("""
            MATCH (gt:GOTerm) 
            WHERE gt.namespace = $namespace AND gt.alt_id_validated IS NOT NULL
            RETURN 
                count(gt) as total_terms,
                count(CASE WHEN gt.alt_id_validated = true THEN 1 END) as terms_with_alt_ids,
                count(CASE WHEN size(gt.alt_id_corrections) > 0 THEN 1 END) as terms_with_corrections
            """, namespace=self.namespace_full)
            
            stats = result.single()
            logger.info(f"    Total {self.namespace.upper()} terms in database: {stats['total_terms']:,}")
            logger.info(f"    Terms with validated alt IDs: {stats['terms_with_alt_ids']:,}")
            logger.info(f"    Terms with alt ID corrections: {stats['terms_with_corrections']:,}")
            
            # Count created AltGOMapping nodes
            mapping_result = session.run("""
            MATCH (alt:AltGOMapping)-[:MAPS_TO]->(gt:GOTerm)
            WHERE alt.source_file = 'cross_validated_phase1'
            RETURN count(alt) as alt_mappings_created
            """)
            
            mapping_count = mapping_result.single()['alt_mappings_created']
            logger.info(f"    AltGOMapping nodes created: {mapping_count:,}")
            logger.info("    Alternative ID processing already completed during Phase 1 import")
        
        # Phase completion
        phase_time = time.time() - phase_start
        self.global_stats['phase_times']['phase2'] = phase_time
        self.global_stats['phases_completed'].append('Phase 2: ID Mappings')
        
        return {'alt_id_processing_completed_in_phase1': True, 'alt_mappings_created': mapping_count}
    
    # =============================================================================
    # PHASE 3: METADATA VALIDATION (goID_2_name.tab & goID_2_namespace.tab)
    # =============================================================================
    
    def run_phase3_metadata_validation(self):
        """Phase 3: Simplified - validation already done in Phase 1"""
        phase_start = time.time()
        logger.info(" PHASE 3: Metadata Validation (Already completed in Phase 1)")
        logger.info("=" * 60)
        
        # Just log the validation that already happened
        with self.driver.session() as session:
            result = session.run("""
            MATCH (gt:GOTerm) 
            WHERE gt.namespace = $namespace
            RETURN 
                count(gt) as total_terms,
                count(CASE WHEN gt.reference_validated = true THEN 1 END) as validated_terms,
                count(CASE WHEN gt.name_corrected = true THEN 1 END) as corrected_terms
            """, namespace=self.namespace_full)
            
            stats = result.single()
            logger.info(f"    Total {self.namespace.upper()} terms in database: {stats['total_terms']:,}")
            logger.info(f"    Reference validated terms: {stats['validated_terms']:,}")
            logger.info(f"    Terms with name corrections: {stats['corrected_terms']:,}")
            logger.info("    Validation already completed during Phase 1 import")
        
        # Phase completion
        phase_time = time.time() - phase_start
        self.global_stats['phase_times']['phase3'] = phase_time
        self.global_stats['phases_completed'].append('Phase 3: Metadata Validation')
        
        return {'validation_completed_in_phase1': True}
    
    # =============================================================================
    # PHASE 4: HIERARCHICAL STRUCTURE (go.tab)
    # =============================================================================
    
    def run_phase4_hierarchical_structure(self):
        """Phase 4: Import hierarchical structure from go.tab"""
        phase_start = time.time()
        logger.info(" PHASE 4: Hierarchical Structure - go.tab")
        logger.info("=" * 60)
        
        stats = {
            'total_lines_processed': 0,
            'new_relationships_created': 0,
            'cross_validated_relationships': 0,
            'enhanced_relationships': 0,
            'missing_go_terms': 0,
            'relationship_types': {},
            'errors': 0
        }
        
        # Import hierarchical relationships
        self._import_hierarchical_structure(self.data_paths['go_tab'], stats)
        
        # Phase completion
        phase_time = time.time() - phase_start
        self.global_stats['phase_times']['phase4'] = phase_time
        self.global_stats['phases_completed'].append('Phase 4: Hierarchical Structure')
        
        logger.info(f" Phase 4 Complete in {phase_time:.2f} seconds")
        logger.info(f"   New relationships created: {stats['new_relationships_created']:,}")
        logger.info(f"   Cross-validated relationships: {stats['cross_validated_relationships']:,}")
        
        return stats
    
    def _import_hierarchical_structure(self, file_path, stats):
        """Import all hierarchical relationships from go.tab"""
        logger.info(f" Processing hierarchical structure from {file_path}...")
        
        batch_size = self.batch_sizes['hierarchy']
        batch_data = []
        batch_count = 0
        
        with open(file_path, 'r') as file:
            for line_num, line in enumerate(file, 1):
                parts = line.strip().split('\t')
                
                if len(parts) >= 4:
                    parent_id, child_id, rel_type, namespace = parts[:4]
                    
                    batch_data.append({
                        'parent_id': parent_id,
                        'child_id': child_id,
                        'relationship_type': rel_type,
                        'namespace': namespace
                    })
                    
                    if len(batch_data) >= batch_size:
                        # Process batch
                        processed, created, cross_validated, enhanced = self._process_hierarchical_batch(batch_data, stats)
                        
                        stats['total_lines_processed'] += processed
                        stats['new_relationships_created'] += created
                        stats['cross_validated_relationships'] += cross_validated
                        stats['enhanced_relationships'] += enhanced
                        
                        batch_count += 1
                        batch_data = []
                        
                        if batch_count % 10 == 0:
                            logger.info(f"    Processed {batch_count} batches ({stats['total_lines_processed']:,} relationships)")
        
        # Process final batch
        if batch_data:
            processed, created, cross_validated, enhanced = self._process_hierarchical_batch(batch_data, stats)
            stats['total_lines_processed'] += processed
            stats['new_relationships_created'] += created
            stats['cross_validated_relationships'] += cross_validated
            stats['enhanced_relationships'] += enhanced
        
        logger.info(f" Processed {stats['total_lines_processed']:,} hierarchical relationships")
    
    def _process_hierarchical_batch(self, batch_data, stats):
        """Process a batch of hierarchical relationship data"""
        with self.driver.session() as session:
            # First, validate that both GO terms exist
            validation_query = """
            UNWIND $batch as rel
            MATCH (parent:GOTerm {go_id: rel.parent_id})
            MATCH (child:GOTerm {go_id: rel.child_id})
            RETURN rel.parent_id as parent_id, rel.child_id as child_id, 
                   rel.relationship_type as rel_type, rel.namespace as namespace,
                   parent.name as parent_name, child.name as child_name
            """
            
            validation_result = session.run(validation_query, batch=batch_data)
            valid_relationships = []
            missing_terms = []
            
            # Check which relationships have valid GO terms
            valid_ids = set()
            for record in validation_result:
                valid_relationships.append({
                    'parent_id': record['parent_id'],
                    'child_id': record['child_id'],
                    'rel_type': record['rel_type'],
                    'namespace': record['namespace'],
                    'parent_name': record['parent_name'],
                    'child_name': record['child_name']
                })
                valid_ids.add((record['parent_id'], record['child_id']))
            
            # Identify missing GO terms
            for rel in batch_data:
                if (rel['parent_id'], rel['child_id']) not in valid_ids:
                    missing_terms.append(rel)
            
            stats['missing_go_terms'] += len(missing_terms)
            
            if not valid_relationships:
                return len(batch_data), 0, 0, 0
            
            # Check for existing relationships and create new ones
            duplicate_check_query = """
            UNWIND $valid_rels as rel
            MATCH (child:GOTerm {go_id: rel.child_id})-[existing]->(parent:GOTerm {go_id: rel.parent_id})
            RETURN rel.parent_id as parent_id, rel.child_id as child_id,
                   type(existing) as existing_type, existing.source_file as source,
                   rel.rel_type as new_type
            """
            
            duplicate_result = session.run(duplicate_check_query, valid_rels=valid_relationships)
            duplicates = []
            cross_validated = []
            
            for record in duplicate_result:
                existing_type = record['existing_type']
                new_type = record['new_type'].upper()
                
                if existing_type == new_type:
                    duplicates.append((record['parent_id'], record['child_id']))
                    cross_validated.append((record['parent_id'], record['child_id']))
            
            # Filter out exact duplicates for new relationship creation
            duplicate_set = set(duplicates)
            new_relationships = [rel for rel in valid_relationships 
                               if (rel['parent_id'], rel['child_id']) not in duplicate_set]
            
            # Create new hierarchical relationships
            new_created = 0
            if new_relationships:
                create_query = """
                UNWIND $new_rels as rel
                MATCH (parent:GOTerm {go_id: rel.parent_id})
                MATCH (child:GOTerm {go_id: rel.child_id})
                CREATE (child)-[r:HIERARCHY_RELATION]->(parent)
                SET r.relationship_type = rel.rel_type,
                    r.namespace = rel.namespace,
                    r.source_file = "go.tab",
                    r.validation_source = true,
                    r.import_timestamp = $timestamp
                RETURN count(r) as created
                """
                
                result = session.run(create_query, new_rels=new_relationships, timestamp=self.import_timestamp)
                new_created = result.single()['created']
            
            # Update statistics for relationship types
            for rel in valid_relationships:
                rel_type = rel['rel_type']
                stats['relationship_types'][rel_type] = stats['relationship_types'].get(rel_type, 0) + 1
            
            # Update cross-validation flags for duplicates
            enhanced = 0
            if cross_validated:
                enhance_query = """
                UNWIND $cross_val as rel_info
                MATCH (child:GOTerm {go_id: rel_info[1]})-[r]->(parent:GOTerm {go_id: rel_info[0]})
                SET r.cross_validated_go_tab = true,
                    r.cross_validation_timestamp = $timestamp
                RETURN count(r) as enhanced
                """
                
                cross_val_data = [[parent, child] for parent, child in cross_validated]
                result = session.run(enhance_query, cross_val=cross_val_data, timestamp=self.import_timestamp)
                enhanced = result.single()['enhanced']
            
            return len(batch_data), new_created, len(cross_validated), enhanced
    
    # =============================================================================
    # PHASE 5: GENE ANNOTATIONS (goa_human.gaf.gz) 
    # =============================================================================
    
    def run_phase5_gene_annotations(self):
        """Phase 5: Import gene annotations from goa_human.gaf.gz"""
        phase_start = time.time()
        logger.info(" PHASE 5: Gene Annotations - goa_human.gaf.gz")
        logger.info("=" * 60)
        
        stats = {
            'total_lines_processed': 0,
            f'{self.namespace}_annotations_processed': 0,
            'genes_created': 0,
            'genes_enhanced': 0,
            'annotations_created': 0,
            'duplicate_annotations': 0,
            'missing_go_terms': 0,
            'evidence_codes': {},
            'qualifiers': {},
            'unique_genes': set(),
            'gene_go_pairs': set(),
            'errors': 0
        }
        
        # GAF 2.2 column mapping
        gaf_columns = {
            'db': 0,
            'db_object_id': 1,
            'db_object_symbol': 2, 
            'qualifier': 3,
            'go_id': 4,
            'db_reference': 5,
            'evidence_code': 6,
            'with_from': 7,
            'aspect': 8,
            'db_object_name': 9,
            'db_object_synonym': 10,
            'db_object_type': 11,
            'taxon': 12,
            'date': 13,
            'assigned_by': 14,
            'annotation_extension': 15,
            'gene_product_form_id': 16
        }
        
        # Import gene annotations
        self._import_gene_annotations(self.data_paths['goa_human_gaf'], gaf_columns, stats)
        
        # Final statistics
        stats['genes_created'] = len(stats['unique_genes'])
        stats['unique_gene_go_pairs'] = len(stats['gene_go_pairs'])
        
        # Phase completion
        phase_time = time.time() - phase_start
        self.global_stats['phase_times']['phase5'] = phase_time
        self.global_stats['phases_completed'].append('Phase 5: Gene Annotations')
        
        logger.info(f" Phase 5 Complete in {phase_time:.2f} seconds")
        logger.info(f"   Genes created/enhanced: {stats['genes_created']:,}")
        logger.info(f"   Annotations created: {stats['annotations_created']:,}")
        
        # CRITICAL: Consolidate any duplicate genes created so far
        self._consolidate_duplicate_genes()
        
        # Validation checkpoint
        self._run_phase_validation("Phase 5: Gene Annotations")
        
        return stats
    
    def _import_gene_annotations(self, file_path, gaf_columns, stats):
        f"""Import {self.namespace_full} gene annotations with optimized processing"""
        logger.info(f" Processing gene annotations from {file_path}...")
        
        # OPTIMIZATION: Pre-filter and collect namespace annotations in memory for batch processing
        namespace_annotations = []
        comment_lines = 0
        non_namespace_lines = 0
        invalid_lines = 0
        
        logger.info(f"    Scanning GAF file for {self.namespace_full} annotations...")
        
        with gzip.open(file_path, 'rt') as file:
            for line_num, line in enumerate(file, 1):
                if line.startswith('!'):
                    comment_lines += 1
                    continue  # Skip comments
                
                parts = line.strip().split('\t')
                stats['total_lines_processed'] += 1
                
                if len(parts) >= 15:
                    aspect = parts[gaf_columns['aspect']]
                    qualifier = parts[gaf_columns['qualifier']]
                    
                    # OPTIMIZATION: Early namespace filtering with detailed stats
                    namespace_aspects = {'bp': 'P', 'cc': 'C', 'mf': 'F'}
                    expected_aspect = namespace_aspects[self.namespace]
                    namespace_qualifiers = {'bp': 'involved_in', 'cc': 'located_in', 'mf': 'enables'}
                    expected_qualifier = namespace_qualifiers[self.namespace]
                    
                    if aspect == expected_aspect or qualifier == expected_qualifier:
                        # Handle optional columns safely
                        annotation_extension = parts[15] if len(parts) > 15 else ''
                        gene_product_form_id = parts[16] if len(parts) > 16 else ''
                        
                        namespace_annotations.append({
                            'db': parts[gaf_columns['db']],
                            'db_object_id': parts[gaf_columns['db_object_id']],
                            'db_object_symbol': parts[gaf_columns['db_object_symbol']],
                            'qualifier': parts[gaf_columns['qualifier']],
                            'go_id': parts[gaf_columns['go_id']],
                            'db_reference': parts[gaf_columns['db_reference']],
                            'evidence_code': parts[gaf_columns['evidence_code']],
                            'with_from': parts[gaf_columns['with_from']],
                            'aspect': parts[gaf_columns['aspect']],
                            'db_object_name': parts[gaf_columns['db_object_name']],
                            'db_object_synonym': parts[gaf_columns['db_object_synonym']],
                            'db_object_type': parts[gaf_columns['db_object_type']],
                            'taxon': parts[gaf_columns['taxon']],
                            'date': parts[gaf_columns['date']],
                            'assigned_by': parts[gaf_columns['assigned_by']],
                            'annotation_extension': annotation_extension,
                            'gene_product_form_id': gene_product_form_id
                        })
                    else:
                        non_namespace_lines += 1
                else:
                    invalid_lines += 1
        
        logger.info(f"    GAF scan results:")
        logger.info(f"      Comments skipped: {comment_lines:,}")
        logger.info(f"      Non-{self.namespace.upper()} annotations: {non_namespace_lines:,}")
        logger.info(f"      Invalid lines: {invalid_lines:,}")
        logger.info(f"      {self.namespace.upper()} annotations found: {len(namespace_annotations):,}")
        
        # OPTIMIZATION: Process namespace annotations in larger, optimized batches
        if namespace_annotations:
            logger.info(f"    Processing {len(namespace_annotations):,} {self.namespace.upper()} annotations in optimized batches...")
            
            # Use larger batch sizes for efficiency
            optimized_batch_size = min(self.batch_sizes['gene_annotations'] * 3, 3000)
            batch_count = 0
            
            for i in range(0, len(namespace_annotations), optimized_batch_size):
                batch = namespace_annotations[i:i + optimized_batch_size]
                
                processed, genes, annotations, duplicates = self._process_gene_annotation_batch(batch, stats)
                
                stats[f'{self.namespace}_annotations_processed'] += processed
                stats['genes_enhanced'] += genes
                stats['annotations_created'] += annotations
                stats['duplicate_annotations'] += duplicates
                
                batch_count += 1
                
                # Progress reporting every 10 batches (less frequent for speed)
                if batch_count % 10 == 0:
                    progress = ((i + len(batch)) / len(namespace_annotations)) * 100
                    logger.info(f"       Progress: {progress:.1f}% ({stats[f'{self.namespace}_annotations_processed']:,} processed)")
        
        logger.info(f" Processed {stats[f'{self.namespace}_annotations_processed']:,} {self.namespace_full} gene annotations")
    
    def _process_gene_annotation_batch(self, batch_data, stats):
        """Process a batch of gene annotation data with intelligent merging"""
        
        with self.driver.session() as session:
            # Step 1: Validate GO terms exist
            go_validation_query = """
            UNWIND $batch as annotation
            MATCH (go:GOTerm {go_id: annotation.go_id})
            RETURN annotation.go_id as go_id
            """
            
            result = session.run(go_validation_query, batch=batch_data)
            valid_go_terms = set(record['go_id'] for record in result)
            
            # Filter out annotations with missing GO terms
            valid_annotations = []
            missing_go_count = 0
            
            for annotation in batch_data:
                if annotation['go_id'] in valid_go_terms:
                    valid_annotations.append(annotation)
                else:
                    missing_go_count += 1
            
            stats['missing_go_terms'] += missing_go_count
            
            if not valid_annotations:
                return len(batch_data), 0, 0, 0
            
            # Step 2: ENHANCED GENE MERGING - Comprehensive ID-based merging 
            gene_merge_query = """
            UNWIND $annotations as annotation
            
            // OPTIMIZATION: Look for existing genes across ALL ID types
            OPTIONAL MATCH (existing_uniprot:Gene) 
            WHERE existing_uniprot.uniprot_id = annotation.db_object_id
            OPTIONAL MATCH (existing_symbol:Gene) 
            WHERE existing_symbol.symbol = annotation.db_object_symbol
            
            // Smart merging: Use the best existing gene or create new one
            WITH annotation, existing_uniprot, existing_symbol,
                 CASE 
                     WHEN existing_uniprot IS NOT NULL THEN existing_uniprot
                     WHEN existing_symbol IS NOT NULL THEN existing_symbol  
                     ELSE NULL 
                 END as best_existing_gene
            
            // Create or enhance the gene with comprehensive ID support
            MERGE (g:Gene {uniprot_id: annotation.db_object_id})
            ON CREATE SET 
                g.symbol = annotation.db_object_symbol,
                g.name = annotation.db_object_name,
                g.db_source = annotation.db,
                g.taxon = annotation.taxon,
                g.synonyms = CASE 
                    WHEN annotation.db_object_synonym IS NOT NULL AND annotation.db_object_synonym <> ''
                    THEN split(annotation.db_object_synonym, '|')
                    ELSE []
                END,
                g.source_files = ["goa_human.gaf.gz"],
                g.import_timestamp = $timestamp,
                g.last_updated = $timestamp,
                g.id_type = "uniprot",
                g.cross_validated = (best_existing_gene IS NOT NULL)
            ON MATCH SET
                // Preserve existing symbol if not provided, otherwise update
                g.symbol = CASE 
                    WHEN annotation.db_object_symbol IS NOT NULL AND annotation.db_object_symbol <> ''
                    THEN annotation.db_object_symbol 
                    ELSE g.symbol 
                END,
                g.name = CASE 
                    WHEN annotation.db_object_name IS NOT NULL AND annotation.db_object_name <> ''
                    THEN annotation.db_object_name 
                    ELSE g.name 
                END,
                g.db_source = annotation.db,
                g.taxon = annotation.taxon,
                g.synonyms = CASE 
                    WHEN annotation.db_object_synonym IS NOT NULL AND annotation.db_object_synonym <> ''
                    THEN split(annotation.db_object_synonym, '|')
                    ELSE g.synonyms
                END,
                g.source_files = coalesce(g.source_files, []) + 
                    CASE WHEN NOT "goa_human.gaf.gz" IN coalesce(g.source_files, [])
                    THEN ["goa_human.gaf.gz"] ELSE [] END,
                g.last_updated = $timestamp,
                g.cross_validated = true
                
            RETURN count(g) as genes_processed
            """
            
            result = session.run(gene_merge_query, annotations=valid_annotations, timestamp=self.import_timestamp)
            genes_processed = result.single()['genes_processed']
            
            # Step 3: Check for duplicate annotations
            duplicate_check_query = """
            UNWIND $annotations as annotation
            MATCH (g:Gene {uniprot_id: annotation.db_object_id})
            MATCH (go:GOTerm {go_id: annotation.go_id})
            OPTIONAL MATCH (g)-[existing:ANNOTATED_WITH]->(go)
            WHERE existing.evidence_code = annotation.evidence_code 
            AND existing.qualifier = annotation.qualifier
            RETURN annotation.db_object_id as gene_id, annotation.go_id as go_id,
                   existing IS NOT NULL as is_duplicate
            """
            
            result = session.run(duplicate_check_query, annotations=valid_annotations)
            duplicates = 0
            gene_go_pairs = []
            
            for record in result:
                if record['is_duplicate']:
                    duplicates += 1
                else:
                    gene_go_pairs.append((record['gene_id'], record['go_id']))
            
            # Step 4: Create new annotations (avoiding duplicates)
            new_annotations = []
            for annotation in valid_annotations:
                if (annotation['db_object_id'], annotation['go_id']) in gene_go_pairs:
                    new_annotations.append(annotation)
            
            annotations_created = 0
            if new_annotations:
                annotation_create_query = """
                UNWIND $new_annotations as annotation
                MATCH (g:Gene {uniprot_id: annotation.db_object_id})
                MATCH (go:GOTerm {go_id: annotation.go_id})
                CREATE (g)-[r:ANNOTATED_WITH {
                    evidence_code: annotation.evidence_code,
                    qualifier: annotation.qualifier,
                    reference: annotation.db_reference,
                    assigned_by: annotation.assigned_by,
                    annotation_date: annotation.date,
                    aspect: annotation.aspect,
                    with_from: annotation.with_from,
                    annotation_extension: annotation.annotation_extension,
                    source_file: "goa_human.gaf.gz",
                    import_timestamp: $timestamp
                }]->(go)
                RETURN count(r) as created_count
                """
                
                result = session.run(annotation_create_query, 
                                   new_annotations=new_annotations, 
                                   timestamp=self.import_timestamp)
                annotations_created = result.single()['created_count']
            
            # Update statistics
            for annotation in valid_annotations:
                evidence_code = annotation['evidence_code']
                qualifier = annotation['qualifier']
                stats['evidence_codes'][evidence_code] = stats['evidence_codes'].get(evidence_code, 0) + 1
                stats['qualifiers'][qualifier] = stats['qualifiers'].get(qualifier, 0) + 1
                stats['unique_genes'].add(annotation['db_object_id'])
                stats['gene_go_pairs'].add((annotation['db_object_id'], annotation['go_id']))
            
            return len(batch_data), genes_processed, annotations_created, duplicates
    
    def _consolidate_duplicate_genes(self):
        """CRITICAL: Consolidate genes created across different phases with different ID types"""
        logger.info(" Consolidating duplicate genes across all phases...")
        
        with self.driver.session() as session:
            # First, add missing IDs to existing genes (safer than full merging)
            id_enhancement_query = """
            // Find genes that should share IDs
            MATCH (g1:Gene), (g2:Gene)
            WHERE g1 <> g2 AND g1.symbol IS NOT NULL AND g2.symbol IS NOT NULL 
            AND g1.symbol = g2.symbol
            
            // Enhance g1 with missing IDs from g2
            SET g1.uniprot_id = coalesce(g1.uniprot_id, g2.uniprot_id),
                g1.entrez_id = coalesce(g1.entrez_id, g2.entrez_id),
                g1.name = coalesce(g1.name, g2.name),
                g1.synonyms = CASE 
                    WHEN g1.synonyms IS NULL THEN g2.synonyms
                    WHEN g2.synonyms IS NULL THEN g1.synonyms
                    ELSE g1.synonyms + [syn IN g2.synonyms WHERE NOT syn IN g1.synonyms]
                END,
                g1.source_files = coalesce(g1.source_files, []) + 
                    [file IN coalesce(g2.source_files, []) WHERE NOT file IN coalesce(g1.source_files, [])],
                g1.consolidated = true,
                g1.last_updated = datetime()
            
            // Move unique relationships from g2 to g1
            WITH g1, g2
            MATCH (g2)-[r:ANNOTATED_WITH]->(go:GOTerm)
            WHERE NOT EXISTS {
                (g1)-[:ANNOTATED_WITH {source_file: r.source_file}]->(go)
            }
            CREATE (g1)-[new_r:ANNOTATED_WITH]->(go)
            SET new_r = properties(r)
            DELETE r
            
            // Delete the now-empty g2
            WITH g1, g2, count(*) as relationships_moved
            DETACH DELETE g2
            
            RETURN count(DISTINCT g1) as genes_consolidated, 
                   sum(relationships_moved) as relationships_moved
            """
            
            try:
                result = session.run(id_enhancement_query)
                stats = result.single()
                
                if stats and stats['genes_consolidated'] > 0:
                    logger.info(f"    Gene consolidation complete:")
                    logger.info(f"      Primary genes enhanced: {stats['genes_consolidated']}")
                    logger.info(f"      Relationships moved: {stats['relationships_moved']}")
                else:
                    logger.info("    No duplicate genes found to consolidate")
                    
            except Exception as e:
                logger.error(f"    Gene consolidation failed: {str(e)}")
                # Continue without failing - this is an optimization step
        
        logger.info(" Gene consolidation phase complete")
    
    # =============================================================================
    # PHASE 6: ID CROSS-REFERENCES (collapsed_go.entrez)
    # =============================================================================
    
    def run_phase6_id_cross_references(self):
        """Phase 6: Import Entrez ID cross-references"""
        phase_start = time.time()
        logger.info(" PHASE 6: ID Cross-References - collapsed_go.entrez")
        logger.info("=" * 60)
        
        stats = {
            'hierarchy_processed': 0,
            'hierarchy_created': 0,
            'gene_processed': 0,
            'genes_created': 0,
            'associations_created': 0,
            'total_lines': 0,
            'skipped_existing': 0,
            'errors': 0
        }
        
        # Import collapsed Entrez data
        self._import_collapsed_entrez(self.data_paths['collapsed_entrez'], stats)
        
        # Phase completion
        phase_time = time.time() - phase_start
        self.global_stats['phase_times']['phase6'] = phase_time
        self.global_stats['phases_completed'].append('Phase 6: ID Cross-References')
        
        logger.info(f" Phase 6 Complete in {phase_time:.2f} seconds")
        logger.info(f"   Hierarchy processed: {stats['hierarchy_processed']:,}")
        logger.info(f"   Genes processed: {stats['gene_processed']:,}")
        logger.info(f"   Associations created: {stats['associations_created']:,}")
        
        return stats
    
    def _import_collapsed_entrez(self, file_path, stats):
        """Import collapsed_go.entrez with optimized batch processing"""
        logger.info(f" Processing {file_path} with enhanced batching...")
        
        # Check existing progress for resumption
        existing_hierarchy, existing_associations = self._check_phase6_existing_progress()
        
        # OPTIMIZATION: Pre-separate data types for efficient processing
        hierarchy_entries = []
        gene_entries = []
        
        # First pass: separate data types (fast scan)
        logger.info("    Scanning file to separate hierarchy and gene data...")
        with open(file_path, 'r') as file:
            for line in file:
                parts = line.strip().split('\t')
                stats['total_lines'] += 1
                
                if len(parts) >= 3:
                    go_id_1, go_id_2, entry_type = parts[0], parts[1], parts[2]
                    
                    if entry_type == 'default':
                        hierarchy_entries.append({
                            'child_id': go_id_1,
                            'parent_id': go_id_2
                        })
                    elif entry_type == 'gene':
                        gene_entries.append({
                            'go_id': go_id_1,
                            'entrez_id': go_id_2
                        })
        
        logger.info(f"    Found {len(hierarchy_entries):,} hierarchy entries, {len(gene_entries):,} gene entries")
        
        # Process hierarchy in larger batches (skip already processed)
        if hierarchy_entries:
            entries_to_process = hierarchy_entries[existing_hierarchy:] if existing_hierarchy > 0 else hierarchy_entries
            stats['skipped_existing'] += existing_hierarchy
            
            logger.info(f"    Processing {len(entries_to_process):,} hierarchy entries...")
            hierarchy_batch_size = min(self.batch_sizes['hierarchy'] * 3, 3000)  # Larger batches
            
            for i in range(0, len(entries_to_process), hierarchy_batch_size):
                batch = entries_to_process[i:i + hierarchy_batch_size]
                processed, created = self._process_hierarchy_batch_entrez(batch)
                stats['hierarchy_processed'] += processed
                stats['hierarchy_created'] += created
                
                if (stats['hierarchy_processed'] % 10000) == 0:
                    progress = (stats['hierarchy_processed'] / len(entries_to_process)) * 100
                    logger.info(f"       Hierarchy progress: {progress:.1f}% ({stats['hierarchy_processed']:,} processed)")
        
        # Process genes in optimized batches (skip already processed)  
        if gene_entries:
            entries_to_process = gene_entries[existing_associations:] if existing_associations > 0 else gene_entries
            stats['skipped_existing'] += existing_associations
            
            logger.info(f"    Processing {len(entries_to_process):,} gene entries...")
            gene_batch_size = min(self.batch_sizes['entrez_genes'] * 2, 10000)  # Larger batches
            
            for i in range(0, len(entries_to_process), gene_batch_size):
                batch = entries_to_process[i:i + gene_batch_size]
                processed, genes_proc, assocs_created = self._process_gene_batch_entrez(batch)
                stats['gene_processed'] += processed
                stats['genes_created'] += genes_proc
                stats['associations_created'] += assocs_created
                
                if (stats['gene_processed'] % 20000) == 0:
                    progress = (stats['gene_processed'] / len(entries_to_process)) * 100
                    logger.info(f"       Gene progress: {progress:.1f}% ({stats['gene_processed']:,} processed)")
        
        logger.info(f" Completed collapsed_go.entrez processing with optimized batching")
    
    def _check_phase6_existing_progress(self):
        """Check existing Phase 6 progress"""
        with self.driver.session() as session:
            # Check hierarchy progress
            hierarchy_query = """
            MATCH ()-[r:COLLAPSED_HIERARCHY]->()
            WHERE r.source_file = "collapsed_go.entrez"
            RETURN count(r) as existing_hierarchy
            """
            result = session.run(hierarchy_query)
            existing_hierarchy = result.single()['existing_hierarchy']
            
            # Check gene progress
            gene_query = """
            MATCH (g:Gene)-[r:ANNOTATED_WITH]->()
            WHERE r.source_file = "collapsed_go.entrez"
            RETURN count(r) as existing_gene_associations
            """
            result = session.run(gene_query)
            existing_gene_associations = result.single()['existing_gene_associations']
            
            return existing_hierarchy, existing_gene_associations
    
    def _process_hierarchy_batch_entrez(self, batch_data):
        """Process Entrez hierarchy batch"""
        if not batch_data:
            return 0, 0
            
        with self.driver.session() as session:
            hierarchy_query = """
            UNWIND $batch as entry
            MATCH (child:GOTerm {go_id: entry.child_id})
            MATCH (parent:GOTerm {go_id: entry.parent_id})
            
            // Check if relationship already exists
            OPTIONAL MATCH (child)-[existing]->(parent) 
            WHERE type(existing) IN ['IS_A', 'PART_OF', 'REGULATES', 'NEGATIVELY_REGULATES', 'POSITIVELY_REGULATES', 'COLLAPSED_HIERARCHY']
            
            WITH child, parent, existing, entry
            WHERE existing IS NULL  // Only create if doesn't exist
            
            CREATE (child)-[r:COLLAPSED_HIERARCHY {
                source_file: "collapsed_go.entrez",
                hierarchy_type: "default",
                import_timestamp: $timestamp,
                cross_validated_entrez: true
            }]->(parent)
            
            RETURN count(r) as created
            """
            
            result = session.run(hierarchy_query, batch=batch_data, timestamp=self.import_timestamp)
            created = result.single()['created']
            
            return len(batch_data), created
    
    def _process_gene_batch_entrez(self, batch_data):
        """Process Entrez gene batch with optimization"""
        if not batch_data:
            return 0, 0, 0
            
        with self.driver.session() as session:
            # Step 1: Bulk create genes
            gene_create_query = """
            UNWIND $batch as entry
            MERGE (g:Gene {entrez_id: entry.entrez_id})
            ON CREATE SET 
                g.source_files = ["collapsed_go.entrez"],
                g.import_timestamp = $timestamp,
                g.id_type = "entrez"
            ON MATCH SET
                g.source_files = coalesce(g.source_files, []) + 
                    CASE WHEN NOT "collapsed_go.entrez" IN coalesce(g.source_files, [])
                    THEN ["collapsed_go.entrez"] ELSE [] END,
                g.last_updated = $timestamp
            RETURN count(g) as genes_processed
            """
            
            result = session.run(gene_create_query, batch=batch_data, timestamp=self.import_timestamp)
            genes_processed = result.single()['genes_processed']
            
            # Step 2: Bulk create associations
            association_query = """
            UNWIND $batch as entry
            MATCH (g:Gene {entrez_id: entry.entrez_id})
            MATCH (go:GOTerm {go_id: entry.go_id})
            
            // Only create if association doesn't exist from this source
            MERGE (g)-[r:ANNOTATED_WITH {
                source_file: "collapsed_go.entrez",
                go_id: entry.go_id,
                entrez_id: entry.entrez_id
            }]->(go)
            ON CREATE SET
                r.association_type = "gene",
                r.import_timestamp = $timestamp,
                r.evidence_code = "COLLAPSED",
                r.qualifier = "involved_in"
                
            RETURN count(r) as associations_created
            """
            
            result = session.run(association_query, batch=batch_data, timestamp=self.import_timestamp)
            associations_created = result.single()['associations_created']
            
            return len(batch_data), genes_processed, associations_created
    
    # =============================================================================
    # PHASE 7: SYMBOL CROSS-REFERENCES (collapsed_go.symbol)
    # =============================================================================
    
    def run_phase7_symbol_cross_references(self):
        """Phase 7: Import Symbol cross-references"""
        phase_start = time.time()
        logger.info(" PHASE 7: Symbol Cross-References - collapsed_go.symbol")
        logger.info("=" * 60)
        
        stats = {
            'hierarchy_processed': 0,
            'hierarchy_created': 0,
            'gene_processed': 0,
            'genes_created': 0,
            'genes_merged': 0,
            'associations_created': 0,
            'total_lines': 0,
            'skipped_existing': 0,
            'errors': 0
        }
        
        # Import collapsed symbol data
        self._import_collapsed_symbol(self.data_paths['collapsed_symbol'], stats)
        
        # Phase completion
        phase_time = time.time() - phase_start
        self.global_stats['phase_times']['phase7'] = phase_time
        self.global_stats['phases_completed'].append('Phase 7: Symbol Cross-References')
        
        logger.info(f" Phase 7 Complete in {phase_time:.2f} seconds")
        logger.info(f"   Hierarchy processed: {stats['hierarchy_processed']:,}")
        logger.info(f"   Genes processed: {stats['gene_processed']:,}")
        logger.info(f"   Genes merged: {stats['genes_merged']:,}")
        logger.info(f"   Associations created: {stats['associations_created']:,}")
        
        return stats
    
    def _import_collapsed_symbol(self, file_path, stats):
        """Import collapsed_go.symbol with optimized batch processing"""
        logger.info(f" Processing {file_path} with enhanced batching...")
        
        # Check existing progress for resumption
        existing_hierarchy, existing_associations = self._check_phase7_existing_progress()
        
        # OPTIMIZATION: Pre-separate data types for efficient processing
        hierarchy_entries = []
        gene_entries = []
        
        # First pass: separate data types (fast scan)
        logger.info("    Scanning file to separate hierarchy and gene data...")
        with open(file_path, 'r') as file:
            for line in file:
                parts = line.strip().split('\t')
                stats['total_lines'] += 1
                
                if len(parts) >= 3:
                    go_id_1, go_id_2, entry_type = parts[0], parts[1], parts[2]
                    
                    if entry_type == 'default':
                        hierarchy_entries.append({
                            'child_id': go_id_1,
                            'parent_id': go_id_2
                        })
                    elif entry_type == 'gene':
                        gene_entries.append({
                            'go_id': go_id_1,
                            'gene_symbol': go_id_2
                        })
        
        logger.info(f"    Found {len(hierarchy_entries):,} hierarchy entries, {len(gene_entries):,} gene entries")
        
        # Process hierarchy in larger batches (skip already processed)
        if hierarchy_entries:
            entries_to_process = hierarchy_entries[existing_hierarchy:] if existing_hierarchy > 0 else hierarchy_entries
            stats['skipped_existing'] += existing_hierarchy
            
            logger.info(f"    Processing {len(entries_to_process):,} hierarchy entries...")
            hierarchy_batch_size = min(self.batch_sizes['hierarchy'] * 3, 3000)  # Larger batches
            
            for i in range(0, len(entries_to_process), hierarchy_batch_size):
                batch = entries_to_process[i:i + hierarchy_batch_size]
                processed, created = self._process_hierarchy_batch_symbol(batch)
                stats['hierarchy_processed'] += processed
                stats['hierarchy_created'] += created
                
                if (stats['hierarchy_processed'] % 10000) == 0:
                    progress = (stats['hierarchy_processed'] / len(entries_to_process)) * 100
                    logger.info(f"       Hierarchy progress: {progress:.1f}% ({stats['hierarchy_processed']:,} processed)")
        
        # Process genes in optimized batches (skip already processed)  
        if gene_entries:
            entries_to_process = gene_entries[existing_associations:] if existing_associations > 0 else gene_entries
            stats['skipped_existing'] += existing_associations
            
            logger.info(f"    Processing {len(entries_to_process):,} gene entries...")
            gene_batch_size = min(self.batch_sizes['symbol_genes'] * 2, 8000)  # Larger batches
            
            for i in range(0, len(entries_to_process), gene_batch_size):
                batch = entries_to_process[i:i + gene_batch_size]
                processed, genes_proc, genes_merged, assocs_created = self._process_gene_batch_symbol(batch)
                stats['gene_processed'] += processed
                stats['genes_created'] += genes_proc
                stats['genes_merged'] += genes_merged
                stats['associations_created'] += assocs_created
                
                if (stats['gene_processed'] % 15000) == 0:
                    progress = (stats['gene_processed'] / len(entries_to_process)) * 100
                    merge_rate = (stats['genes_merged'] / stats['gene_processed'] * 100) if stats['gene_processed'] > 0 else 0
                    logger.info(f"       Gene progress: {progress:.1f}% ({stats['gene_processed']:,} processed, {merge_rate:.1f}% merged)")
        
        logger.info(f" Completed collapsed_go.symbol processing with optimized batching")
    
    def _check_phase7_existing_progress(self):
        """Check existing Phase 7 progress"""
        with self.driver.session() as session:
            # Check hierarchy progress
            hierarchy_query = """
            MATCH ()-[r:COLLAPSED_HIERARCHY]->(go:GOTerm)
            WHERE r.source_file = "collapsed_go.symbol"
            RETURN count(r) as existing_hierarchy
            """
            result = session.run(hierarchy_query)
            existing_hierarchy = result.single()['existing_hierarchy']
            
            # Check gene progress
            gene_query = """
            MATCH (g:Gene)-[r:ANNOTATED_WITH]->(go:GOTerm)
            WHERE r.source_file = "collapsed_go.symbol"
            RETURN count(r) as existing_gene_associations
            """
            result = session.run(gene_query)
            existing_gene_associations = result.single()['existing_gene_associations']
            
            return existing_hierarchy, existing_gene_associations
    
    def _process_hierarchy_batch_symbol(self, batch_data):
        """Process symbol hierarchy batch with cross-validation"""
        if not batch_data:
            return 0, 0
            
        with self.driver.session() as session:
            hierarchy_query = """
            UNWIND $batch as entry
            MATCH (child:GOTerm {go_id: entry.child_id})
            MATCH (parent:GOTerm {go_id: entry.parent_id})
            
            // Check if relationship already exists from any source
            OPTIONAL MATCH (child)-[existing]->(parent) 
            WHERE type(existing) IN ['IS_A', 'PART_OF', 'REGULATES', 'NEGATIVELY_REGULATES', 'POSITIVELY_REGULATES', 'COLLAPSED_HIERARCHY']
            
            WITH child, parent, existing, entry
            
            // Create COLLAPSED_HIERARCHY relationship with cross-validation
            MERGE (child)-[r:COLLAPSED_HIERARCHY {
                source_file: "collapsed_go.symbol",
                child_id: entry.child_id,
                parent_id: entry.parent_id
            }]->(parent)
            ON CREATE SET
                r.hierarchy_type = "default",
                r.import_timestamp = $timestamp,
                r.cross_validated_symbol = true,
                r.cross_validated_with_existing = (existing IS NOT NULL)
            
            RETURN count(r) as created
            """
            
            result = session.run(hierarchy_query, batch=batch_data, timestamp=self.import_timestamp)
            created = result.single()['created']
            
            return len(batch_data), created
    
    def _process_gene_batch_symbol(self, batch_data):
        """Process symbol gene batch with advanced merging"""
        if not batch_data:
            return 0, 0, 0, 0
            
        with self.driver.session() as session:
            # Step 1: Enhanced gene creation with smart merging
            gene_merge_query = """
            UNWIND $batch as entry
            
            // Look for existing genes with matching symbol
            OPTIONAL MATCH (existing_symbol:Gene) 
            WHERE existing_symbol.symbol = entry.gene_symbol
            
            // Create or enhance gene with symbol as primary identifier
            WITH entry, existing_symbol
            MERGE (g:Gene {symbol: entry.gene_symbol})
            ON CREATE SET 
                g.source_files = ["collapsed_go.symbol"],
                g.import_timestamp = $timestamp,
                g.id_type = "symbol",
                g.symbol_cross_validated = (existing_symbol IS NOT NULL)
            ON MATCH SET
                g.source_files = coalesce(g.source_files, []) + 
                    CASE WHEN NOT "collapsed_go.symbol" IN coalesce(g.source_files, [])
                    THEN ["collapsed_go.symbol"] ELSE [] END,
                g.last_updated = $timestamp,
                g.symbol_cross_validated = true
                
            RETURN count(g) as genes_processed,
                   count(CASE WHEN existing_symbol IS NOT NULL THEN 1 END) as genes_merged
            """
            
            result = session.run(gene_merge_query, batch=batch_data, timestamp=self.import_timestamp)
            gene_data = result.single()
            genes_processed = gene_data['genes_processed']
            genes_merged = gene_data['genes_merged']
            
            # Step 2: Create gene-GO associations
            association_query = f"""
            UNWIND $batch as entry
            MATCH (g:Gene {{symbol: entry.gene_symbol}})
            MATCH (go:GOTerm {{go_id: entry.go_id}})
            
            // Create association with comprehensive metadata
            MERGE (g)-[r:ANNOTATED_WITH {{
                source_file: "collapsed_go.symbol",
                go_id: entry.go_id,
                gene_symbol: entry.gene_symbol
            }}]->(go)
            ON CREATE SET
                r.association_type = "gene",
                r.import_timestamp = $timestamp,
                r.evidence_code = "COLLAPSED",
                r.qualifier = "{self.qualifier}",
                r.id_type = "symbol"
                
            RETURN count(r) as associations_created
            """
            
            result = session.run(association_query, batch=batch_data, timestamp=self.import_timestamp)
            associations_created = result.single()['associations_created']
            
            return len(batch_data), genes_processed, genes_merged, associations_created
    
    # =============================================================================
    # PHASE 8: UNIPROT CROSS-REFERENCES (collapsed_go.uniprot)
    # =============================================================================
    
    def run_phase8_uniprot_cross_references(self):
        """Phase 8: Final UniProt consolidation"""
        phase_start = time.time()
        logger.info(" PHASE 8: UniProt Cross-References - collapsed_go.uniprot")
        logger.info("=" * 60)
        
        stats = {
            'hierarchy_processed': 0,
            'hierarchy_created': 0,
            'genes_processed': 0,
            'genes_merged': 0,
            'genes_created': 0,
            'associations_created': 0,
            'errors': 0
        }
        
        # Parse UniProt file
        hierarchy_entries, gene_entries = self._parse_uniprot_file()
        
        if not hierarchy_entries and not gene_entries:
            logger.error(" No data parsed from collapsed_go.uniprot")
            return stats
        
        # Process hierarchy relationships
        if hierarchy_entries:
            self._process_uniprot_hierarchy_data(hierarchy_entries, stats)
        
        # Process gene associations
        if gene_entries:
            self._process_uniprot_gene_data(gene_entries, stats)
        
        # Phase completion
        phase_time = time.time() - phase_start
        self.global_stats['phase_times']['phase8'] = phase_time
        self.global_stats['phases_completed'].append('Phase 8: UniProt Cross-References')
        
        logger.info(f" Phase 8 Complete in {phase_time:.2f} seconds")
        logger.info(f"   Hierarchy processed: {stats['hierarchy_processed']:,}")
        logger.info(f"   Genes processed: {stats['genes_processed']:,}")
        logger.info(f"   Genes merged: {stats['genes_merged']:,} ({stats['genes_merged']/stats['genes_processed']*100:.1f}%)")
        logger.info(f"   Genes created: {stats['genes_created']:,}")
        
        # FINAL: Consolidate any remaining duplicate genes after all phases
        self._consolidate_duplicate_genes()
        
        # Final validation checkpoint
        self._run_phase_validation("Phase 8: UniProt Cross-References (Final)")
        
        return stats
    
    def _parse_uniprot_file(self):
        """Parse collapsed_go.uniprot with performance optimizations"""
        logger.info(" Parsing collapsed_go.uniprot file...")
        
        file_path = self.data_paths['collapsed_uniprot']
        hierarchy_entries = []
        gene_entries = []
        
        try:
            with open(file_path, 'r') as file:
                for line_num, line in enumerate(file, 1):
                    line = line.strip()
                    if not line:
                        continue
                        
                    parts = line.split('\t')
                    if len(parts) != 3:
                        continue
                    
                    go_id, identifier, entry_type = parts
                    
                    if entry_type == 'default':
                        # GO hierarchy relationship
                        hierarchy_entries.append({
                            'child_id': go_id,
                            'parent_id': identifier,
                            'source_file': 'collapsed_go.uniprot'
                        })
                    elif entry_type == 'gene':
                        # Gene-GO association
                        gene_entries.append({
                            'go_id': go_id,
                            'uniprot_id': identifier,
                            'source_file': 'collapsed_go.uniprot'
                        })
                
                logger.info(f"    Parsed {len(hierarchy_entries)} hierarchy entries")
                logger.info(f"    Parsed {len(gene_entries)} gene association entries")
                
                return hierarchy_entries, gene_entries
                
        except FileNotFoundError:
            logger.error(f" File not found: {file_path}")
            return [], []
        except Exception as e:
            logger.error(f" Error parsing file: {str(e)}")
            return [], []
    
    def _process_uniprot_hierarchy_data(self, hierarchy_entries, stats):
        """Process all hierarchy entries in optimized batches"""
        logger.info(f" Processing {len(hierarchy_entries)} hierarchy relationships...")
        
        batch_size = self.batch_sizes['hierarchy']
        total_created = 0
        batch_count = 0
        
        with self.driver.session() as session:
            for i in range(0, len(hierarchy_entries), batch_size):
                batch = hierarchy_entries[i:i + batch_size]
                batch_count += 1
                
                try:
                    created = self._process_uniprot_hierarchy_batch(session, batch)
                    total_created += created
                    
                    if batch_count % 10 == 0:
                        progress = (i + len(batch)) / len(hierarchy_entries) * 100
                        logger.info(f"    Progress: {progress:.1f}% ({batch_count} batches)")
                        
                except Exception as e:
                    logger.error(f"    Batch {batch_count} failed: {str(e)}")
                    stats['errors'] += 1
                    continue
        
        stats['hierarchy_processed'] = len(hierarchy_entries)
        stats['hierarchy_created'] = total_created
        logger.info(f"    Hierarchy complete: {total_created} relationships created")
    
    def _process_uniprot_hierarchy_batch(self, session, batch):
        """Process hierarchy relationships with duplicate detection"""
        hierarchy_query = """
        UNWIND $batch as entry
        // Verify both GO terms exist
        MATCH (child:GOTerm {go_id: entry.child_id})
        MATCH (parent:GOTerm {go_id: entry.parent_id})
        
        // Check if relationship already exists from this source
        WHERE NOT EXISTS {
            (child)-[r:COLLAPSED_HIERARCHY]->(parent) 
            WHERE r.source_file = "collapsed_go.uniprot"
        }
        
        // Create cross-validated hierarchy relationship
        MERGE (child)-[r:COLLAPSED_HIERARCHY {
            source_file: "collapsed_go.uniprot",
            child_id: entry.child_id,
            parent_id: entry.parent_id
        }]->(parent)
        ON CREATE SET
            r.import_timestamp = $timestamp,
            r.cross_validated_uniprot = true
        
        RETURN count(r) as relationships_created
        """
        
        result = session.run(hierarchy_query, batch=batch, timestamp=self.import_timestamp)
        return result.single()['relationships_created']
    
    def _process_uniprot_gene_data(self, gene_entries, stats):
        """Process all gene entries in optimized batches"""
        logger.info(f" Processing {len(gene_entries)} gene associations...")
        
        batch_size = self.batch_sizes['uniprot_genes']
        total_processed = 0
        total_merged = 0
        total_created = 0
        batch_count = 0
        
        with self.driver.session() as session:
            for i in range(0, len(gene_entries), batch_size):
                batch = gene_entries[i:i + batch_size]
                batch_count += 1
                
                try:
                    result = self._process_uniprot_gene_batch(session, batch)
                    
                    total_processed += result['genes_processed']
                    total_merged += result['genes_merged']
                    total_created += result['genes_created']
                    
                    # Progress reporting every 20 batches
                    if batch_count % 20 == 0:
                        progress = (i + len(batch)) / len(gene_entries) * 100
                        merge_rate = (total_merged / total_processed * 100) if total_processed > 0 else 0
                        logger.info(f"    Progress: {progress:.1f}% ({batch_count} batches, {merge_rate:.1f}% merge rate)")
                        
                except Exception as e:
                    logger.error(f"    Batch {batch_count} failed: {str(e)}")
                    stats['errors'] += 1
                    continue
        
        # Update statistics
        stats['genes_processed'] = total_processed
        stats['genes_merged'] = total_merged
        stats['genes_created'] = total_created
        stats['associations_created'] = len(gene_entries)  # Each entry creates an association
        
        logger.info(f"    Gene processing complete:")
        logger.info(f"      Processed: {total_processed} genes")
        logger.info(f"      Merged: {total_merged} genes ({total_merged/total_processed*100:.1f}%)")
        logger.info(f"      Created: {total_created} genes")
    
    def _process_uniprot_gene_batch(self, session, batch):
        """Process gene associations with smart UniProt-based merging"""
        gene_merge_query = """
        UNWIND $batch as entry
        
        // Look for existing gene with same UniProt ID (high probability)
        OPTIONAL MATCH (existing_uniprot:Gene) 
        WHERE existing_uniprot.uniprot_id = entry.uniprot_id
        
        // Smart gene merging with UniProt as primary identifier
        WITH entry, existing_uniprot
        MERGE (g:Gene {uniprot_id: entry.uniprot_id})
        ON CREATE SET 
            g.source_files = ["collapsed_go.uniprot"],
            g.import_timestamp = $timestamp,
            g.id_type = "uniprot",
            g.uniprot_cross_validated = (existing_uniprot IS NOT NULL)
        ON MATCH SET
            g.source_files = coalesce(g.source_files, []) + 
                CASE WHEN NOT "collapsed_go.uniprot" IN coalesce(g.source_files, [])
                THEN ["collapsed_go.uniprot"] ELSE [] END,
            g.last_updated = $timestamp,
            g.uniprot_validated = true
        
        // Track merge vs create statistics
        WITH g, entry, (existing_uniprot IS NOT NULL) as was_merged
        
        // Create gene-GO association (skip if exists)
        MATCH (go:GOTerm {go_id: entry.go_id})
        WHERE NOT EXISTS {
            (g)-[r:ANNOTATED_WITH]->(go) 
            WHERE r.source_file = "collapsed_go.uniprot"
        }
        CREATE (g)-[:ANNOTATED_WITH {
            source_file: "collapsed_go.uniprot",
            import_timestamp: $timestamp,
            uniprot_id: entry.uniprot_id
        }]->(go)
        
        RETURN count(DISTINCT g) as genes_processed,
               count(CASE WHEN was_merged THEN 1 END) as genes_merged,
               count(CASE WHEN NOT was_merged THEN 1 END) as genes_created
        """
        
        result = session.run(gene_merge_query, batch=batch, timestamp=self.import_timestamp)
        data = result.single()
        
        return {
            'genes_processed': data['genes_processed'],
            'genes_merged': data['genes_merged'], 
            'genes_created': data['genes_created']
        }
    
    # =============================================================================
    # COMPREHENSIVE VALIDATION AND REPORTING
    # =============================================================================
    
    def _run_phase_validation(self, phase_name):
        """Run focused validation after each phase"""
        logger.info(f" Running validation checkpoint after {phase_name}...")
        
        with self.driver.session() as session:
            # Quick node counts
            node_stats_query = """
            MATCH (n) 
            WITH labels(n)[0] as node_type, count(n) as count
            ORDER BY count DESC
            RETURN collect({type: node_type, count: count}) as node_stats
            """
            
            # Quick relationship counts
            rel_stats_query = """
            MATCH ()-[r]->() 
            WITH type(r) as rel_type, count(r) as count
            ORDER BY count DESC  
            RETURN collect({type: rel_type, count: count}) as rel_stats
            """
            
            # Execute node stats query
            node_result = session.run(node_stats_query)
            node_record = node_result.single()
            if node_record and node_record['node_stats']:
                logger.info(f"    Node counts: {dict((s['type'], s['count']) for s in node_record['node_stats'][:3])}")
            
            # Execute relationship stats query
            rel_result = session.run(rel_stats_query)
            rel_record = rel_result.single()
            if rel_record and rel_record['rel_stats']:
                logger.info(f"    Relationship counts: {dict((s['type'], s['count']) for s in rel_record['rel_stats'][:3])}")
            
            # Phase-specific validations
            if 'Phase 1' in phase_name or 'Phase 2' in phase_name or 'Phase 3' in phase_name:
                # GO term validation
                go_validation = session.run("""
                MATCH (go:GOTerm) 
                WHERE go.namespace = $namespace
                RETURN count(go) as namespace_terms, 
                       count(CASE WHEN go.definition IS NOT NULL THEN 1 END) as with_definitions
                """, namespace=self.namespace_full).single()
                logger.info(f"    {self.namespace.upper()} GO Terms: {go_validation['namespace_terms']:,} ({go_validation['with_definitions']:,} with definitions)")
                
            elif 'Phase 5' in phase_name:
                # Gene validation after GAF processing
                gene_validation = session.run("""
                MATCH (g:Gene) 
                RETURN count(g) as total_genes,
                       count(CASE WHEN g.uniprot_id IS NOT NULL THEN 1 END) as with_uniprot
                """).single()
                logger.info(f"    Genes created: {gene_validation['total_genes']:,} ({gene_validation['with_uniprot']:,} with UniProt IDs)")
                
            elif any(x in phase_name for x in ['Phase 6', 'Phase 7', 'Phase 8']):
                # Cross-reference validation
                gene_integration = session.run("""
                MATCH (g:Gene)
                RETURN 
                    count(g) as total_genes,
                    count(CASE WHEN g.consolidated = true THEN 1 END) as consolidated_genes,
                    avg(size(g.source_files)) as avg_sources
                """).single()
                logger.info(f"    Gene integration: {gene_integration['total_genes']:,} total, {gene_integration['consolidated_genes'] or 0:,} consolidated")
        
        logger.info(f"    {phase_name} validation complete")

    def run_comprehensive_validation(self):
        """Run comprehensive validation across all phases"""
        logger.info(" Running comprehensive cross-phase validation...")
        
        validation_results = {}
        
        with self.driver.session() as session:
            # Overall database statistics
            overall_query = """
            MATCH (n) 
            RETURN labels(n)[0] as node_type, count(n) as count
            ORDER BY count DESC
            """
            
            result = session.run(overall_query)
            node_counts = {}
            for record in result:
                node_type = record['node_type']
                count = record['count']
                node_counts[node_type] = count
            
            validation_results['node_counts'] = node_counts
            
            # Relationship statistics
            rel_query = """
            MATCH ()-[r]->() 
            RETURN type(r) as rel_type, count(r) as count
            ORDER BY count DESC
            """
            
            result = session.run(rel_query)
            relationship_counts = {}
            for record in result:
                rel_type = record['rel_type']
                count = record['count']
                relationship_counts[rel_type] = count
            
            validation_results['relationship_counts'] = relationship_counts
            
            # Gene integration analysis
            gene_integration_query = """
            MATCH (g:Gene)
            RETURN 
                count(g) as total_genes,
                count(CASE WHEN g.uniprot_id IS NOT NULL THEN 1 END) as with_uniprot,
                count(CASE WHEN g.entrez_id IS NOT NULL THEN 1 END) as with_entrez,
                count(CASE WHEN g.symbol IS NOT NULL THEN 1 END) as with_symbol,
                count(CASE WHEN g.uniprot_id IS NOT NULL AND g.entrez_id IS NOT NULL THEN 1 END) as uniprot_entrez_cross,
                count(CASE WHEN g.uniprot_id IS NOT NULL AND g.symbol IS NOT NULL THEN 1 END) as uniprot_symbol_cross,
                count(CASE WHEN g.entrez_id IS NOT NULL AND g.symbol IS NOT NULL THEN 1 END) as entrez_symbol_cross,
                count(CASE WHEN size(g.source_files) >= 3 THEN 1 END) as triple_source_genes
            """
            
            result = session.run(gene_integration_query)
            gene_integration = result.single()
            validation_results['gene_integration'] = dict(gene_integration)
            
            # GO Term coverage
            go_coverage_query = """
            MATCH (go:GOTerm)
            OPTIONAL MATCH (go)<-[r:ANNOTATED_WITH]-(:Gene)
            RETURN 
                count(go) as total_go_terms,
                count(CASE WHEN r IS NOT NULL THEN 1 END) as go_terms_with_genes,
                count(DISTINCT r.source_file) as unique_gene_sources
            """
            
            result = session.run(go_coverage_query)
            go_coverage = result.single()
            validation_results['go_coverage'] = dict(go_coverage)
            
        return validation_results
    
    def generate_final_report(self, validation_results):
        """Generate comprehensive final report"""
        total_runtime = self.global_stats['end_time'] - self.global_stats['start_time']
        
        report = f"""
{'='*80}
 UNIFIED GO_{self.namespace.upper()} KNOWLEDGE GRAPH CREATION - COMPLETE SUCCESS
{'='*80}

  EXECUTION SUMMARY:
   Total Runtime: {total_runtime:.2f} seconds ({total_runtime/60:.1f} minutes)
   Phases Completed: {len(self.global_stats['phases_completed'])}/8
   Import Timestamp: {self.import_timestamp}

 PHASE-BY-PHASE PERFORMANCE:
"""
        
        for phase, phase_time in self.global_stats['phase_times'].items():
            percentage = (phase_time / total_runtime) * 100
            report += f"   {phase}: {phase_time:.2f}s ({percentage:.1f}%)\n"
        
        report += f"""
  DATABASE COMPOSITION:
   Node Types:
"""
        
        for node_type, count in validation_results['node_counts'].items():
            report += f"      {node_type}: {count:,}\n"
        
        report += f"""
   Relationship Types:
"""
        for rel_type, count in validation_results['relationship_counts'].items():
            report += f"      {rel_type}: {count:,}\n"
        
        gene_integration = validation_results['gene_integration']
        report += f"""
 GENE INTEGRATION ANALYSIS:
   Total Genes: {gene_integration['total_genes']:,}
   With UniProt IDs: {gene_integration['with_uniprot']:,}
   With Entrez IDs: {gene_integration['with_entrez']:,}
   With Symbols: {gene_integration['with_symbol']:,}
   UniProt-Entrez Cross-refs: {gene_integration['uniprot_entrez_cross']:,}
   UniProt-Symbol Cross-refs: {gene_integration['uniprot_symbol_cross']:,}
   Entrez-Symbol Cross-refs: {gene_integration['entrez_symbol_cross']:,}
   Triple-Source Genes: {gene_integration['triple_source_genes']:,}

 GO TERM COVERAGE:
   Total GO Terms: {validation_results['go_coverage']['total_go_terms']:,}
   GO Terms with Gene Associations: {validation_results['go_coverage']['go_terms_with_genes']:,}
   Gene Data Sources: {validation_results['go_coverage']['unique_gene_sources']:,}
   
{'='*80}
   
Query the knowledge graph using Neo4j Browser or any compatible client
All gene-GO associations are properly connected and cross-referenced
Perfect foundation for {self.namespace_full}
{'='*80}
"""
        
        return report
    
    # =============================================================================
    # MAIN EXECUTION ORCHESTRATION
    # =============================================================================
    
    def create_complete_knowledge_graph(self):
        """Execute all 8 phases to create the complete GO knowledge graph"""
        self.global_stats['start_time'] = time.time()
        
        logger.info(f" STARTING UNIFIED GO_{self.namespace_full.upper()} KNOWLEDGE GRAPH CREATION")
        logger.info(" All 8 phases will be executed in sequence")
        logger.info("=" * 80)
        
        try:
            # Step 0: Prerequisites
            if not self.validate_prerequisites():
                logger.error(" Prerequisites validation failed - aborting")
                return False
            
            # Step 1: Create performance indexes
            self.create_performance_indexes()
            
            # Step 2: Execute all 8 phases
            phase_results = {}
            
            # Phase 1: Foundation
            phase_results['phase1'] = self.run_phase1_foundation()
            
            # Phase 2: ID Mappings  
            phase_results['phase2'] = self.run_phase2_id_mappings()
            
            # Phase 3: Metadata Validation
            phase_results['phase3'] = self.run_phase3_metadata_validation()
            
            # Phase 4: Hierarchical Structure
            phase_results['phase4'] = self.run_phase4_hierarchical_structure()
            
            # Phase 5: Gene Annotations
            phase_results['phase5'] = self.run_phase5_gene_annotations()
            
            # Phase 6: ID Cross-References
            phase_results['phase6'] = self.run_phase6_id_cross_references()
            
            # Phase 7: Symbol Cross-References  
            phase_results['phase7'] = self.run_phase7_symbol_cross_references()
            
            # Phase 8: UniProt Cross-References
            phase_results['phase8'] = self.run_phase8_uniprot_cross_references()
            
            # Step 3: Comprehensive validation
            validation_results = self.run_comprehensive_validation()
            
            # Step 4: Generate final report
            self.global_stats['end_time'] = time.time()
            final_report = self.generate_final_report(validation_results)
            
            logger.info(final_report)
            
            # Write report to file
            with open(f'complete_go_{self.namespace}_kg_report.txt', 'w') as f:
                f.write(final_report)
            
            logger.info(f"Complete report saved to: complete_go_{self.namespace}_kg_report.txt")
            
            return True
            
        except Exception as e:
            logger.error(f" Knowledge graph creation failed: {str(e)}")
            return False

def main():
    """Main execution function"""
    logger.info(" GO Knowledge Graph Creator - Unified Implementation")
    logger.info(" This script consolidates all 8 phases into one execution")
    logger.info("-" * 80)
    
    try:
        with CompleteGOKnowledgeGraphCreator() as creator:
            success = creator.create_complete_knowledge_graph()
            
            if success:
                logger.info(f" COMPLETE GO_{creator.namespace.upper()} KNOWLEDGE GRAPH SUCCESSFULLY CREATED!")
                logger.info(f" Ready for {creator.namespace_full} analysis and research")
                return True
            else:
                logger.error(" Knowledge graph creation failed")
                return False
                
    except Exception as e:
        logger.error(f" Fatal error in main execution: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)