#!/usr/bin/env python3
"""
Talisman Geneset Parser

Parses all 77 talisman geneset files from various formats (YAML, JSON) into
standardized ParsedGeneset objects for integration into Neo4j knowledge graph.

Handles 3 data formats:
1. YAML with gene_symbols list
2. YAML with gene_ids list (HGNC format)  
3. JSON with rich metadata (MSigDB format)

"""

import yaml
import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ParsedGeneset:
    """Standardized representation of a parsed geneset."""
    geneset_id: str
    name: str
    gene_symbols: List[str] = field(default_factory=list)
    gene_ids: List[str] = field(default_factory=list) 
    description: Optional[str] = None
    taxon: str = "human"
    source_file: str = ""
    source_collection: str = ""  # HALLMARK, BICLUSTER, CUSTOM
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Post-initialization validation and cleanup."""
        # Ensure gene symbols are unique and clean
        self.gene_symbols = list(set(self.gene_symbols)) if self.gene_symbols else []
        self.gene_ids = list(set(self.gene_ids)) if self.gene_ids else []
        
        # Clean up gene symbols (remove whitespace, empty strings)
        self.gene_symbols = [s.strip() for s in self.gene_symbols if s and s.strip()]
        self.gene_ids = [s.strip() for s in self.gene_ids if s and s.strip()]


class TalismanGenesetParser:
    """Parser for all 77 talisman geneset files with format auto-detection."""
    
    def __init__(self, data_directory: str):
        """
        Initialize parser with talisman data directory.
        
        Args:
            data_directory: Path to talisman-paper/genesets/human/
        """
        self.data_dir = Path(data_directory)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Talisman data directory not found: {data_directory}")
        
        self.stats = {
            "files_processed": 0,
            "yaml_files": 0, 
            "json_files": 0,
            "parse_errors": 0,
            "total_genes_parsed": 0,
            "collection_counts": {"HALLMARK": 0, "BICLUSTER": 0, "CUSTOM": 0}
        }
        
    def parse_all_genesets(self) -> List[ParsedGeneset]:
        """
        Parse all geneset files in the directory.
        
        Handles duplicate genesets by preferring JSON over YAML for richer metadata.
        
        Returns:
            List of ParsedGeneset objects, one per geneset
        """
        logger.info(f"Starting to parse all genesets from {self.data_dir}")
        
        genesets = []
        geneset_dict = {}  # geneset_id -> ParsedGeneset (for deduplication)
        
        # Process YAML files first
        yaml_files = list(self.data_dir.glob("*.yaml"))
        logger.info(f"Found {len(yaml_files)} YAML files")
        
        for file_path in yaml_files:
            try:
                geneset = self._parse_yaml_file(file_path)
                geneset_dict[geneset.geneset_id] = geneset
                self.stats["yaml_files"] += 1
                self.stats["files_processed"] += 1
                self.stats["total_genes_parsed"] += len(geneset.gene_symbols) + len(geneset.gene_ids)
                self.stats["collection_counts"][geneset.source_collection] += 1
                
                logger.debug(f"Parsed YAML: {file_path.name} -> {geneset.geneset_id} ({len(geneset.gene_symbols)} genes)")
                
            except Exception as e:
                logger.error(f"Error parsing YAML file {file_path}: {e}")
                self.stats["parse_errors"] += 1
        
        # Process JSON files (will override YAML if duplicate)
        json_files = list(self.data_dir.glob("*.json"))
        logger.info(f"Found {len(json_files)} JSON files")
        
        duplicates_resolved = 0
        for file_path in json_files:
            try:
                parsed_genesets = self._parse_json_file(file_path)
                
                for geneset in parsed_genesets:
                    if geneset.geneset_id in geneset_dict:
                        logger.info(f"Resolving duplicate: preferring JSON over YAML for {geneset.geneset_id}")
                        duplicates_resolved += 1
                    
                    geneset_dict[geneset.geneset_id] = geneset  # JSON overwrites YAML
                    self.stats["total_genes_parsed"] += len(geneset.gene_symbols) + len(geneset.gene_ids)
                    self.stats["collection_counts"][geneset.source_collection] += 1
                
                self.stats["json_files"] += 1
                self.stats["files_processed"] += 1
                
                logger.debug(f"Parsed JSON: {file_path.name} -> {len(parsed_genesets)} genesets")
                
            except Exception as e:
                logger.error(f"Error parsing JSON file {file_path}: {e}")
                self.stats["parse_errors"] += 1
        
        # Convert to list
        genesets = list(geneset_dict.values())
        
        logger.info(f"Parsing complete: {len(genesets)} unique genesets from {self.stats['files_processed']} files")
        logger.info(f"Resolved {duplicates_resolved} duplicate genesets (preferred JSON over YAML)")
        logger.info(f"Collection breakdown: {self.stats['collection_counts']}")
        
        return genesets
    
    def _parse_yaml_file(self, file_path: Path) -> ParsedGeneset:
        """
        Parse a single YAML geneset file.
        
        Args:
            file_path: Path to YAML file
            
        Returns:
            ParsedGeneset object
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        if not isinstance(data, dict):
            raise ValueError(f"Expected dict in YAML file {file_path}, got {type(data)}")
        
        # Extract basic fields
        name = data.get('name', file_path.stem)
        gene_symbols = data.get('gene_symbols', [])
        gene_ids = data.get('gene_ids', [])
        
        # Handle both 'description' and 'descriptions' fields
        description = data.get('description')
        if description is None:
            description = data.get('descriptions')
        
        taxon = data.get('taxon', 'human')
        
        # Ensure gene_symbols and gene_ids are lists
        if not isinstance(gene_symbols, list):
            gene_symbols = []
        if not isinstance(gene_ids, list):
            gene_ids = []
        
        # Generate standardized geneset ID
        geneset_id = self._generate_geneset_id(name)
        
        # Classify collection type
        source_collection = self._classify_collection(file_path.name)
        
        return ParsedGeneset(
            geneset_id=geneset_id,
            name=name,
            gene_symbols=gene_symbols,
            gene_ids=gene_ids,
            description=description,
            taxon=taxon,
            source_file=file_path.name,
            source_collection=source_collection,
            metadata=data
        )
    
    def _parse_json_file(self, file_path: Path) -> List[ParsedGeneset]:
        """
        Parse a single JSON geneset file.
        
        JSON files can contain multiple genesets in MSigDB format.
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            List of ParsedGeneset objects (usually 1 per file)
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, dict):
            raise ValueError(f"Expected dict in JSON file {file_path}, got {type(data)}")
        
        genesets = []
        
        # JSON files typically have structure: {"GENESET_NAME": {"systematicName": ..., "geneSymbols": [...]}}
        for geneset_name, geneset_data in data.items():
            if not isinstance(geneset_data, dict):
                logger.warning(f"Skipping invalid geneset data in {file_path}: {geneset_name}")
                continue
            
            # Extract gene symbols
            gene_symbols = geneset_data.get('geneSymbols', [])
            if not isinstance(gene_symbols, list):
                gene_symbols = []
            
            # Extract metadata
            systematic_name = geneset_data.get('systematicName')
            pmid = geneset_data.get('pmid')
            msigdb_url = geneset_data.get('msigdbURL')
            collection = geneset_data.get('collection', 'H')  # H = Hallmark
            
            # Generate description from metadata
            description = None
            if pmid:
                description = f"MSigDB geneset (PMID: {pmid})"
            elif systematic_name:
                description = f"MSigDB geneset ({systematic_name})"
            
            # Classify collection type
            source_collection = self._classify_collection(file_path.name)
            
            # Add MSigDB metadata
            metadata = dict(geneset_data)
            if msigdb_url:
                metadata['msigdb_url'] = msigdb_url
            if pmid:
                metadata['pmid'] = pmid
            if systematic_name:
                metadata['systematic_name'] = systematic_name
            
            geneset = ParsedGeneset(
                geneset_id=geneset_name,  # JSON files already have standardized IDs
                name=geneset_name,
                gene_symbols=gene_symbols,
                gene_ids=[],  # JSON format typically doesn't have gene_ids
                description=description,
                taxon='human',
                source_file=file_path.name,
                source_collection=source_collection,
                metadata=metadata
            )
            
            genesets.append(geneset)
        
        return genesets
    
    def _generate_geneset_id(self, name: str) -> str:
        """
        Generate consistent geneset ID from name.
        
        Args:
            name: Original geneset name
            
        Returns:
            Standardized geneset ID
        """
        if not name:
            return "UNKNOWN_GENESET"
        
        # Convert to uppercase and replace spaces/hyphens with underscores
        geneset_id = re.sub(r'[^\w]', '_', name.upper())
        
        # Remove multiple consecutive underscores
        geneset_id = re.sub(r'_+', '_', geneset_id)
        
        # Remove leading/trailing underscores
        geneset_id = geneset_id.strip('_')
        
        return geneset_id
    
    def _classify_collection(self, filename: str) -> str:
        """
        Classify geneset into collection type based on filename.
        
        Args:
            filename: Name of the geneset file
            
        Returns:
            Collection type: HALLMARK, BICLUSTER, or CUSTOM
        """
        filename_lower = filename.lower()
        
        if filename_lower.startswith('hallmark_'):
            return 'HALLMARK'
        elif filename_lower.startswith('bicluster_'):
            return 'BICLUSTER'
        else:
            return 'CUSTOM'
    
    def get_parsing_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive parsing statistics.
        
        Returns:
            Dictionary with parsing stats
        """
        return dict(self.stats)
    
    def validate_parsed_genesets(self, genesets: List[ParsedGeneset]) -> Dict[str, Any]:
        """
        Validate parsed genesets for common issues.
        
        Args:
            genesets: List of parsed genesets to validate
            
        Returns:
            Validation report
        """
        validation = {
            "total_genesets": len(genesets),
            "genesets_with_genes": 0,
            "empty_genesets": [],
            "duplicate_ids": [],
            "collection_breakdown": {"HALLMARK": 0, "BICLUSTER": 0, "CUSTOM": 0},
            "gene_count_distribution": {"min": float('inf'), "max": 0, "avg": 0},
            "issues": []
        }
        
        geneset_ids_seen = set()
        total_gene_count = 0
        
        for geneset in genesets:
            # Count genes
            gene_count = len(geneset.gene_symbols) + len(geneset.gene_ids)
            total_gene_count += gene_count
            
            if gene_count > 0:
                validation["genesets_with_genes"] += 1
                validation["gene_count_distribution"]["min"] = min(validation["gene_count_distribution"]["min"], gene_count)
                validation["gene_count_distribution"]["max"] = max(validation["gene_count_distribution"]["max"], gene_count)
            else:
                validation["empty_genesets"].append(geneset.geneset_id)
            
            # Check for duplicates
            if geneset.geneset_id in geneset_ids_seen:
                validation["duplicate_ids"].append(geneset.geneset_id)
            else:
                geneset_ids_seen.add(geneset.geneset_id)
            
            # Collection counts
            validation["collection_breakdown"][geneset.source_collection] += 1
            
            # Check for potential issues
            if not geneset.name or not geneset.name.strip():
                validation["issues"].append(f"Empty name: {geneset.geneset_id}")
            
            if geneset.taxon != 'human':
                validation["issues"].append(f"Non-human taxon: {geneset.geneset_id} ({geneset.taxon})")
        
        # Calculate average
        if validation["genesets_with_genes"] > 0:
            validation["gene_count_distribution"]["avg"] = total_gene_count / validation["genesets_with_genes"]
        else:
            validation["gene_count_distribution"]["min"] = 0
        
        return validation


if __name__ == "__main__":
    # Example usage
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # Parse all genesets
    parser = TalismanGenesetParser("/app/data/talisman-paper/genesets/human")
    genesets = parser.parse_all_genesets()
    
    print(f"\nParsed {len(genesets)} genesets")
    print(f"Parsing stats: {parser.get_parsing_statistics()}")
    
    # Validate
    validation = parser.validate_parsed_genesets(genesets)
    print(f"\nValidation report: {validation}")
    
    # Show sample genesets
    print(f"\nSample genesets:")
    for i, geneset in enumerate(genesets[:3]):
        print(f"{i+1}. {geneset.geneset_id}")
        print(f"   Name: {geneset.name}")
        print(f"   Collection: {geneset.source_collection}")
        print(f"   Genes: {len(geneset.gene_symbols)} symbols, {len(geneset.gene_ids)} IDs")
        print(f"   File: {geneset.source_file}")
        if geneset.description:
            print(f"   Description: {geneset.description}")
        print()