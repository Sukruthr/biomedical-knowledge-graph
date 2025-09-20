#!/usr/bin/env python3
"""
Gene Symbol Validator for Talisman Integration

Validates gene symbols from talisman genesets against the existing Neo4j
knowledge graph containing 43,873 genes. Provides gene resolution, ID mapping,
and validation statistics for quality control.

Based on TALISMAN_GENESET_INTEGRATION_PLAN.md Phase 2.1

"""

import logging
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass
from collections import defaultdict

from talisman_geneset_parser import ParsedGeneset

logger = logging.getLogger(__name__)


@dataclass
class GeneValidationResult:
    """Result of validating genes in a geneset."""
    geneset_id: str
    valid_genes: List[str]
    invalid_genes: List[str]
    gene_ids_resolved: List[str]
    total_input_genes: int
    resolution_rate: float
    kg_gene_details: Dict[str, Dict]  # symbol -> {name, uniprot_id, etc}


class GeneSymbolValidator:
    """Validates talisman gene symbols against existing knowledge graph."""
    
    def __init__(self, neo4j_driver):
        """
        Initialize validator with Neo4j driver.
        
        Args:
            neo4j_driver: Active Neo4j GraphDatabase driver instance
        """
        self.connection = neo4j_driver
        self.gene_cache = {}
        self.cache_loaded = False
        self.stats = {
            "genes_in_kg": 0,
            "cache_load_time": 0,
            "validation_calls": 0
        }
    
    def _load_gene_cache(self) -> None:
        """
        Load all genes from KG for fast lookup.
        
        Loads all 43,873 genes with their properties into memory cache
        for efficient validation of talisman gene symbols.
        """
        if self.cache_loaded:
            return
            
        logger.info("Loading gene cache from Neo4j knowledge graph...")
        
        import time
        start_time = time.time()
        
        # Load all genes with their properties
        query = """
        MATCH (g:Gene)
        RETURN g.symbol as symbol, 
               g.name as name, 
               g.uniprot_id as uniprot_id,
               g.entrez_id as entrez_id,
               g as gene_node
        """
        
        with self.connection.session() as session:
            results = session.run(query)
            
            for row in results:
                symbol = row.get('symbol')
                if symbol:
                    self.gene_cache[symbol] = {
                        'symbol': symbol,
                        'name': row.get('name'),
                        'uniprot_id': row.get('uniprot_id'), 
                    'entrez_id': row.get('entrez_id'),
                    'properties': dict(row.get('gene_node', {}))
                }
        
        self.stats["genes_in_kg"] = len(self.gene_cache)
        self.stats["cache_load_time"] = time.time() - start_time
        self.cache_loaded = True
        
        logger.info(f"Gene cache loaded: {self.stats['genes_in_kg']} genes in {self.stats['cache_load_time']:.2f}s")
    
    def validate_geneset_genes(self, geneset: ParsedGeneset) -> GeneValidationResult:
        """
        Validate all genes in a geneset against the KG.
        
        Args:
            geneset: ParsedGeneset to validate
            
        Returns:
            GeneValidationResult with detailed validation results
        """
        self._load_gene_cache()
        self.stats["validation_calls"] += 1
        
        valid_genes = []
        invalid_genes = []
        gene_ids_resolved = []
        kg_gene_details = {}
        
        # Validate gene symbols
        for symbol in geneset.gene_symbols:
            if symbol in self.gene_cache:
                valid_genes.append(symbol)
                kg_gene_details[symbol] = self.gene_cache[symbol]
            else:
                invalid_genes.append(symbol)
        
        # Attempt to resolve gene IDs (HGNC format)
        for gene_id in geneset.gene_ids:
            resolved_symbol = self._resolve_gene_id(gene_id)
            if resolved_symbol and resolved_symbol in self.gene_cache:
                gene_ids_resolved.append(resolved_symbol)
                kg_gene_details[resolved_symbol] = self.gene_cache[resolved_symbol]
        
        # Calculate statistics
        total_input_genes = len(geneset.gene_symbols) + len(geneset.gene_ids)
        total_resolved = len(valid_genes) + len(gene_ids_resolved)
        resolution_rate = total_resolved / total_input_genes if total_input_genes > 0 else 0.0
        
        return GeneValidationResult(
            geneset_id=geneset.geneset_id,
            valid_genes=valid_genes,
            invalid_genes=invalid_genes,
            gene_ids_resolved=gene_ids_resolved,
            total_input_genes=total_input_genes,
            resolution_rate=resolution_rate,
            kg_gene_details=kg_gene_details
        )
    
    def validate_all_genesets(self, genesets: List[ParsedGeneset]) -> Dict[str, GeneValidationResult]:
        """
        Validate all genesets in batch.
        
        Args:
            genesets: List of ParsedGeneset objects to validate
            
        Returns:
            Dictionary mapping geneset_id to GeneValidationResult
        """
        logger.info(f"Validating {len(genesets)} genesets against knowledge graph...")
        
        results = {}
        
        for geneset in genesets:
            validation_result = self.validate_geneset_genes(geneset)
            results[geneset.geneset_id] = validation_result
            
            if len(results) % 10 == 0:
                logger.debug(f"Validated {len(results)}/{len(genesets)} genesets")
        
        logger.info(f"Validation complete for {len(results)} genesets")
        return results
    
    def _resolve_gene_id(self, gene_id: str) -> Optional[str]:
        """
        Resolve HGNC gene ID to gene symbol using HGNC REST API.
        
        Args:
            gene_id: Gene ID in HGNC format (e.g., "HGNC:11998")
            
        Returns:
            Gene symbol if resolved, None otherwise
        """
        if not gene_id.startswith('HGNC:'):
            logger.warning(f"Unsupported gene ID format: {gene_id}")
            return None
        
        try:
            import requests
            import time
            
            # Extract HGNC ID number
            hgnc_num = gene_id.split(':')[1]
            
            # HGNC REST API endpoint
            url = f"https://rest.genenames.org/fetch/hgnc_id/{hgnc_num}"
            headers = {'Accept': 'application/json'}
            
            # Add small delay to be respectful to the API
            time.sleep(0.1)
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract gene symbol from response
                if 'response' in data and 'docs' in data['response']:
                    docs = data['response']['docs']
                    if docs and len(docs) > 0:
                        gene_doc = docs[0]
                        symbol = gene_doc.get('symbol')
                        if symbol:
                            logger.debug(f"Resolved {gene_id} -> {symbol}")
                            return symbol
            
            logger.debug(f"Could not resolve HGNC ID: {gene_id}")
            return None
            
        except Exception as e:
            logger.warning(f"Error resolving HGNC ID {gene_id}: {e}")
            return None
    
    def generate_validation_summary(self, validation_results: Dict[str, GeneValidationResult]) -> Dict[str, Any]:
        """
        Generate comprehensive validation summary statistics.
        
        Args:
            validation_results: Results from validate_all_genesets()
            
        Returns:
            Summary statistics dictionary
        """
        summary = {
            "total_genesets": len(validation_results),
            "total_input_genes": 0,
            "total_resolved_genes": 0,
            "overall_resolution_rate": 0.0,
            "collection_stats": defaultdict(lambda: {
                "genesets": 0,
                "input_genes": 0,
                "resolved_genes": 0,
                "resolution_rate": 0.0
            }),
            "resolution_distribution": {
                "perfect": 0,      # 100% resolution
                "excellent": 0,    # >= 95%
                "good": 0,         # >= 80%
                "moderate": 0,     # >= 60%
                "poor": 0          # < 60%
            },
            "unique_genes_found": set(),
            "unique_genes_missing": set(),
            "problematic_genesets": []
        }
        
        for geneset_id, result in validation_results.items():
            # Overall stats
            summary["total_input_genes"] += result.total_input_genes
            summary["total_resolved_genes"] += len(result.valid_genes) + len(result.gene_ids_resolved)
            
            # Track unique genes
            summary["unique_genes_found"].update(result.valid_genes)
            summary["unique_genes_found"].update(result.gene_ids_resolved)
            summary["unique_genes_missing"].update(result.invalid_genes)
            
            # Resolution rate distribution
            if result.resolution_rate == 1.0:
                summary["resolution_distribution"]["perfect"] += 1
            elif result.resolution_rate >= 0.95:
                summary["resolution_distribution"]["excellent"] += 1
            elif result.resolution_rate >= 0.80:
                summary["resolution_distribution"]["good"] += 1
            elif result.resolution_rate >= 0.60:
                summary["resolution_distribution"]["moderate"] += 1
            else:
                summary["resolution_distribution"]["poor"] += 1
                summary["problematic_genesets"].append({
                    "geneset_id": geneset_id,
                    "resolution_rate": result.resolution_rate,
                    "resolved": len(result.valid_genes) + len(result.gene_ids_resolved),
                    "total": result.total_input_genes
                })
        
        # Calculate overall resolution rate
        if summary["total_input_genes"] > 0:
            summary["overall_resolution_rate"] = summary["total_resolved_genes"] / summary["total_input_genes"]
        
        # Convert sets to counts for JSON serialization
        summary["unique_genes_found_count"] = len(summary["unique_genes_found"])
        summary["unique_genes_missing_count"] = len(summary["unique_genes_missing"])
        del summary["unique_genes_found"]  # Too large for summary
        del summary["unique_genes_missing"]  # Too large for summary
        
        return summary
    
    def get_missing_genes_report(self, validation_results: Dict[str, GeneValidationResult], 
                                limit: int = 50) -> Dict[str, Any]:
        """
        Generate report of most commonly missing genes across genesets.
        
        Args:
            validation_results: Validation results
            limit: Maximum number of missing genes to report
            
        Returns:
            Report of missing genes with frequencies
        """
        missing_gene_counts = defaultdict(int)
        genesets_with_missing = defaultdict(list)
        
        for geneset_id, result in validation_results.items():
            for missing_gene in result.invalid_genes:
                missing_gene_counts[missing_gene] += 1
                genesets_with_missing[missing_gene].append(geneset_id)
        
        # Sort by frequency
        sorted_missing = sorted(missing_gene_counts.items(), key=lambda x: x[1], reverse=True)
        
        report = {
            "total_unique_missing": len(missing_gene_counts),
            "most_common_missing": []
        }
        
        for gene, count in sorted_missing[:limit]:
            report["most_common_missing"].append({
                "gene_symbol": gene,
                "missing_from_genesets": count,
                "affected_genesets": genesets_with_missing[gene]
            })
        
        return report
    
    def get_validation_statistics(self) -> Dict[str, Any]:
        """Get validator performance statistics."""
        return dict(self.stats)


if __name__ == "__main__":
    # Example usage
    import logging
    import sys
    from pathlib import Path
    
    # Setup
    logging.basicConfig(level=logging.INFO)
    sys.path.append(str(Path(__file__).parent.parent))
    
    from neo4j_utils.connection import Neo4jConnection
    from talisman_integration.talisman_parser import TalismanGenesetParser
    
    # Initialize
    connection = Neo4jConnection()
    parser = TalismanGenesetParser("talisman-paper/genesets/human")
    validator = GeneSymbolValidator(connection)
    
    try:
        # Parse genesets
        genesets = parser.parse_all_genesets()
        print(f"Parsed {len(genesets)} genesets")
        
        # Validate genes
        validation_results = validator.validate_all_genesets(genesets)
        
        # Generate summary
        summary = validator.generate_validation_summary(validation_results)
        print(f"\nValidation Summary:")
        print(f"  Total genesets: {summary['total_genesets']}")
        print(f"  Overall resolution rate: {summary['overall_resolution_rate']:.2%}")
        print(f"  Perfect resolution (100%): {summary['resolution_distribution']['perfect']} genesets")
        print(f"  Excellent resolution (≥95%): {summary['resolution_distribution']['excellent']} genesets")
        print(f"  Good resolution (≥80%): {summary['resolution_distribution']['good']} genesets")
        print(f"  Unique genes found in KG: {summary['unique_genes_found_count']}")
        print(f"  Unique genes missing: {summary['unique_genes_missing_count']}")
        
        # Missing genes report
        missing_report = validator.get_missing_genes_report(validation_results, limit=10)
        print(f"\nTop 10 Missing Genes:")
        for item in missing_report["most_common_missing"]:
            print(f"  {item['gene_symbol']}: missing from {item['missing_from_genesets']} genesets")
    
    finally:
        connection.close()