#!/usr/bin/env python3
"""
Knowledge Graph Metrics Monitor
===============================

Clean, focused script to track essential KG metrics:
- Node counts by type/namespace
- Relationship counts by type 
- Key connectivity statistics
- Data quality indicators
"""

import logging
from neo4j import GraphDatabase
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class KGMetrics:
    """Knowledge Graph metrics collector"""
    
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="password"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            self.driver.close()
    
    def get_node_counts(self):
        """Get node counts by label and namespace"""
        with self.driver.session() as session:
            # Total nodes
            total = session.run("MATCH (n) RETURN count(n) as count").single()['count']
            
            # Nodes by label - simple approach
            labels = session.run("""
            MATCH (n)
            RETURN labels(n)[0] as label, count(n) as count
            ORDER BY count DESC
            """).data()
            
            # GO terms by namespace
            go_namespaces = session.run("""
            MATCH (go:GOTerm)
            WHERE go.namespace IS NOT NULL
            RETURN go.namespace as namespace, count(go) as count
            ORDER BY count DESC
            """).data()
            
            return {
                'total_nodes': total,
                'by_label': labels,
                'go_namespaces': go_namespaces
            }
    
    def get_relationship_counts(self):
        """Get relationship counts by type"""
        with self.driver.session() as session:
            # Total relationships
            total = session.run("MATCH ()-[r]->() RETURN count(r) as count").single()['count']
            
            # By relationship type - simple approach
            rel_types = session.run("""
            MATCH ()-[r]->()
            RETURN type(r) as type, count(r) as count
            ORDER BY count DESC
            """).data()
            
            return {
                'total_relationships': total,
                'by_type': rel_types
            }
    
    def get_connectivity_stats(self):
        """Get key connectivity statistics"""
        with self.driver.session() as session:
            # Basic connectivity by label
            stats = session.run("""
            MATCH (n)-[r]-(m)
            WITH labels(n)[0] as label, count(r) as total_connections, count(DISTINCT n) as nodes
            RETURN label, nodes, total_connections, (total_connections * 1.0 / nodes) as avg_degree
            ORDER BY avg_degree DESC
            """).data()
            
            # Cross-namespace connections for GO - corrected query
            cross_ns = []
            
            # BP -> CC connections
            bp_cc = session.run("""
            MATCH (bp:GOTerm {namespace: 'biological_process'})-[r]->(cc:GOTerm {namespace: 'cellular_component'})
            RETURN type(r) as rel_type, count(r) as count
            """).data()
            cross_ns.extend(bp_cc)
            
            # BP -> MF connections  
            bp_mf = session.run("""
            MATCH (bp:GOTerm {namespace: 'biological_process'})-[r]->(mf:GOTerm {namespace: 'molecular_function'})
            RETURN type(r) as rel_type, count(r) as count
            """).data()
            cross_ns.extend(bp_mf)
            
            # CC -> MF connections
            cc_mf = session.run("""
            MATCH (cc:GOTerm {namespace: 'cellular_component'})-[r]->(mf:GOTerm {namespace: 'molecular_function'})
            RETURN type(r) as rel_type, count(r) as count
            """).data()
            cross_ns.extend(cc_mf)
            
            return {
                'connectivity_by_label': stats,
                'cross_namespace_connections': cross_ns
            }
    
    def get_quality_indicators(self):
        """Get data quality indicators"""
        with self.driver.session() as session:
            # Orphaned nodes - nodes with no relationships
            orphans = session.run("""
            MATCH (n)
            WHERE NOT (n)-[]-()
            RETURN labels(n)[0] as label, count(n) as orphan_count
            ORDER BY orphan_count DESC
            """).data()
            
            # Multi-namespace genes
            multi_ns_genes = session.run("""
            MATCH (g:Gene)-[:ANNOTATED_WITH]->(go:GOTerm)
            WHERE go.namespace IS NOT NULL
            WITH g, collect(DISTINCT go.namespace) as namespaces
            WHERE size(namespaces) > 1
            RETURN size(namespaces) as namespace_count, count(g) as gene_count
            ORDER BY namespace_count DESC
            """).data()
            
            # Multi-modal gene coverage (genes with 5+ data types)
            multi_modal = session.run("""
            MATCH (g:Gene)
            WITH g, 
                 (CASE WHEN EXISTS((g)-[:ANNOTATED_WITH]->(:GOTerm)) THEN 1 ELSE 0 END) +
                 (CASE WHEN EXISTS((g)-[:ASSOCIATED_WITH_DISEASE]->(:Disease)) THEN 1 ELSE 0 END) +
                 (CASE WHEN EXISTS((g)-[:INFECTED_BY]->(:Virus)) THEN 1 ELSE 0 END) +
                 (CASE WHEN EXISTS((g)-[:PERTURBED_BY]->(:Drug)) THEN 1 ELSE 0 END) +
                 (CASE WHEN EXISTS((g)-[:BELONGS_TO_MODULE]->(:FunctionalModule)) THEN 1 ELSE 0 END) +
                 (CASE WHEN EXISTS((g)-[:MEMBER_OF_PATHWAY]->(:PathwayModule)) THEN 1 ELSE 0 END) as data_types
            WHERE data_types >= 5
            RETURN count(g) as count
            """).single()['count']
            
            # Total gene count for reference
            total_genes = session.run("MATCH (g:Gene) RETURN count(g) as count").single()['count']
            
            return {
                'orphaned_nodes': orphans,
                'multi_namespace_genes': multi_ns_genes,
                'multi_modal_genes': multi_modal,
                'total_genes': total_genes
            }
    
    def collect_all_metrics(self):
        """Collect all KG metrics"""
        logger.info(" Collecting KG metrics...")
        
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'nodes': self.get_node_counts(),
            'relationships': self.get_relationship_counts(),
            'connectivity': self.get_connectivity_stats(),
            'quality': self.get_quality_indicators()
        }
        
        return metrics
    
    def print_summary(self, metrics):
        """Print concise metrics summary"""
        print("=" * 60)
        print("KNOWLEDGE GRAPH METRICS")
        print("=" * 60)
        
        # Nodes
        nodes = metrics['nodes']
        print(f"\n NODES: {nodes['total_nodes']:,}")
        for item in nodes['by_label'][:5]:  # Top 5
            print(f"   {item['label']}: {item['count']:,}")
        
        if nodes['go_namespaces']:
            print(f"\n   GO Namespaces:")
            for ns in nodes['go_namespaces']:
                print(f"   {ns['namespace']}: {ns['count']:,}")
        
        # Relationships
        rels = metrics['relationships']
        print(f"\n RELATIONSHIPS: {rels['total_relationships']:,}")
        for item in rels['by_type'][:5]:  # Top 5
            print(f"   {item['type']}: {item['count']:,}")
        
        # Cross-namespace connections
        cross_ns = metrics['connectivity']['cross_namespace_connections']
        if cross_ns:
            print(f"\n CROSS-NAMESPACE CONNECTIONS:")
            # Group by relationship type
            rel_counts = {}
            for conn in cross_ns:
                rel_type = conn['rel_type']
                count = conn['count']
                if rel_type in rel_counts:
                    rel_counts[rel_type] += count
                else:
                    rel_counts[rel_type] = count
            
            for rel_type, count in sorted(rel_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"   {rel_type}: {count:,}")
        
        # Quality indicators
        quality = metrics['quality']
        
        # Orphaned nodes
        if quality['orphaned_nodes']:
            total_orphans = sum(item['orphan_count'] for item in quality['orphaned_nodes'])
            print(f"\  ORPHANED NODES: {total_orphans:,}")
            for orphan in quality['orphaned_nodes'][:3]:  # Top 3
                print(f"   {orphan['label']}: {orphan['orphan_count']:,}")
        
        # Multi-namespace genes
        if quality['multi_namespace_genes']:
            total_multi = sum(item['gene_count'] for item in quality['multi_namespace_genes'])
            print(f"\n MULTI-NAMESPACE GENES: {total_multi:,} / {quality['total_genes']:,} total")
            for multi in quality['multi_namespace_genes']:
                print(f"   {multi['namespace_count']} namespaces: {multi['gene_count']:,} genes")
        
        # Multi-modal genes
        if quality['multi_modal_genes']:
            print(f"\n MULTI-MODAL GENES (5+ data types): {quality['multi_modal_genes']:,}")
        
        # Connectivity
        conn = metrics['connectivity']['connectivity_by_label']
        if conn:
            print(f"\n CONNECTIVITY (Top 3):")
            for item in conn[:3]:
                print(f"   {item['label']}: {item['avg_degree']:.1f} avg connections")
        
        print("=" * 60)


def main():
    """Main execution"""
    try:
        with KGMetrics() as metrics:
            data = metrics.collect_all_metrics()
            
            # Print summary
            metrics.print_summary(data)
            
            # Save detailed metrics
            with open('biomedical_kg_metrics.json', 'w') as f:
                json.dump(data, f, indent=2, default=str)
            
            logger.info(" Detailed metrics saved to biomedical_kg_metrics.json")
            return True
            
    except Exception as e:
        logger.error(f" Metrics collection failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)