"""
Neo4j Configuration for Biomedical Knowledge Graph
"""

NEO4J_CONFIG = {
    'uri': 'bolt://localhost:7687',
    'username': 'neo4j',
    'password': 'password',  # Simple password
    'database': 'neo4j'  # Default database
}

# Connection pool settings
NEO4J_CONNECTION_POOL = {
    'max_connection_pool_size': 50,
    'max_transaction_retry_time': 30,
    'initial_retry_delay': 1.0,
    'retry_delay_multiplier': 2.0,
    'retry_delay_jitter_factor': 0.2
}

# Batch processing settings
BATCH_CONFIG = {
    'batch_size': 1000,
    'max_batch_size': 5000,
    'transaction_timeout': 300  # 5 minutes
}