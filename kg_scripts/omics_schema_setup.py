"""
Schema Extension for Omics Integration
"""
from neo4j import GraphDatabase

def create_omics_constraints():
    """Create constraints for omics entities"""
    constraints = [
        "CREATE CONSTRAINT disease_name_unique IF NOT EXISTS FOR (d:Disease) REQUIRE d.name IS UNIQUE",
        "CREATE CONSTRAINT virus_name_unique IF NOT EXISTS FOR (v:Virus) REQUIRE v.name IS UNIQUE", 
        "CREATE CONSTRAINT drug_name_unique IF NOT EXISTS FOR (d:Drug) REQUIRE d.name IS UNIQUE",
        "CREATE CONSTRAINT study_geo_unique IF NOT EXISTS FOR (s:Study) REQUIRE s.geo_id IS UNIQUE",
        "CREATE CONSTRAINT module_id_unique IF NOT EXISTS FOR (m:FunctionalModule) REQUIRE m.cluster_id IS UNIQUE"
    ]
    return constraints

def create_omics_indexes():
    """Create indexes for omics entities"""
    indexes = [
        "CREATE INDEX disease_name_idx IF NOT EXISTS FOR (d:Disease) ON (d.name)",
        "CREATE INDEX virus_strain_idx IF NOT EXISTS FOR (v:Virus) ON (v.strain)",
        "CREATE INDEX drug_name_idx IF NOT EXISTS FOR (d:Drug) ON (d.name)",
        "CREATE INDEX study_geo_idx IF NOT EXISTS FOR (s:Study) ON (s.geo_id)",
        "CREATE INDEX module_level_idx IF NOT EXISTS FOR (m:FunctionalModule) ON (m.level)"
    ]
    return indexes

def extend_schema():
    """Extend the existing schema with omics entities"""
    
    driver = GraphDatabase.driver("bolt://localhost:7687")
    
    with driver.session(database="biomedical-kg") as session:
        print("Creating omics constraints...")
        constraints = create_omics_constraints()
        for constraint in constraints:
            try:
                session.run(constraint)
                print(f" {constraint}")
            except Exception as e:
                print(f" {constraint}: {e}")
        
        print("\nCreating omics indexes...")
        indexes = create_omics_indexes()
        for index in indexes:
            try:
                session.run(index)
                print(f"{index}")
            except Exception as e:
                print(f" {index}: {e}")
    
    driver.close()
    print("\nPhase 1 complete: Schema extended for omics data")

if __name__ == "__main__":
    extend_schema()