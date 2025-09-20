#!/usr/bin/env python3

import sys
import os
import subprocess
from pathlib import Path
from go_kg_builder import CompleteGOKnowledgeGraphCreator
from go_terms_interconnector import GOInterconnector
from go_branch_integrator import GOBranchIntegrator


def get_data_dir():
    """Get data directory path - works both in Docker and locally."""
    if os.path.exists('/app/data'):
        return '/app/data'  # Docker environment
    else:
        # Local environment - find project root
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        return os.path.join(project_root, 'data')


def main():
    # Detect environment and get data directory
    data_dir = get_data_dir()
    print(f"Environment detected - Data directory: {data_dir}")

    # Build individual namespaces
    namespaces = ['bp', 'cc', 'mf']
    
    for namespace in namespaces:
        print(f"\n{'='*60}")
        print(f"STARTING GO {namespace.upper()} KNOWLEDGE GRAPH")
        print(f"{'='*60}")
        
        try:
            with CompleteGOKnowledgeGraphCreator(namespace=namespace, data_dir=data_dir) as creator:
                success = creator.create_complete_knowledge_graph()
                
                if success:
                    print(f"GO {namespace.upper()} completed successfully")
                else:
                    print(f"GO {namespace.upper()} failed")
                    return False
                    
        except Exception as e:
            print(f"Error with GO {namespace.upper()}: {e}")
            return False
    
    print("\nALL GO NAMESPACES COMPLETED SUCCESSFULLY")
    
    # Interconnect namespaces
    print(f"\n{'='*60}")
    print("STARTING GO NAMESPACE INTERCONNECTION")
    print(f"{'='*60}")
    
    try:
        with GOInterconnector(data_dir=data_dir) as interconnector:
            success = interconnector.create_interconnections()
            
            if success:
                print("GO INTERCONNECTION completed successfully")
            else:
                print("GO INTERCONNECTION failed")
                return False
                
    except Exception as e:
        print(f"Error with GO INTERCONNECTION: {e}")
        return False
    
    # Integrate external GO branch data
    print(f"\n{'='*60}")
    print("STARTING GO BRANCH DATA INTEGRATION")
    print(f"{'='*60}")
    
    try:
        with GOBranchIntegrator(data_dir=data_dir) as integrator:
            success = integrator.run_integration()
            
            if success:
                print("GO BRANCH INTEGRATION completed successfully")
            else:
                print("GO BRANCH INTEGRATION failed")
                return False
                
    except Exception as e:
        print(f"Error with GO BRANCH INTEGRATION: {e}")
        return False
    
    print("\nCOMPLETE GO KNOWLEDGE GRAPH BUILD FINISHED")
    
    # OMICS DATA INTEGRATION
    print(f"\n{'='*60}")
    print("STARTING OMICS DATA INTEGRATION")
    print(f"{'='*60}")
    
    omics_scripts = [
        ("omics_schema_setup.py", "OMICS SCHEMA SETUP"),
        ("omics_disease_integration.py", "DISEASE INTEGRATION"),
        ("omics_viral_integration.py", "VIRAL INTEGRATION"),
        ("omics_drug_integration.py", "DRUG INTEGRATION"),
        ("omics_nest_integration.py", "NEST NETWORK INTEGRATION"),
        ("omics_pathway_integration.py", "PATHWAY INTEGRATION")
    ]
    
    for script, description in omics_scripts:
        print(f"\n{'='*60}")
        print(f"STARTING {description}")
        print(f"{'='*60}")

        try:
            # Detect script directory - works both in Docker and locally
            current_dir = os.path.dirname(os.path.abspath(__file__))
            script_path = os.path.join(current_dir, script)

            # Schema setup doesn't need data-dir, others do
            if script == "omics_schema_setup.py":
                cmd = [sys.executable, script_path]
            else:
                cmd = [sys.executable, script_path, '--data-dir', data_dir]

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            print(f"{description} completed successfully")
            
        except subprocess.CalledProcessError as e:
            print(f"Error with {description}: {e}")
            print(f"STDOUT: {e.stdout}")
            print(f"STDERR: {e.stderr}")
            return False
        except Exception as e:
            print(f"Error with {description}: {e}")
            return False
    
    # TALISMAN GENESET INTEGRATION
    print(f"\n{'='*60}")
    print("STARTING TALISMAN GENESET INTEGRATION")
    print(f"{'='*60}")
    
    talisman_scripts = [
        ("talisman_schema_setup.py", "TALISMAN SCHEMA SETUP"),
        ("talisman_integration_engine.py", "TALISMAN GENESET INTEGRATION")
    ]
    
    for script, description in talisman_scripts:
        print(f"\n{'='*60}")
        print(f"STARTING {description}")
        print(f"{'='*60}")

        try:
            # Detect script directory - works both in Docker and locally
            current_dir = os.path.dirname(os.path.abspath(__file__))
            script_path = os.path.join(current_dir, script)
            result = subprocess.run([sys.executable, script_path, '--data-dir', data_dir],
                                  capture_output=True, text=True, check=True)
            print(f"{description} completed successfully")
            
        except subprocess.CalledProcessError as e:
            print(f"Error with {description}: {e}")
            print(f"STDOUT: {e.stdout}")
            print(f"STDERR: {e.stderr}")
            return False
        except Exception as e:
            print(f"Error with {description}: {e}")
            return False
    
    print("\nCOMPLETE BIOMEDICAL KNOWLEDGE GRAPH BUILD FINISHED")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)