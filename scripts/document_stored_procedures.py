#!/usr/bin/env python3
"""
Stored Procedures Documentation Generator
Generates detailed documentation for the 29 stored procedures in the staging schema
"""

import os
import sys
import json
from datetime import datetime

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

# Load .env file from backend directory
env_path = os.path.join(project_root, 'backend', '.env')
if os.path.exists(env_path):
    from dotenv import load_dotenv
    load_dotenv(env_path)
else:
    print(f"Warning: .env file not found at {env_path}")

from backend_functions.database_functions import get_conn, sql_to_dict, one_sql_result

def get_staging_procedures():
    """Get all stored procedures in the staging schema"""
    sql = """
    SELECT routine_name, routine_type, data_type, routine_definition
    FROM information_schema.routines
    WHERE routine_schema = 'staging'
    ORDER BY routine_name
    """
    return sql_to_dict(sql)

def get_procedure_parameters(schema, procedure_name):
    """Get parameters for a specific stored procedure"""
    sql = """
    SELECT parameter_name, data_type, parameter_mode, ordinal_position
    FROM information_schema.parameters
    WHERE specific_schema = %s AND specific_name = %s
    ORDER BY ordinal_position
    """
    return sql_to_dict(sql, (schema, procedure_name))

def get_procedure_dependencies(procedure_name):
    """Get dependencies for a specific stored procedure"""
    sql = """
    SELECT DISTINCT
        dep.refobjid::regclass AS dependent_object,
        dep.refobjsubid AS dependent_subid,
        CASE
            WHEN dep.deptype = 'n' THEN 'normal'
            WHEN dep.deptype = 'a' THEN 'auto'
            WHEN dep.deptype = 'p' THEN 'pinned'
            ELSE dep.deptype::text
        END AS dependency_type
    FROM pg_depend dep
    JOIN pg_proc proc ON dep.objid = proc.oid
    JOIN pg_namespace ns ON proc.pronamespace = ns.oid
    WHERE ns.nspname = 'staging'
    AND proc.proname = %s
    AND dep.refobjid <> 0
    """
    # Use direct psycopg2 connection for this query
    import psycopg2
    from dotenv import load_dotenv
    import os

    load_dotenv(env_path)
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PGSSLMODE", "disable")
    )
    cur = conn.cursor()
    try:
        cur.execute(sql, (procedure_name,))
        results = []
        for row in cur.fetchall():
            dependent_object = row[0]
            if dependent_object.startswith('staging.'):
                dependent_object = dependent_object[8:]  # Remove schema prefix
            results.append({
                'dependent_object': dependent_object,
                'dependency_type': row[2]
            })
        return results
    except Exception as e:
        print(f"Error getting dependencies for {procedure_name}: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def analyze_procedure_definition(procedure_name, definition):
    """Analyze the procedure definition to extract key information"""
    analysis = {
        'tables_referenced': [],
        'functions_called': [],
        'complex_operations': [],
        'side_effects': []
    }

    # Simple text analysis - this could be enhanced with proper SQL parsing
    lines = definition.split('\n')
    for line in lines:
        line = line.strip().upper()

        # Look for table references
        if ' FROM ' in line or ' INTO ' in line or ' UPDATE ' in line or ' INSERT INTO ' in line:
            words = line.split()
            for i, word in enumerate(words):
                if word in ['FROM', 'INTO', 'UPDATE', 'INSERT'] and i+1 < len(words):
                    possible_table = words[i+1]
                    if possible_table not in ['(', 'SELECT', 'VALUES'] and '.' not in possible_table:
                        analysis['tables_referenced'].append(possible_table)

        # Look for function calls
        if 'CALL ' in line or 'PERFORM ' in line:
            words = line.split()
            for i, word in enumerate(words):
                if word in ['CALL', 'PERFORM'] and i+1 < len(words):
                    analysis['functions_called'].append(words[i+1])

        # Look for side effects
        if line.startswith('CREATE') or line.startswith('DROP') or line.startswith('ALTER'):
            analysis['side_effects'].append(f"Schema modification: {line}")
        elif 'DELETE FROM' in line or 'UPDATE ' in line or 'INSERT INTO' in line:
            analysis['side_effects'].append(f"Data modification: {line}")
        elif 'RAISE' in line or 'EXCEPTION' in line:
            analysis['side_effects'].append(f"Error handling: {line}")

    # Remove duplicates
    analysis['tables_referenced'] = list(set(analysis['tables_referenced']))
    analysis['functions_called'] = list(set(analysis['functions_called']))

    return analysis

def document_stored_procedures():
    """Generate comprehensive documentation for staging schema stored procedures"""
    documentation = {
        "generated_at": datetime.now().isoformat(),
        "schema": "staging",
        "procedures": {},
        "summary": {
            "total_procedures": 0,
            "by_category": {},
            "tables_referenced": set(),
            "functions_called": set()
        }
    }

    procedures = get_staging_procedures()
    print(f"Found {len(procedures)} procedures in staging schema")

    for proc in procedures:
        proc_name = proc['routine_name']
        print(f"Processing procedure: {proc_name}")

        # Get parameters
        parameters = get_procedure_parameters('staging', proc_name)

        # Get dependencies
        dependencies = get_procedure_dependencies(proc_name)

        # Analyze definition
        analysis = analyze_procedure_definition(proc_name, proc['routine_definition'])

        # Categorize procedure
        category = "Unknown"
        if "flatten" in proc_name.lower():
            category = "Data Flattening"
        elif "activity" in proc_name.lower():
            category = "Activity Processing"
        elif "health" in proc_name.lower() or "heartrate" in proc_name.lower() or "sleep" in proc_name.lower():
            category = "Health Data Processing"
        elif "music" in proc_name.lower() or "playlist" in proc_name.lower() or "track" in proc_name.lower():
            category = "Music Data Processing"
        elif "segment" in proc_name.lower():
            category = "Segment Operations"
        elif "elevation" in proc_name.lower():
            category = "Elevation Processing"
        elif "json" in proc_name.lower() or "sample" in proc_name.lower():
            category = "Utility"

        # Build procedure documentation
        proc_doc = {
            "type": proc['routine_type'],
            "return_type": proc['data_type'],
            "category": category,
            "parameters": parameters,
            "dependencies": dependencies,
            "analysis": analysis,
            "definition_preview": proc['routine_definition'][:500] + "..." if len(proc['routine_definition']) > 500 else proc['routine_definition']
        }

        documentation["procedures"][proc_name] = proc_doc

        # Update summary statistics
        documentation["summary"]["total_procedures"] += 1
        documentation["summary"]["by_category"][category] = documentation["summary"]["by_category"].get(category, 0) + 1
        documentation["summary"]["tables_referenced"].update(analysis['tables_referenced'])
        documentation["summary"]["functions_called"].update(analysis['functions_called'])

    # Convert sets to lists for JSON serialization
    documentation["summary"]["tables_referenced"] = sorted(list(documentation["summary"]["tables_referenced"]))
    documentation["summary"]["functions_called"] = sorted(list(documentation["summary"]["functions_called"]))

    return documentation

def save_procedure_documentation(documentation):
    """Save procedure documentation to files"""
    # Save JSON version
    json_file = "memory-bank/stored_procedures_documentation.json"
    os.makedirs(os.path.dirname(json_file), exist_ok=True)
    with open(json_file, 'w') as f:
        json.dump(documentation, f, indent=2)
    print(f"Stored procedures documentation saved to {json_file}")

    # Generate markdown version
    md_content = generate_markdown_documentation(documentation)
    md_file = "memory-bank/stored_procedures_summary.md"
    os.makedirs(os.path.dirname(md_file), exist_ok=True)
    with open(md_file, 'w', encoding='utf-8') as f:
        f.write(md_content)
    print(f"Stored procedures summary saved to {md_file}")

def generate_markdown_documentation(documentation):
    """Generate human-readable markdown documentation"""
    md_content = f"# Staging Schema Stored Procedures Documentation\n\n"
    md_content += f"Generated: {documentation['generated_at']}\n\n"

    # Summary section
    md_content += "## Summary\n\n"
    md_content += f"- **Total Procedures:** {documentation['summary']['total_procedures']}\n"
    md_content += "- **By Category:**\n"
    for category, count in documentation['summary']['by_category'].items():
        md_content += f"  - {category}: {count}\n"
    md_content += f"- **Tables Referenced:** {len(documentation['summary']['tables_referenced'])}\n"
    md_content += f"- **Functions Called:** {len(documentation['summary']['functions_called'])}\n\n"

    # Detailed procedures section
    md_content += "## Detailed Procedure Documentation\n\n"

    # Group by category
    by_category = {}
    for proc_name, proc_doc in documentation['procedures'].items():
        category = proc_doc['category']
        if category not in by_category:
            by_category[category] = []
        by_category[category].append((proc_name, proc_doc))

    for category in sorted(by_category.keys()):
        md_content += f"### {category}\n\n"
        for proc_name, proc_doc in by_category[category]:
            md_content += f"#### `{proc_name}`\n\n"
            md_content += f"- **Type:** {proc_doc['type']}\n"
            md_content += f"- **Returns:** {proc_doc['return_type']}\n"

            # Parameters
            if proc_doc['parameters']:
                md_content += "- **Parameters:**\n"
                for param in proc_doc['parameters']:
                    mode = param['parameter_mode']
                    if mode == 'IN':
                        mode_symbol = "→"
                    elif mode == 'OUT':
                        mode_symbol = "←"
                    else:
                        mode_symbol = "↔"
                    md_content += f"  - `{param['parameter_name']}`: {param['data_type']} {mode_symbol}\n"

            # Dependencies
            if proc_doc['dependencies']:
                md_content += "- **Dependencies:**\n"
                for dep in proc_doc['dependencies']:
                    md_content += f"  - {dep['dependency_type']}: `{dep['dependent_object']}`\n"

            # Analysis
            if proc_doc['analysis']['tables_referenced']:
                md_content += "- **Tables Referenced:** " + ", ".join([f"`{table}`" for table in proc_doc['analysis']['tables_referenced']]) + "\n"

            if proc_doc['analysis']['functions_called']:
                md_content += "- **Functions Called:** " + ", ".join([f"`{func}`" for func in proc_doc['analysis']['functions_called']]) + "\n"

            if proc_doc['analysis']['side_effects']:
                md_content += "- **Side Effects:**\n"
                for effect in proc_doc['analysis']['side_effects'][:3]:  # Limit to 3 most important
                    md_content += f"  - {effect}\n"

            md_content += f"- **Definition Preview:**\n```sql\n{proc_doc['definition_preview']}\n```\n\n"

    return md_content

if __name__ == "__main__":
    print("Generating detailed stored procedures documentation...")
    documentation = document_stored_procedures()
    save_procedure_documentation(documentation)
    print("Stored procedures documentation generation complete!")