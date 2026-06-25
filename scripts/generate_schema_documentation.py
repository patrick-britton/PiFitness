#!/usr/bin/env python3
"""
Database Schema Documentation Generator
Generates comprehensive documentation for the PiFitness database schema
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

def get_schemas():
    """Get all schemas in the database - filter to application schemas only"""
    sql = """
    SELECT schema_name
    FROM information_schema.schemata
    WHERE schema_name IN ('activities', 'health', 'music', 'staging', 'logging', 'metrics', 'api_services', 'tasks', 'public')
    ORDER BY schema_name
    """
    return sql_to_dict(sql)

def get_tables_for_schema(schema):
    """Get all tables in a specific schema"""
    sql = """
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = %s
    ORDER BY table_name
    """
    return sql_to_dict(sql, (schema,))

def get_columns_for_table(schema, table):
    """Get all columns for a specific table"""
    sql = """
    SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """
    return sql_to_dict(sql, (schema, table))

def get_primary_keys(schema, table):
    """Get primary key constraints for a table"""
    sql = """
    SELECT a.attname
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = %s::regclass AND i.indisprimary
    """
    # Need to use direct psycopg2 connection for this query
    import psycopg2
    from dotenv import load_dotenv
    import os

    load_dotenv()
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
        cur.execute(sql, (f"{schema}.{table}",))
        results = [row[0] for row in cur.fetchall()]
        return results
    except Exception as e:
        print(f"Error getting primary keys for {schema}.{table}: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def get_foreign_keys(schema, table):
    """Get foreign key constraints for a table"""
    sql = """
    SELECT
        tc.constraint_name,
        kcu.column_name,
        ccu.table_schema AS foreign_table_schema,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
    FROM
        information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema = %s
    AND tc.table_name = %s
    """
    return sql_to_dict(sql, (schema, table))

def get_stored_procedures(schema):
    """Get all stored procedures in a schema"""
    sql = """
    SELECT routine_name, routine_type, data_type, routine_definition
    FROM information_schema.routines
    WHERE routine_schema = %s
    ORDER BY routine_name
    """
    return sql_to_dict(sql, (schema,))

def get_procedure_parameters(schema, procedure_name):
    """Get parameters for a specific stored procedure"""
    sql = """
    SELECT parameter_name, data_type, parameter_mode
    FROM information_schema.parameters
    WHERE specific_schema = %s AND specific_name = %s
    ORDER BY ordinal_position
    """
    return sql_to_dict(sql, (schema, procedure_name))

def get_views(schema):
    """Get all views in a schema"""
    sql = """
    SELECT table_name, view_definition
    FROM information_schema.views
    WHERE table_schema = %s
    ORDER BY table_name
    """
    return sql_to_dict(sql, (schema,))

def generate_schema_documentation():
    """Generate comprehensive schema documentation"""
    documentation = {
        "generated_at": datetime.now().isoformat(),
        "schemas": {}
    }

    schemas = get_schemas()
    print(f"Found {len(schemas)} schemas to document")

    for schema in schemas:
        schema_name = schema['schema_name']
        print(f"Processing schema: {schema_name}")

        schema_doc = {
            "tables": {},
            "views": {},
            "procedures": {}
        }

        # Get tables and views
        tables = get_tables_for_schema(schema_name)

        for table in tables:
            table_name = table['table_name']
            table_type = table['table_type']

            if table_type == 'BASE TABLE':
                print(f"  Processing table: {table_name}")
                table_doc = {
                    "columns": get_columns_for_table(schema_name, table_name),
                    "primary_keys": get_primary_keys(schema_name, table_name),
                    "foreign_keys": get_foreign_keys(schema_name, table_name)
                }
                schema_doc["tables"][table_name] = table_doc
            elif table_type == 'VIEW':
                print(f"  Processing view: {table_name}")
                view_doc = {
                    "definition": get_views(schema_name)
                }
                schema_doc["views"][table_name] = view_doc

        # Get stored procedures
        procedures = get_stored_procedures(schema_name)
        for proc in procedures:
            proc_name = proc['routine_name']
            print(f"  Processing procedure: {proc_name}")
            proc_doc = {
                "type": proc['routine_type'],
                "return_type": proc['data_type'],
                "parameters": get_procedure_parameters(schema_name, proc_name),
                "definition": proc['routine_definition']
            }
            schema_doc["procedures"][proc_name] = proc_doc

        documentation["schemas"][schema_name] = schema_doc

    return documentation

def save_documentation(documentation, output_file="memory-bank/schema_documentation.json"):
    """Save documentation to a JSON file"""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as f:
        json.dump(documentation, f, indent=2)
    print(f"Schema documentation saved to {output_file}")

def generate_markdown_summary(documentation, output_file="memory-bank/schema_summary.md"):
    """Generate a human-readable markdown summary"""
    md_content = f"# PiFitness Database Schema Documentation\n\n"
    md_content += f"Generated: {documentation['generated_at']}\n\n"

    for schema_name, schema_doc in documentation['schemas'].items():
        md_content += f"\n## Schema: `{schema_name}`\n\n"

        # Tables
        if schema_doc['tables']:
            md_content += "### Tables\n\n"
            for table_name, table_doc in schema_doc['tables'].items():
                md_content += f"#### `{table_name}`\n\n"
                md_content += "- **Columns:**\n"
                for col in table_doc['columns']:
                    col_info = f"  - `{col['column_name']}`: {col['data_type']}"
                    if col['character_maximum_length']:
                        col_info += f"({col['character_maximum_length']})"
                    if col['is_nullable'] == 'NO':
                        col_info += " (NOT NULL)"
                    if col['column_default']:
                        col_info += f" DEFAULT {col['column_default']}"
                    md_content += col_info + "\n"

                if table_doc['primary_keys']:
                    md_content += "- **Primary Key:** " + ", ".join([f"`{pk}`" for pk in table_doc['primary_keys']]) + "\n"

                if table_doc['foreign_keys']:
                    md_content += "- **Foreign Keys:**\n"
                    for fk in table_doc['foreign_keys']:
                        md_content += f"  - `{fk['constraint_name']}`: `{fk['column_name']}` → `{fk['foreign_table_schema']}.{fk['foreign_table_name']}({fk['foreign_column_name']})`\n"

        # Views
        if schema_doc['views']:
            md_content += "### Views\n\n"
            for view_name, view_doc in schema_doc['views'].items():
                md_content += f"#### `{view_name}`\n\n"
                # View definitions would be added here

        # Procedures
        if schema_doc['procedures']:
            md_content += "### Stored Procedures\n\n"
            for proc_name, proc_doc in schema_doc['procedures'].items():
                md_content += f"#### `{proc_name}`\n\n"
                md_content += f"- **Type:** {proc_doc['type']}\n"
                md_content += f"- **Returns:** {proc_doc['return_type']}\n"

                if proc_doc['parameters']:
                    md_content += "- **Parameters:**\n"
                    for param in proc_doc['parameters']:
                        md_content += f"  - `{param['parameter_name']}`: {param['data_type']} ({param['parameter_mode']})\n"

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(md_content)
    print(f"Schema summary saved to {output_file}")

if __name__ == "__main__":
    print("Generating PiFitness database schema documentation...")
    documentation = generate_schema_documentation()
    save_documentation(documentation)
    generate_markdown_summary(documentation)
    print("Schema documentation generation complete!")