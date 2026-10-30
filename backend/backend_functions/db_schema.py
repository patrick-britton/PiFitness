"""
Database Schema Introspection
==============================

Helper functions to query and validate database schema.
Connects to the production database (Pi) using standard .env configuration.
"""

from typing import List, Dict, Any, Sequence
from backend_functions.database_functions import sql_to_dict

def get_columns(schema: str, table: str) -> List[str]:
    """
    Get all column names for a specific table.

    Args:
        schema: The schema name (e.g., 'activities', 'health')
        table: The table name

    Returns:
        List of column names
    """
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    results = sql_to_dict(sql, (schema, table))
    return [row['column_name'] for row in results]

def get_tables(schema: str) -> List[str]:
    """
    Get all table names in a schema.

    Args:
        schema: The schema name

    Returns:
        List of table names
    """
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """
    results = sql_to_dict(sql, (schema,))
    return [row['table_name'] for row in results]

def column_exists(schema: str, table: str, column: str) -> bool:
    """
    Check if a specific column exists in a table.

    Args:
        schema: The schema name
        table: The table name
        column: The column name to check

    Returns:
        True if the column exists, False otherwise
    """
    cols = get_columns(schema, table)
    return column in cols

def get_table_info(schema: str, table: str) -> Sequence[Dict[str, Any]]:
    """
    Get comprehensive information about a table.

    Args:
        schema: The schema name
        table: The table name

    Returns:
        List of dictionaries with column information
    """
    sql = """
        SELECT
            column_name,
            data_type,
            is_nullable,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    return sql_to_dict(sql, (schema, table))

def generate_schema_documentation(output_file: str = None) -> str:
    """
    Generate comprehensive schema documentation.

    Args:
        output_file: If provided, write documentation to this file

    Returns:
        Schema documentation as a string
    """
    schemas = ['activities', 'health', 'music', 'tasks', 'logging', 'staging']
    output_lines = []

    for schema in schemas:
        try:
            tables = get_tables(schema)
            output_lines.append(f"\n-- Schema: {schema}")
            output_lines.append(f"-- Tables: {len(tables)}")

            for table in tables:
                output_lines.append(f"\n-- Table: {schema}.{table}")
                cols = get_columns(schema, table)
                for col in cols:
                    output_lines.append(f"--   {col}")

        except Exception as e:
            output_lines.append(f"\n-- Schema {schema}: Error - {str(e)}")

    documentation = '\n'.join(output_lines)

    if output_file:
        with open(output_file, 'w') as f:
            f.write(documentation)

    return documentation

__all__ = [
    'get_columns',
    'get_tables',
    'column_exists',
    'get_table_info',
    'generate_schema_documentation',
]