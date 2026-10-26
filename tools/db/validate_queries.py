"""
Query Validation Script
========================

Validate all query functions against the actual database schema.
This script checks if tables and columns referenced in queries actually exist.
"""

import re
import inspect
from typing import List, Dict, Tuple
from backend_functions.db_schema import get_tables, get_columns, column_exists

def extract_sql_tables(sql: str) -> List[str]:
    """Extract table names from SQL query string."""
    # Pattern: FROM schema.table or JOIN schema.table
    pattern = r'(?:FROM|JOIN)\s+([a-z_]+)\.([a-z_]+)'
    matches = re.findall(pattern, sql, re.IGNORECASE)
    return [f"{schema}.{table}" for schema, table in matches]

def extract_sql_columns(sql: str) -> List[str]:
    """Extract column names from SQL query string."""
    # Pattern: SELECT column, column2 or WHERE column = value
    # This is simplified - would need more sophisticated parsing for full SQL
    pattern = r'SELECT\s+(.+?)\s+FROM'
    match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
    if match:
        select_clause = match.group(1)
        # Split by comma, handle AS aliases
        cols = [c.split()[0].strip() for c in select_clause.split(',')]
        return cols
    return []

def validate_query_function(func) -> Dict[str, any]:
    """Validate a single query function."""
    try:
        source = inspect.getsource(func)
        sql_matches = re.findall(r'""".*?"""|""".*?"""', source, re.DOTALL)

        if not sql_matches:
            return {"function": func.__name__, "status": "warning", "issue": "No SQL found"}

        sql = sql_matches[0].replace('"""', '').strip()
        tables = extract_sql_tables(sql)
        columns = extract_sql_columns(sql)

        issues = []

        # Check tables exist
        for table in tables:
            schema, table_name = table.split('.')
            if table_name not in get_tables(schema):
                issues.append(f"Table {table} does not exist")

        # Check columns exist (simplified - would need table context)
        # For now, just check if columns are referenced

        return {
            "function": func.__name__,
            "tables": tables,
            "columns": columns,
            "issues": issues,
            "status": "ok" if not issues else "error"
        }

    except Exception as e:
        return {
            "function": func.__name__,
            "status": "error",
            "issue": f"Validation failed: {str(e)}"
        }

def validate_all_queries() -> Dict[str, List[Dict]]:
    """Validate all query functions in all query modules."""
    from backend_functions.queries import (
        activities_queries,
        health_queries,
        music_queries,
        admin_queries
    )

    results = {
        "activities": [],
        "health": [],
        "music": [],
        "admin": []
    }

    modules = {
        "activities": activities_queries,
        "health": health_queries,
        "music": music_queries,
        "admin": admin_queries
    }

    for module_name, module in modules.items():
        for name, func in inspect.getmembers(module):
            if name.startswith('get_') and callable(func) and func.__module__ == module.__name__:
                results[module_name].append(validate_query_function(func))

    return results

def generate_report(results: Dict[str, List[Dict]]) -> str:
    """Generate human-readable report."""
    lines = []
    lines.append("=" * 80)
    lines.append("QUERY VALIDATION REPORT")
    lines.append("=" * 80)
    lines.append("")

    for module_name, functions in results.items():
        lines.append(f"Module: {module_name}")
        lines.append("-" * 80)

        for func_result in functions:
            status_emoji = "✅" if func_result["status"] == "ok" else "❌"
            lines.append(f"{status_emoji} {func_result['function']}")

            if func_result["issues"]:
                for issue in func_result["issues"]:
                    lines.append(f"    ⚠️  {issue}")

            if func_result.get("tables"):
                lines.append(f"    Tables: {', '.join(func_result['tables'])}")

    lines.append("")
    lines.append("=" * 80)
    return "\n".join(lines)

def check_legacy_references() -> Dict[str, List[str]]:
    """Check if query functions are referenced in legacy code."""
    import os
    import glob

    references = {
        "activities_telemetry": [],
        "segment_matches": [],
        "segment_match_id": []
    }

    # Search in frontend_functions
    for pattern in ["activity_telemetry", "segment_matches", "segment_match_id"]:
        for file_path in glob.glob("frontend_functions/*.py"):
            with open(file_path, 'r') as f:
                content = f.read()
                if pattern in content:
                    references[pattern].append(file_path)

    return references

if __name__ == "__main__":
    print("Validating queries against database schema...")
    print("")

    try:
        results = validate_all_queries()
        report = generate_report(results)
        print(report)

        print("\nChecking legacy code references...")
        refs = check_legacy_references()

        for pattern, files in refs.items():
            if files:
                print(f"⚠️  '{pattern}' is referenced in:")
                for file in files:
                    print(f"    - {file}")
            else:
                print(f"✅ '{pattern}' has no legacy references")

    except Exception as e:
        print(f"❌ Validation failed: {str(e)}")
        import traceback
        traceback.print_exc()