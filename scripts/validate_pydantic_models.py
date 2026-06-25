#!/usr/bin/env python3
"""
Pydantic Models Validation Script
Validates Pydantic models against actual database columns
"""

import os
import sys
import json
from datetime import datetime
import importlib

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

from backend_functions.database_functions import sql_to_dict, get_conn
from backend.schemas import Activity, HeartRate, SleepData, Track, Playlist, TaskExecution

def get_table_columns(schema, table):
    """Get column information for a specific table"""
    sql = """
    SELECT column_name, data_type, is_nullable, character_maximum_length
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """
    return sql_to_dict(sql, (schema, table))

def validate_model_against_table(model, schema, table):
    """Validate a Pydantic model against a database table"""
    print(f"\n🔍 Validating {model.__name__} against {schema}.{table}...")

    # Get actual database columns
    db_columns = get_table_columns(schema, table)
    db_column_names = {col['column_name'] for col in db_columns}
    db_column_info = {col['column_name']: col for col in db_columns}

    # Get model fields
    model_fields = model.model_fields
    model_column_names = set(model_fields.keys())

    # Find missing fields
    missing_in_model = db_column_names - model_column_names
    missing_in_db = model_column_names - db_column_names

    # Find type mismatches
    type_mismatches = []
    for field_name, field_info in model_fields.items():
        if field_name in db_column_info:
            db_col = db_column_info[field_name]
            model_type = str(field_info.annotation)
            db_type = db_col['data_type']

            # Simple type mapping for comparison
            type_map = {
                'int': ['integer', 'bigint', 'smallint'],
                'float': ['numeric', 'double precision', 'real'],
                'str': ['character varying', 'text', 'jsonb'],
                'bool': ['boolean'],
                'datetime': ['timestamp with time zone', 'timestamp without time zone', 'date']
            }

            # Check if types are compatible
            compatible = False
            for model_type_key, db_types in type_map.items():
                if model_type_key in model_type.lower() and db_type in db_types:
                    compatible = True
                    break

            if not compatible:
                type_mismatches.append({
                    'field': field_name,
                    'model_type': model_type,
                    'db_type': db_type,
                    'nullable_model': field_info.is_required() is False,
                    'nullable_db': db_col['is_nullable'] == 'YES'
                })

    # Report findings
    validation_results = {
        'model': model.__name__,
        'table': f"{schema}.{table}",
        'total_db_columns': len(db_column_names),
        'total_model_fields': len(model_column_names),
        'missing_in_model': sorted(list(missing_in_model)),
        'missing_in_db': sorted(list(missing_in_db)),
        'type_mismatches': type_mismatches,
        'status': '✅ Valid' if not (missing_in_model or missing_in_db or type_mismatches) else '⚠️ Needs Review'
    }

    # Print summary
    print(f"  Database columns: {len(db_column_names)}")
    print(f"  Model fields: {len(model_column_names)}")

    if missing_in_model:
        print(f"  ⚠️ Missing in model: {sorted(list(missing_in_model))}")
    if missing_in_db:
        print(f"  ⚠️ Missing in database: {sorted(list(missing_in_db))}")
    if type_mismatches:
        print(f"  ⚠️ Type mismatches: {len(type_mismatches)}")
        for mismatch in type_mismatches:
            print(f"    - {mismatch['field']}: Model={mismatch['model_type']}, DB={mismatch['db_type']}")

    print(f"  Status: {validation_results['status']}")
    return validation_results

def validate_all_models():
    """Validate all Pydantic models against their corresponding database tables"""
    print("🚀 Starting Pydantic model validation...")

    validation_report = {
        "generated_at": datetime.now().isoformat(),
        "models_validated": [],
        "summary": {
            "total_models": 0,
            "valid_models": 0,
            "models_needing_review": 0,
            "total_missing_in_models": 0,
            "total_missing_in_db": 0,
            "total_type_mismatches": 0
        }
    }

    # Define model to table mappings
    model_mappings = [
        # Activity models
        (Activity, 'activities', 'activities'),
        (HeartRate, 'health', 'heartrate_raw'),
        (SleepData, 'health', 'sleep_totals'),
        (Track, 'music', 'all_tracks'),
        (Playlist, 'music', 'playlist_config'),
        (TaskExecution, 'logging', 'task_executions')
    ]

    for model, schema, table in model_mappings:
        try:
            result = validate_model_against_table(model, schema, table)
            validation_report["models_validated"].append(result)

            # Update summary
            validation_report["summary"]["total_models"] += 1
            if result['status'] == '✅ Valid':
                validation_report["summary"]["valid_models"] += 1
            else:
                validation_report["summary"]["models_needing_review"] += 1

            validation_report["summary"]["total_missing_in_models"] += len(result['missing_in_model'])
            validation_report["summary"]["total_missing_in_db"] += len(result['missing_in_db'])
            validation_report["summary"]["total_type_mismatches"] += len(result['type_mismatches'])

        except Exception as e:
            print(f"❌ Error validating {model.__name__}: {e}")
            validation_report["models_validated"].append({
                'model': model.__name__,
                'table': f"{schema}.{table}",
                'error': str(e),
                'status': '❌ Error'
            })

    # Generate markdown report
    md_report = generate_markdown_report(validation_report)

    # Save reports
    save_validation_reports(validation_report, md_report)

    print(f"\n📊 Validation complete!")
    print(f"  Total models: {validation_report['summary']['total_models']}")
    print(f"  Valid models: {validation_report['summary']['valid_models']}")
    print(f"  Models needing review: {validation_report['summary']['models_needing_review']}")
    print(f"  Missing in models: {validation_report['summary']['total_missing_in_models']}")
    print(f"  Missing in DB: {validation_report['summary']['total_missing_in_db']}")
    print(f"  Type mismatches: {validation_report['summary']['total_type_mismatches']}")

    return validation_report

def generate_markdown_report(validation_report):
    """Generate a human-readable markdown report"""
    md_content = f"# Pydantic Models Validation Report\n\n"
    md_content += f"Generated: {validation_report['generated_at']}\n\n"

    # Summary
    md_content += "## Summary\n\n"
    md_content += f"- **Total Models Validated:** {validation_report['summary']['total_models']}\n"
    md_content += f"- **Valid Models:** {validation_report['summary']['valid_models']}\n"
    md_content += f"- **Models Needing Review:** {validation_report['summary']['models_needing_review']}\n"
    md_content += f"- **Missing in Models:** {validation_report['summary']['total_missing_in_models']}\n"
    md_content += f"- **Missing in Database:** {validation_report['summary']['total_missing_in_db']}\n"
    md_content += f"- **Type Mismatches:** {validation_report['summary']['total_type_mismatches']}\n\n"

    # Detailed results
    md_content += "## Detailed Validation Results\n\n"

    for result in validation_report["models_validated"]:
        if 'error' in result:
            md_content += f"### {result['model']} ❌\n\n"
            md_content += f"- **Error:** {result['error']}\n\n"
        else:
            md_content += f"### {result['model']} {result['status']}\n\n"
            md_content += f"- **Table:** `{result['table']}`\n"
            md_content += f"- **Database Columns:** {result['total_db_columns']}\n"
            md_content += f"- **Model Fields:** {result['total_model_fields']}\n"

            if result['missing_in_model']:
                md_content += "- **Missing in Model:**\n"
                for field in result['missing_in_model']:
                    md_content += f"  - `{field}`\n"

            if result['missing_in_db']:
                md_content += "- **Missing in Database:**\n"
                for field in result['missing_in_db']:
                    md_content += f"  - `{field}`\n"

            if result['type_mismatches']:
                md_content += "- **Type Mismatches:**\n"
                for mismatch in result['type_mismatches']:
                    md_content += f"  - `{mismatch['field']}`: Model `{mismatch['model_type']}` vs DB `{mismatch['db_type']}`\n"

            md_content += "\n"

    return md_content

def save_validation_reports(validation_report, md_report):
    """Save validation reports to files"""
    # Save JSON report
    json_file = "memory-bank/pydantic_validation_report.json"
    os.makedirs(os.path.dirname(json_file), exist_ok=True)
    with open(json_file, 'w') as f:
        json.dump(validation_report, f, indent=2)
    print(f"Validation report saved to {json_file}")

    # Save Markdown report
    md_file = "memory-bank/pydantic_validation_report.md"
    os.makedirs(os.path.dirname(md_file), exist_ok=True)
    with open(md_file, 'w', encoding='utf-8') as f:
        f.write(md_report)
    print(f"Validation report (Markdown) saved to {md_file}")

if __name__ == "__main__":
    validation_report = validate_all_models()