#!/usr/bin/env python3
"""
Streamlit to Backend Mapping Generator
Maps each Streamlit page to its backend function calls and data dependencies
"""

import os
import sys
import json
import ast
import re
from datetime import datetime

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

def analyze_python_file(filepath):
    """Analyze a Python file to extract function calls and imports"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # Extract imports
        imports = []
        for line in content.split('\n'):
            if line.strip().startswith('from backend_functions') or line.strip().startswith('from frontend_functions'):
                imports.append(line.strip())

        # Extract function calls using regex
        function_calls = []
        pattern = r'\b(from backend_functions|from frontend_functions)\s+import\s+([^#\n]+)'
        matches = re.findall(pattern, content)
        for match in matches:
            module_type = match[0].replace('from ', '')
            functions = [f.strip() for f in match[1].split(',')]
            for func in functions:
                if func and func != '*':
                    function_calls.append(f"{module_type}.{func}")

        # Extract database function calls
        db_calls = []
        db_pattern = r'\b(qec|one_sql_result|sql_to_dict|sql_to_list|get_conn)\s*\('
        db_matches = re.findall(db_pattern, content)
        for match in db_matches:
            db_calls.append(match)

        return {
            'imports': imports,
            'function_calls': list(set(function_calls)),
            'db_calls': list(set(db_calls))
        }
    except Exception as e:
        print(f"Error analyzing {filepath}: {e}")
        return {
            'imports': [],
            'function_calls': [],
            'db_calls': []
        }

def map_streamlit_modules():
    """Map Streamlit modules to their backend dependencies"""
    modules = {
        'home': {
            'file': 'frontend_functions/homepage.py',
            'render_function': 'render_homepage'
        },
        'admin': {
            'file': 'frontend_functions/admin_module.py',
            'render_function': 'render_admin_module'
        },
        'music': {
            'file': 'frontend_functions/music_module.py',
            'render_function': 'render_music'
        },
        'health': {
            'file': 'frontend_functions/health_module.py',
            'render_function': 'render_health_module'
        },
        'running': {
            'file': 'frontend_functions/running_module.py',
            'render_function': 'render_running_module'
        },
        'food': {
            'file': 'frontend_functions/running_module.py',  # Placeholder - not yet built
            'render_function': None
        }
    }

    mapping = {
        "generated_at": datetime.now().isoformat(),
        "modules": {},
        "summary": {
            "total_modules": len(modules),
            "backend_functions_used": set(),
            "database_calls": set(),
            "frontend_helpers": set()
        }
    }

    for module_name, module_info in modules.items():
        print(f"Analyzing {module_name} module...")
        filepath = os.path.join(project_root, module_info['file'])

        if not os.path.exists(filepath):
            print(f"  File not found: {filepath}")
            mapping["modules"][module_name] = {
                "status": "not_found",
                "file": module_info['file'],
                "render_function": module_info['render_function']
            }
            continue

        analysis = analyze_python_file(filepath)

        # Categorize the function calls
        backend_functions = [f for f in analysis['function_calls'] if f.startswith('backend_functions.')]
        frontend_helpers = [f for f in analysis['function_calls'] if f.startswith('frontend_functions.')]

        module_mapping = {
            "status": "analyzed",
            "file": module_info['file'],
            "render_function": module_info['render_function'],
            "imports": analysis['imports'],
            "backend_functions": backend_functions,
            "frontend_helpers": frontend_helpers,
            "database_calls": analysis['db_calls'],
            "dependencies": {
                "backend_modules": set(),
                "database_tables": set(),
                "external_apis": set()
            }
        }

        # Infer dependencies based on function names
        for func in backend_functions:
            if 'database' in func:
                module_mapping["dependencies"]["backend_modules"].add("database_functions")
            elif 'music' in func:
                module_mapping["dependencies"]["backend_modules"].add("music_functions")
            elif 'activity' in func or 'running' in func:
                module_mapping["dependencies"]["backend_modules"].add("running_functions")
            elif 'health' in func:
                module_mapping["dependencies"]["backend_modules"].add("health_functions")
            elif 'admin' in func or 'task' in func:
                module_mapping["dependencies"]["backend_modules"].add("admin_functions")
            elif 'service' in func or 'credential' in func:
                module_mapping["dependencies"]["backend_modules"].add("service_logins")

        # Convert sets to lists for JSON serialization
        module_mapping["dependencies"]["backend_modules"] = sorted(list(module_mapping["dependencies"]["backend_modules"]))
        module_mapping["dependencies"]["database_tables"] = sorted(list(module_mapping["dependencies"]["database_tables"]))
        module_mapping["dependencies"]["external_apis"] = sorted(list(module_mapping["dependencies"]["external_apis"]))

        mapping["modules"][module_name] = module_mapping

        # Update summary statistics
        mapping["summary"]["backend_functions_used"].update(backend_functions)
        mapping["summary"]["database_calls"].update(analysis['db_calls'])
        mapping["summary"]["frontend_helpers"].update(frontend_helpers)

    # Convert sets to lists in summary
    mapping["summary"]["backend_functions_used"] = sorted(list(mapping["summary"]["backend_functions_used"]))
    mapping["summary"]["database_calls"] = sorted(list(mapping["summary"]["database_calls"]))
    mapping["summary"]["frontend_helpers"] = sorted(list(mapping["summary"]["frontend_helpers"]))

    return mapping

def save_mapping(mapping):
    """Save the mapping to files"""
    # Save JSON version
    json_file = "memory-bank/streamlit_backend_mapping.json"
    os.makedirs(os.path.dirname(json_file), exist_ok=True)
    with open(json_file, 'w') as f:
        json.dump(mapping, f, indent=2)
    print(f"Streamlit backend mapping saved to {json_file}")

    # Generate markdown version
    md_content = generate_markdown_mapping(mapping)
    md_file = "memory-bank/streamlit_backend_mapping.md"
    os.makedirs(os.path.dirname(md_file), exist_ok=True)
    with open(md_file, 'w', encoding='utf-8') as f:
        f.write(md_content)
    print(f"Streamlit backend mapping summary saved to {md_file}")

def generate_markdown_mapping(mapping):
    """Generate human-readable markdown documentation"""
    md_content = f"# Streamlit to Backend Mapping\n\n"
    md_content += f"Generated: {mapping['generated_at']}\n\n"

    # Summary section
    md_content += "## Summary\n\n"
    md_content += f"- **Total Modules:** {mapping['summary']['total_modules']}\n"
    md_content += f"- **Backend Functions Used:** {len(mapping['summary']['backend_functions_used'])}\n"
    md_content += f"- **Database Calls:** {len(mapping['summary']['database_calls'])}\n"
    md_content += f"- **Frontend Helpers:** {len(mapping['summary']['frontend_helpers'])}\n\n"

    # Detailed module mapping
    md_content += "## Module Details\n\n"

    for module_name, module_data in mapping['modules'].items():
        md_content += f"### {module_name.capitalize()} Module\n\n"

        if module_data['status'] == "not_found":
            md_content += f"- **Status:** ❌ Not Found\n"
            md_content += f"- **File:** `{module_data['file']}`\n"
            continue

        md_content += f"- **Status:** ✅ Analyzed\n"
        md_content += f"- **File:** `{module_data['file']}`\n"
        md_content += f"- **Render Function:** `{module_data['render_function']}`\n"

        if module_data['backend_functions']:
            md_content += "- **Backend Functions:**\n"
            for func in module_data['backend_functions']:
                md_content += f"  - `{func}`\n"

        if module_data['frontend_helpers']:
            md_content += "- **Frontend Helpers:**\n"
            for helper in module_data['frontend_helpers']:
                md_content += f"  - `{helper}`\n"

        if module_data['database_calls']:
            md_content += "- **Database Calls:**\n"
            for call in module_data['database_calls']:
                md_content += f"  - `{call}`\n"

        if module_data['dependencies']['backend_modules']:
            md_content += "- **Backend Dependencies:**\n"
            for module in module_data['dependencies']['backend_modules']:
                md_content += f"  - `{module}`\n"

        md_content += "\n"

    # Data flow section
    md_content += "## Data Flow Analysis\n\n"

    # Group by backend module
    backend_modules = {}
    for module_name, module_data in mapping['modules'].items():
        if module_data['status'] == "analyzed":
            for backend_module in module_data['dependencies']['backend_modules']:
                if backend_module not in backend_modules:
                    backend_modules[backend_module] = []
                backend_modules[backend_module].append(module_name)

    for backend_module, modules in backend_modules.items():
        md_content += f"### {backend_module}\n\n"
        md_content += f"Used by: {', '.join([m.capitalize() for m in modules])}\n\n"

    return md_content

if __name__ == "__main__":
    print("Mapping Streamlit modules to backend dependencies...")
    mapping = map_streamlit_modules()
    save_mapping(mapping)
    print("Streamlit backend mapping generation complete!")