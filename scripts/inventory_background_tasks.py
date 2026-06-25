#!/usr/bin/env python3
"""
Background Tasks Inventory Generator
Inventories all background tasks (cron, agents, systemd timers) and their triggers
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

from backend_functions.database_functions import sql_to_dict, one_sql_result

def get_all_tasks():
    """Get all tasks from the database"""
    sql = """
    SELECT *
    FROM tasks.vw_task_info
    ORDER BY task_name
    """
    return sql_to_dict(sql)

def get_task_configurations():
    """Get task configurations"""
    sql = """
    SELECT *
    FROM tasks.task_config
    ORDER BY task_name
    """
    return sql_to_dict(sql)

def get_systemd_services():
    """Get systemd service information"""
    # This would normally query the system, but we'll document what we know
    services = [
        {
            'service_name': 'pifitness-streamlit.service',
            'description': 'Streamlit legacy application',
            'status': 'active',
            'trigger': 'system boot',
            'file_location': '/etc/systemd/system/pifitness-streamlit.service'
        },
        {
            'service_name': 'pifitness-fastapi.service',
            'description': 'FastAPI backend application',
            'status': 'prepared',
            'trigger': 'system boot',
            'file_location': '/etc/systemd/system/pifitness-fastapi.service'
        }
    ]
    return services

def get_cron_jobs():
    """Get cron job information"""
    # This would normally query the system cron, but we'll document what we know
    cron_jobs = [
        {
            'job_name': 'hourly_agent',
            'description': 'Hourly data synchronization agent',
            'schedule': 'Every hour',
            'command': 'python /home/god/PiFitness/agents/agent_hourly.py',
            'status': 'active',
            'file_location': '/home/god/PiFitness/agents/agent_hourly.py'
        }
    ]
    return cron_jobs

def get_agent_scripts():
    """Get agent script information"""
    agents_dir = os.path.join(project_root, 'agents')
    agents = []

    if os.path.exists(agents_dir):
        for filename in os.listdir(agents_dir):
            if filename.endswith('.py'):
                filepath = os.path.join(agents_dir, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        content = f.read()
                        lines = content.split('\n')

                    # Extract basic info
                    agent_info = {
                        'script_name': filename,
                        'file_path': filepath,
                        'description': '',
                        'main_function': '',
                        'dependencies': []
                    }

                    # Look for description in comments
                    for line in lines[:10]:  # Check first 10 lines
                        if line.strip().startswith('#') and 'agent' in line.lower():
                            agent_info['description'] = line.strip()[1:].strip()
                            break

                    # Look for main function call
                    for line in lines:
                        if 'if __name__ == "__main__":' in line:
                            # Look for the function call in the next few lines
                            for i in range(1, 5):
                                if len(lines) > lines.index(line) + i:
                                    next_line = lines[lines.index(line) + i].strip()
                                    if next_line and not next_line.startswith('#'):
                                        agent_info['main_function'] = next_line.split('(')[0]
                                        break
                            break

                    # Look for imports
                    for line in lines:
                        if line.strip().startswith('import ') or line.strip().startswith('from '):
                            agent_info['dependencies'].append(line.strip())

                    agents.append(agent_info)
                except Exception as e:
                    print(f"Error reading agent {filename}: {e}")

    return agents

def inventory_background_tasks():
    """Generate comprehensive inventory of background tasks"""
    inventory = {
        "generated_at": datetime.now().isoformat(),
        "database_tasks": {},
        "systemd_services": {},
        "cron_jobs": {},
        "agent_scripts": {},
        "summary": {
            "total_database_tasks": 0,
            "total_systemd_services": 0,
            "total_cron_jobs": 0,
            "total_agent_scripts": 0,
            "active_tasks": 0,
            "inactive_tasks": 0
        }
    }

    # Get database tasks
    print("Inventorying database tasks...")
    tasks = get_all_tasks()
    inventory["summary"]["total_database_tasks"] = len(tasks)

    for task in tasks:
        task_id = task['task_id']
        task_name = task['task_name']
        print(f"  Processing task: {task_name}")

        task_info = {
            "task_id": task_id,
            "task_name": task_name,
            "api_service_name": task.get('api_service_name'),
            "should_execute": task.get('should_execute', False),
            "next_planned_execution_utc": str(task.get('next_planned_execution_utc')) if task.get('next_planned_execution_utc') else None,
            "last_execution_utc": str(task.get('last_execution_utc')) if task.get('last_execution_utc') else None,
            "last_execution_status": task.get('last_execution_status'),
            "execution_frequency": task.get('execution_frequency'),
            "task_function": task.get('task_function'),
            "api_service_function": task.get('api_service_function'),
            "status": "active" if task.get('should_execute', False) else "inactive"
        }

        inventory["database_tasks"][task_name] = task_info

        # Update summary
        if task_info["status"] == "active":
            inventory["summary"]["active_tasks"] += 1
        else:
            inventory["summary"]["inactive_tasks"] += 1

    # Get systemd services
    print("Inventorying systemd services...")
    services = get_systemd_services()
    inventory["summary"]["total_systemd_services"] = len(services)

    for service in services:
        service_name = service['service_name']
        inventory["systemd_services"][service_name] = service

    # Get cron jobs
    print("Inventorying cron jobs...")
    cron_jobs = get_cron_jobs()
    inventory["summary"]["total_cron_jobs"] = len(cron_jobs)

    for job in cron_jobs:
        job_name = job['job_name']
        inventory["cron_jobs"][job_name] = job

    # Get agent scripts
    print("Inventorying agent scripts...")
    agents = get_agent_scripts()
    inventory["summary"]["total_agent_scripts"] = len(agents)

    for agent in agents:
        script_name = agent['script_name']
        inventory["agent_scripts"][script_name] = agent

    return inventory

def save_inventory(inventory):
    """Save the inventory to files"""
    # Save JSON version
    json_file = "memory-bank/background_tasks_inventory.json"
    os.makedirs(os.path.dirname(json_file), exist_ok=True)
    with open(json_file, 'w') as f:
        json.dump(inventory, f, indent=2)
    print(f"Background tasks inventory saved to {json_file}")

    # Generate markdown version
    md_content = generate_markdown_inventory(inventory)
    md_file = "memory-bank/background_tasks_inventory.md"
    os.makedirs(os.path.dirname(md_file), exist_ok=True)
    with open(md_file, 'w', encoding='utf-8') as f:
        f.write(md_content)
    print(f"Background tasks inventory summary saved to {md_file}")

def generate_markdown_inventory(inventory):
    """Generate human-readable markdown documentation"""
    md_content = f"# Background Tasks Inventory\n\n"
    md_content += f"Generated: {inventory['generated_at']}\n\n"

    # Summary section
    md_content += "## Summary\n\n"
    md_content += f"- **Database Tasks:** {inventory['summary']['total_database_tasks']}\n"
    md_content += f"- **Active Tasks:** {inventory['summary']['active_tasks']}\n"
    md_content += f"- **Inactive Tasks:** {inventory['summary']['inactive_tasks']}\n"
    md_content += f"- **Systemd Services:** {inventory['summary']['total_systemd_services']}\n"
    md_content += f"- **Cron Jobs:** {inventory['summary']['total_cron_jobs']}\n"
    md_content += f"- **Agent Scripts:** {inventory['summary']['total_agent_scripts']}\n\n"

    # Database tasks section
    md_content += "## Database Tasks\n\n"

    # Group by status
    active_tasks = []
    inactive_tasks = []

    for task_name, task_info in inventory['database_tasks'].items():
        if task_info['status'] == 'active':
            active_tasks.append((task_name, task_info))
        else:
            inactive_tasks.append((task_name, task_info))

    if active_tasks:
        md_content += "### Active Tasks\n\n"
        for task_name, task_info in sorted(active_tasks, key=lambda x: x[1]['task_name']):
            md_content += f"#### {task_name}\n\n"
            md_content += f"- **ID:** {task_info['task_id']}\n"
            md_content += f"- **API Service:** {task_info['api_service_name']}\n"
            md_content += f"- **Function:** `{task_info['task_function']}`\n"
            md_content += f"- **Frequency:** {task_info['execution_frequency']}\n"
            md_content += f"- **Next Execution:** {task_info['next_planned_execution_utc']}\n"
            md_content += f"- **Last Execution:** {task_info['last_execution_utc']}\n"
            md_content += f"- **Last Status:** {task_info['last_execution_status']}\n\n"

    if inactive_tasks:
        md_content += "### Inactive Tasks\n\n"
        for task_name, task_info in sorted(inactive_tasks, key=lambda x: x[1]['task_name']):
            md_content += f"#### {task_name}\n\n"
            md_content += f"- **ID:** {task_info['task_id']}\n"
            md_content += f"- **API Service:** {task_info['api_service_name']}\n"
            md_content += f"- **Function:** `{task_info['task_function']}`\n"
            md_content += f"- **Frequency:** {task_info['execution_frequency']}\n\n"

    # Systemd services section
    if inventory['systemd_services']:
        md_content += "## Systemd Services\n\n"
        for service_name, service_info in inventory['systemd_services'].items():
            md_content += f"#### {service_name}\n\n"
            md_content += f"- **Description:** {service_info['description']}\n"
            md_content += f"- **Status:** {service_info['status']}\n"
            md_content += f"- **Trigger:** {service_info['trigger']}\n"
            md_content += f"- **File:** `{service_info['file_location']}`\n\n"

    # Cron jobs section
    if inventory['cron_jobs']:
        md_content += "## Cron Jobs\n\n"
        for job_name, job_info in inventory['cron_jobs'].items():
            md_content += f"#### {job_name}\n\n"
            md_content += f"- **Description:** {job_info['description']}\n"
            md_content += f"- **Schedule:** {job_info['schedule']}\n"
            md_content += f"- **Command:** `{job_info['command']}`\n"
            md_content += f"- **Status:** {job_info['status']}\n"
            md_content += f"- **File:** `{job_info['file_location']}`\n\n"

    # Agent scripts section
    if inventory['agent_scripts']:
        md_content += "## Agent Scripts\n\n"
        for script_name, script_info in inventory['agent_scripts'].items():
            md_content += f"#### {script_name}\n\n"
            md_content += f"- **Description:** {script_info['description']}\n"
            md_content += f"- **Main Function:** `{script_info['main_function']}`\n"
            md_content += f"- **File:** `{script_info['file_path']}`\n"

            if script_info['dependencies']:
                md_content += "- **Dependencies:**\n"
                for dep in script_info['dependencies']:
                    md_content += f"  - `{dep}`\n"

            md_content += "\n"

    return md_content

if __name__ == "__main__":
    print("Inventorying background tasks...")
    inventory = inventory_background_tasks()
    save_inventory(inventory)
    print("Background tasks inventory generation complete!")