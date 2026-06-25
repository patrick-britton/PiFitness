#!/usr/bin/env python3
"""
Data Flow Diagrams Generator
Creates Mermaid.js diagrams for Activities, Health, Music, and Admin modules
"""

import os
import sys
import json
from datetime import datetime

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

def generate_activities_data_flow():
    """Generate data flow diagram for Activities module"""
    diagram = """## Activities Data Flow

```mermaid
flowchart TD
    %% Activity Data Flow
    A[Garmin Connect API] -->|Raw JSON| B[staging.garmin_activity_raw]
    B -->|staging.activities_flatten| C[activities.activity_details]
    C -->|staging.derive_activity_elevations| D[activities.activity_elevations]
    C -->|staging.flatten_activity_metrics| E[activities.activity_metrics]
    C -->|Segment Matching| F[activities.segment_matches]

    %% User Interaction
    G[User via Streamlit] -->|Request Activity List| H[backend_functions.running_functions.get_activities]
    H -->|SQL Query| C
    C -->|Activity Data| G

    %% Segment Creation
    G -->|Create Segment Request| I[backend_functions.segment_creation.create_segment_from_activity]
    I -->|staging.create_segment_from_activity| J[activities.segments]
    J -->|Segment Data| G

    %% Data Processing Pipeline
    K[Hourly Agent] -->|Trigger| L[ultimate_task_executioner]
    L -->|Execute| B
    L -->|Execute| C
    L -->|Execute| D

    style A fill:#f9f,stroke:#333
    style G fill:#ccf,stroke:#333
    style K fill:#f96,stroke:#333
```

### Activities Data Flow Description

1. **Data Ingestion**: Garmin Connect API provides raw activity data in JSON format, stored in `staging.garmin_activity_raw`
2. **Processing Pipeline**: Stored procedures flatten and normalize the data into relational tables
3. **User Access**: Streamlit frontend queries processed data via backend functions
4. **Segment Analysis**: Users can create GPS segments from activities for performance comparison
5. **Automated Processing**: Hourly agent triggers the processing pipeline for new data
"""
    return diagram

def generate_health_data_flow():
    """Generate data flow diagram for Health module"""
    diagram = """## Health Data Flow

```mermaid
flowchart TD
    %% Health Data Flow
    A[Garmin Connect API] -->|Raw JSON| B[staging.garmin_health_raw]
    B -->|staging.heartrate_flatten| C[health.heartrate]
    B -->|staging.sleep_flatten| D[health.sleep]
    B -->|staging.body_battery_flatten| E[health.body_battery]
    B -->|staging.stress_flatten| F[health.stress]
    B -->|staging.hrv_flatten| G[health.heartrate_variation]

    %% User Interaction
    H[User via Streamlit] -->|Request Health Dashboard| I[backend_functions.health_module.render_health_module]
    I -->|Query Heartrate| C
    I -->|Query Sleep| D
    I -->|Query Body Battery| E
    C -->|Heartrate Data| H
    D -->|Sleep Data| H
    E -->|Body Battery Data| H

    %% Data Processing Pipeline
    J[Hourly Agent] -->|Trigger Health Sync| K[ultimate_task_executioner]
    K -->|Execute| B
    K -->|Execute| C
    K -->|Execute| D

    style A fill:#f9f,stroke:#333
    style H fill:#ccf,stroke:#333
    style J fill:#f96,stroke:#333
```

### Health Data Flow Description

1. **Data Ingestion**: Garmin Connect API provides health metrics in JSON format, stored in `staging.garmin_health_raw`
2. **Metric Processing**: Separate stored procedures process different health metrics (heartrate, sleep, body battery, etc.)
3. **User Access**: Health module queries processed metrics and displays them in the Streamlit dashboard
4. **Automated Processing**: Hourly agent ensures health data is regularly updated
"""
    return diagram

def generate_music_data_flow():
    """Generate data flow diagram for Music module"""
    diagram = """## Music Data Flow

```mermaid
flowchart TD
    %% Music Data Flow
    A[Spotify API] -->|Raw JSON| B[staging.spotify_raw]
    B -->|staging.listen_history_flatten| C[music.listening_history]
    B -->|staging.flatten_playlist_details| D[music.playlist_details]
    B -->|staging.flatten_track_inserts| E[music.tracks]

    %% ELO Processing
    F[ELO Calculation] -->|staging.track_id_search_flatten| G[music.track_recommendations]
    G -->|Update Ratings| E

    %% User Interaction
    H[User via Streamlit] -->|Request Music Dashboard| I[backend_functions.music_module.render_music]
    I -->|Query Playlists| D
    I -->|Query Tracks| E
    I -->|Query Recommendations| G
    D -->|Playlist Data| H
    E -->|Track Data| H
    G -->|Recommendation Data| H

    %% Smart Shuffle
    H -->|Request Smart Shuffle| J[backend_functions.music_functions.auto_shuffle_playlists]
    J -->|Generate Playlist| D
    J -->|Use ELO Ratings| G

    %% Data Processing Pipeline
    K[Hourly Agent] -->|Trigger Music Sync| L[ultimate_task_executioner]
    L -->|Execute| B
    L -->|Execute| C
    L -->|Execute| D

    style A fill:#f9f,stroke:#333
    style H fill:#ccf,stroke:#333
    style K fill:#f96,stroke:#333
    style F fill:#9f9,stroke:#333
```

### Music Data Flow Description

1. **Data Ingestion**: Spotify API provides listening history and playlist data in JSON format
2. **Processing Pipeline**: Stored procedures normalize data into relational music tables
3. **ELO System**: Track recommendations use ELO rating system for smart playlist generation
4. **User Interaction**: Music module displays playlists, tracks, and generates smart shuffles
5. **Automated Processing**: Hourly agent keeps music data synchronized
"""
    return diagram

def generate_admin_data_flow():
    """Generate data flow diagram for Admin module"""
    diagram = """## Admin Data Flow

```mermaid
flowchart TD
    %% Admin Data Flow
    A[User via Streamlit] -->|Request Admin Dashboard| B[backend_functions.admin_module.render_admin_module]
    B -->|Query Task Status| C[tasks.task_config]
    B -->|Query Execution Logs| D[logging.task_executions]
    B -->|Query DB Stats| E[logging.db_stats]

    %% Task Management
    A -->|Trigger Task Execution| F[backend_functions.admin_functions.execute_task]
    F -->|Update Status| C
    F -->|Log Execution| D

    %% Database Maintenance
    G[Hourly Agent] -->|Run Maintenance| H[backend_functions.backend_tasks.nightly_maintenance]
    H -->|Vacuum/Analyze| I[PostgreSQL Database]
    H -->|Log Results| E

    %% Backup System
    G -->|Run Backup| J[backend_functions.backend_tasks.backup_database]
    J -->|Create Dump| K[PG Backup Files]
    J -->|Log Backup| D

    style A fill:#ccf,stroke:#333
    style G fill:#f96,stroke:#333
    style I fill:#9f9,stroke:#333
    style K fill:#ff9,stroke:#333
```

### Admin Data Flow Description

1. **Dashboard Access**: Admin module provides overview of task status, execution logs, and database statistics
2. **Task Management**: Users can manually trigger task execution and monitor results
3. **Database Maintenance**: Automated nightly maintenance includes vacuuming, analyzing, and reindexing
4. **Backup System**: Regular database backups are created and managed
5. **Monitoring**: All operations are logged for auditing and troubleshooting
"""
    return diagram

def generate_overall_architecture_diagram():
    """Generate overall system architecture diagram"""
    diagram = """## Overall System Architecture

```mermaid
flowchart TD
    %% External Systems
    subgraph External[External Systems]
        A[Garmin Connect API]
        B[Spotify API]
        C[User Browser]
    end

    %% PiFitness System
    subgraph PiFitness[PiFitness System]
        %% Database Layer
        subgraph Database[PostgreSQL/PostGIS]
            D[(personal_fitness DB)]
            E[Staging Schema]
            F[Activities Schema]
            G[Health Schema]
            H[Music Schema]
            I[Logging Schema]
        end

        %% Backend Layer
        subgraph Backend[Python Backend]
            J[FastAPI Server]
            K[Backend Functions]
            L[Stored Procedures]
            M[Hourly Agent]
        end

        %% Frontend Layer
        subgraph Frontend[Streamlit Frontend]
            N[Home Module]
            O[Health Module]
            P[Music Module]
            Q[Admin Module]
            R[Running Module]
        end
    end

    %% Data Flows
    A -->|JSON Data| E
    B -->|JSON Data| E
    C -->|HTTP Requests| J
    J -->|API Responses| C
    J -->|DB Queries| D
    K -->|Execute SQL| D
    L -->|Process Data| D
    M -->|Trigger Tasks| K
    E -->|Flatten Data| L

    %% Module Dependencies
    N -->|Backend Calls| K
    O -->|Backend Calls| K
    P -->|Backend Calls| K
    Q -->|Backend Calls| K
    R -->|Backend Calls| K

    %% System Boundaries
    style External fill:#f9f,stroke:#333
    style Database fill:#9f9,stroke:#333
    style Backend fill:#f96,stroke:#333
    style Frontend fill:#ccf,stroke:#333
```

### Overall Architecture Description

1. **External Systems**: Garmin Connect and Spotify APIs provide raw data; users interact via browser
2. **Database Layer**: PostgreSQL with PostGIS stores all data in organized schemas
3. **Backend Layer**: FastAPI server, backend functions, and stored procedures handle data processing
4. **Frontend Layer**: Streamlit modules provide user interface for different functionality areas
5. **Data Flow**: External APIs → Staging Tables → Processed Tables → Backend → Frontend → User
"""
    return diagram

def generate_data_flow_diagrams():
    """Generate all data flow diagrams"""
    diagrams = {
        "generated_at": datetime.now().isoformat(),
        "diagrams": {
            "activities": generate_activities_data_flow(),
            "health": generate_health_data_flow(),
            "music": generate_music_data_flow(),
            "admin": generate_admin_data_flow(),
            "architecture": generate_overall_architecture_diagram()
        },
        "summary": {
            "total_diagrams": 5,
            "diagram_types": ["Activities", "Health", "Music", "Admin", "Overall Architecture"],
            "format": "Mermaid.js",
            "purpose": "Documentation and system understanding"
        }
    }

    return diagrams

def save_diagrams(diagrams):
    """Save the diagrams to files"""
    # Save JSON version
    json_file = "memory-bank/data_flow_diagrams.json"
    os.makedirs(os.path.dirname(json_file), exist_ok=True)
    with open(json_file, 'w') as f:
        json.dump(diagrams, f, indent=2)
    print(f"Data flow diagrams saved to {json_file}")

    # Generate markdown version
    md_content = generate_markdown_diagrams(diagrams)
    md_file = "memory-bank/data_flow_diagrams.md"
    os.makedirs(os.path.dirname(md_file), exist_ok=True)
    with open(md_file, 'w', encoding='utf-8') as f:
        f.write(md_content)
    print(f"Data flow diagrams summary saved to {md_file}")

def generate_markdown_diagrams(diagrams):
    """Generate human-readable markdown documentation with diagrams"""
    md_content = f"# Data Flow Diagrams\n\n"
    md_content += f"Generated: {diagrams['generated_at']}\n\n"

    # Summary section
    md_content += "## Summary\n\n"
    md_content += f"- **Total Diagrams:** {diagrams['summary']['total_diagrams']}\n"
    md_content += f"- **Diagram Types:** {', '.join(diagrams['summary']['diagram_types'])}\n"
    md_content += f"- **Format:** {diagrams['summary']['format']}\n"
    md_content += f"- **Purpose:** {diagrams['summary']['purpose']}\n\n"

    md_content += "## How to View Diagrams\n\n"
    md_content += "These diagrams are in Mermaid.js format. You can:\n"
    md_content += "- View them in any Markdown viewer that supports Mermaid (like GitHub, VS Code with Mermaid plugin)\n"
    md_content += "- Copy the Mermaid code into [Mermaid Live Editor](https://mermaid.live/)\n"
    md_content += "- Use a Mermaid renderer in your documentation system\n\n"

    # Individual diagrams
    for diagram_name, diagram_content in diagrams['diagrams'].items():
        md_content += diagram_content
        md_content += "\n---\n\n"

    return md_content

if __name__ == "__main__":
    print("Generating data flow diagrams...")
    diagrams = generate_data_flow_diagrams()
    save_diagrams(diagrams)
    print("Data flow diagrams generation complete!")