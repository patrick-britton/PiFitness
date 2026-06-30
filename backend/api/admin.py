"""
Admin API Endpoints
=================

FastAPI endpoints for administrative functions (task monitoring, DB stats, etc.).
"""

from fastapi import APIRouter, HTTPException
from typing import Optional, Dict, Any
from pydantic import BaseModel

from backend_functions.queries import (
    get_task_execution_view,
    get_task_scheduling_view,
    get_distinct_task_names,
    get_event_history,
    get_active_db_sessions,
    get_api_service_list,
    get_function_library,
    insert_function_library_entry,
    update_function_library_entry,
    delete_function_library_entry,
    get_credential_requirements,
    upsert_credentials,
    delete_credentials,
    get_log_tables_simple,
    get_log_data_simple,
    insert_api_service,
    delete_api_service,
    delete_task_configuration,
    delete_fact_configuration,
    upsert_fact_configuration,
    update_task_configuration,
    kill_db_session,
    get_task_summary_chart,
    get_db_size_chart,
    get_db_size_breakdown,
)

router = APIRouter(prefix="/api/admin", tags=["admin"])


# Pydantic models for request bodies
class TaskConfigEdit(BaseModel):
    task_id: int
    is_active: bool
    task_frequency: str

class FactConfigUpsert(BaseModel):
    fact_id: Optional[int] = None
    task_id: int
    staging_id: int
    is_active: bool
    custom_params: Optional[Dict[str, Any]] = None

class CredentialUpsert(BaseModel):
    api_service_name: str
    raw_credentials_json_string: str

class APIServiceCreate(BaseModel):
    service_name: str

class FunctionLibraryEntry(BaseModel):
    friendly_name: str
    api_service_name: str
    python_extraction_function: str
    description: Optional[str] = None


@router.get("/tasks")
async def list_tasks():
    """
    List all tasks with execution history.

    Returns:
        List of task execution records
    """
    try:
        tasks = get_task_execution_view()
        return {"data": tasks, "count": len(tasks)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch tasks: {str(e)}",
        )


@router.get("/tasks/schedule")
async def get_task_schedule():
    """
    Get task scheduling configuration.

    Returns:
        List of task configuration records
    """
    try:
        schedule = get_task_scheduling_view()
        return {"data": schedule, "count": len(schedule)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch task schedule: {str(e)}",
        )


@router.get("/tasks/names")
async def get_task_names():
    """
    Get distinct task names.

    Returns:
        List of task names
    """
    try:
        names = get_distinct_task_names()
        return {"data": names, "count": len(names)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch task names: {str(e)}",
        )


@router.post("/tasks/{task_name}/execute")
async def execute_task(task_name: str):
    """
    Trigger execution of a background task.

    Args:
        task_name: The name of the task to execute

    Returns:
        Execution result or error
    """
    try:
        from backend_functions.task_execution import execute_task_by_name
        result = execute_task_by_name(task_name)
        return {"status": "ok", "result": result}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to execute task: {str(e)}",
        )


@router.put("/tasks/{task_id}")
async def update_task_configuration_endpoint(task_id: int, task_config: TaskConfigEdit):
    """
    Update task schedule configuration.

    Args:
        task_id: The ID of the task to update
        task_config: The updated task configuration

    Returns:
        Success message or error
    """
    try:
        result = update_task_configuration(
            task_id=task_id,
            is_active=task_config.is_active,
            task_frequency=task_config.task_frequency
        )
        if result:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to update task configuration: {result}"
            )
        return {"status": "ok", "message": f"Task {task_id} updated successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update task configuration: {str(e)}",
        )


@router.delete("/tasks/schedule/{task_id}")
async def delete_task_configuration_endpoint(task_id: int):
    """
    Delete a task configuration entry.

    Args:
        task_id: The ID of the task configuration to delete

    Returns:
        Success message or error
    """
    try:
        result = delete_task_configuration(task_id)
        if result:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to delete task configuration: {result}"
            )
        return {"status": "ok", "message": f"Task configuration {task_id} deleted successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete task configuration: {str(e)}",
        )


@router.post("/tasks/facts")
async def upsert_fact_configuration_endpoint(fact_config: FactConfigUpsert):
    """
    Add or update a fact configuration record.

    Args:
        fact_config: The fact configuration to insert or update

    Returns:
        Success message or error
    """
    try:
        result = upsert_fact_configuration(
            fields=fact_config.dict(exclude={'fact_id'}),
            is_insert=fact_config.fact_id is None,
            fact_id=fact_config.fact_id
        )
        if result:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to upsert fact configuration: {result}"
            )
        return {"status": "ok", "message": "Fact configuration saved successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to upsert fact configuration: {str(e)}",
        )


@router.delete("/tasks/facts/{fact_id}")
async def delete_fact_configuration_endpoint(fact_id: int):
    """
    Delete a fact configuration entry.

    Args:
        fact_id: The ID of the fact configuration to delete

    Returns:
        Success message or error
    """
    try:
        result = delete_fact_configuration(fact_id)
        if result:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to delete fact configuration: {result}"
            )
        return {"status": "ok", "message": f"Fact configuration {fact_id} deleted successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete fact configuration: {str(e)}",
        )


@router.get("/events")
async def get_events(
    search: Optional[str] = None,
    errors_only: bool = False,
    ignore_skips: bool = False,
    event_type: Optional[str] = None,
    limit: int = 250
):
    """
    Get event history with optional filtering.

    Args:
        search: Text search across event_type, description, error_text
        errors_only: If True, show only error events
        ignore_skips: If True, filter out skipped rows
        event_type: Filter by specific event type
        limit: Maximum number of rows to return (default: 250)

    Returns:
        List of filtered event history records
    """
    try:
        events = get_event_history(
            search_val=search,
            errors_only=errors_only,
            ignore_skips=ignore_skips,
            event_type=event_type,
            limit=limit
        )
        return {"data": events, "count": len(events)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch events: {str(e)}",
        )


@router.get("/db-sessions")
async def get_db_sessions():
    """
    Get active database sessions.

    Returns:
        List of active database session records
    """
    try:
        sessions = get_active_db_sessions()
        return {"data": sessions, "count": len(sessions)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch DB sessions: {str(e)}",
        )


@router.delete("/db-sessions/{pid}")
async def kill_db_session_endpoint(pid: int):
    """
    Terminate a database session by PID.

    Args:
        pid: The process ID to terminate

    Returns:
        Success message or error
    """
    try:
        result = kill_db_session(pid)
        if result:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to kill database session: {result}"
            )
        return {"status": "ok", "message": f"Database session {pid} terminated successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to kill database session: {str(e)}",
        )


@router.get("/services")
async def get_services():
    """
    Get API services list.

    Returns:
        List of API service records
    """
    try:
        services = get_api_service_list()
        return {"data": services, "count": len(services)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch services: {str(e)}",
        )


@router.post("/services")
async def add_service_endpoint(service: APIServiceCreate):
    """
    Add a new API service.

    Args:
        service: The service to add

    Returns:
        Success message or error
    """
    try:
        result = insert_api_service(service.service_name)
        if result:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to add service: {result}"
            )
        return {"status": "ok", "message": f"Service '{service.service_name}' added successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to add service: {str(e)}",
        )


@router.delete("/services/{service_name}")
async def delete_service_endpoint(service_name: str):
    """
    Delete an API service by name.

    Args:
        service_name: The name of the service to delete

    Returns:
        Success message or error
    """
    try:
        result = delete_api_service(service_name)
        if result:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to delete service: {result}"
            )
        return {"status": "ok", "message": f"Service '{service_name}' deleted successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete service: {str(e)}",
        )


@router.get("/functions")
async def get_function_library_endpoint():
    """
    Get API function library.

    Returns:
        List of function library records
    """
    try:
        functions = get_function_library()
        return {"data": functions, "count": len(functions)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch function library: {str(e)}",
        )


@router.post("/functions")
async def add_function_library_entry_endpoint(function_entry: FunctionLibraryEntry):
    """
    Add a new function library entry.

    Args:
        function_entry: The function library entry to add

    Returns:
        Success message or error
    """
    try:
        result = insert_function_library_entry(function_entry.dict())
        if result:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to add function library entry: {result}"
            )
        return {"status": "ok", "message": "Function library entry added successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to add function library entry: {str(e)}",
        )


@router.put("/functions/{friendly_name}")
async def update_function_library_entry_endpoint(friendly_name: str, function_entry: FunctionLibraryEntry):
    """
    Update an existing function library entry.

    Args:
        friendly_name: The friendly_name identifying the row to update
        function_entry: The updated function library entry

    Returns:
        Success message or error
    """
    try:
        # Remove friendly_name from updates since it's used as the WHERE clause
        updates = function_entry.dict()
        updates.pop('friendly_name', None)
        
        result = update_function_library_entry(friendly_name, updates)
        if result:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to update function library entry: {result}"
            )
        return {"status": "ok", "message": f"Function library entry '{friendly_name}' updated successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update function library entry: {str(e)}",
        )


@router.delete("/functions/{friendly_name}")
async def delete_function_library_entry_endpoint(friendly_name: str):
    """
    Delete a function library entry by friendly_name.

    Args:
        friendly_name: The friendly_name of the entry to delete

    Returns:
        Success message or error
    """
    try:
        result = delete_function_library_entry(friendly_name)
        if result:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to delete function library entry: {result}"
            )
        return {"status": "ok", "message": f"Function library entry '{friendly_name}' deleted successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete function library entry: {str(e)}",
        )


@router.get("/credentials/requirements")
async def get_credential_requirements_endpoint():
    """
    Get credential requirements for all API services.

    Returns:
        List of records with api_service_name and api_credential_requirements
    """
    try:
        requirements = get_credential_requirements()
        return {"data": requirements, "count": len(requirements)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch credential requirements: {str(e)}",
        )


@router.post("/credentials")
async def upsert_credentials_endpoint(credential: CredentialUpsert):
    """
    Encrypt and upsert credentials for a service.

    Args:
        credential: The service name and credentials to encrypt and store

    Returns:
        Success message or error
    """
    try:
        # Note: In a real implementation, we would encrypt the credentials here
        # For now, we'll pass them as-is to the upsert_credentials function
        # which should handle encryption internally
        result = upsert_credentials(
            service_name=credential.api_service_name,
            encrypted_credentials=credential.raw_credentials_json_string
        )
        if result:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to upsert credentials: {result}"
            )
        return {"status": "ok", "message": f"Credentials for '{credential.api_service_name}' updated successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to upsert credentials: {str(e)}",
        )


@router.delete("/credentials/{service_name}")
async def delete_credentials_endpoint(service_name: str):
    """
    Delete credentials for a service.

    Args:
        service_name: The name of the service whose credentials to delete

    Returns:
        Success message or error
    """
    try:
        result = delete_credentials(service_name)
        if result:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to delete credentials: {result}"
            )
        return {"status": "ok", "message": f"Credentials for '{service_name}' deleted successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete credentials: {str(e)}",
        )


@router.get("/logs/tables")
async def get_log_tables_endpoint():
    """
    Get list of log table names from the logging schema.

    Returns:
        List of log table names
    """
    try:
        tables = get_log_tables_simple()
        return {"data": tables, "count": len(tables)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch log tables: {str(e)}",
        )


@router.get("/db-info/task-summary")
async def get_task_summary():
    """
    Get task summary chart data.

    Returns:
        List of task summary records with timing, execution count, and recency details
    """
    try:
        data = get_task_summary_chart()
        return {"data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch task summary chart: {str(e)}",
        )


@router.get("/db-info/db-size-chart")
async def get_db_size_history():
    """
    Get historical database size growth data.

    Returns:
        List of records with date_utc, table_size_mb, index_size_mb, other_size_mb
    """
    try:
        data = get_db_size_chart()
        return {"data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch DB size chart: {str(e)}",
        )


@router.get("/db-info/db-size-breakdown")
async def get_db_size_breakdown_endpoint():
    """
    Get current database size breakdown by table.

    Returns:
        List of records with table_name, table_size_mb, index_size_mb, other_size_mb
    """
    try:
        data = get_db_size_breakdown()
        return {"data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch DB size breakdown: {str(e)}",
        )


@router.get("/logs/data/{table_name}")
async def get_log_data_endpoint(table_name: str, limit: int = 100):
    """
    Get log data from a specific log table.

    Args:
        table_name: The name of the log table to query
        limit: Maximum number of rows to return (default: 100)

    Returns:
        List of log records
    """
    try:
        # Basic validation to prevent SQL injection - in a real app, use an allowlist
        if not table_name.isalnum() and '_' not in table_name:
            raise HTTPException(
                status_code=400,
                detail="Invalid table name"
            )
        
        logs = get_log_data_simple(table_name, limit)
        return {"data": logs, "count": len(logs)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch log data: {str(e)}",
        )