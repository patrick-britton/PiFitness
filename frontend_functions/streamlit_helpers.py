from backend_functions.database_functions import qec, get_conn
from backend_functions.logging_functions import log_app_event
from streamlit import session_state as ss
import time


def start_timer():
    return time.perf_counter()


def elapsed_ms(start_time):
    return int((time.perf_counter() - start_time) * 1000)


def reconcile_with_postgres(orig_df, new_df_key, pg_table, pg_table_key, de_col_config):
    #Applies updates, inserts, and deletes made with st.data_editor to PostgreSQL table.

    t0 = start_timer()
    edited_dict = ss[new_df_key]

    editable_cols = [col for col, config in de_col_config.items()
                     if not config.disabled]

    if not isinstance(edited_dict, dict):
        return

    # Process all operations
    updated_count = _handle_updates(edited_dict, orig_df, pg_table, pg_table_key, editable_cols)
    inserted_count = _handle_inserts(edited_dict, pg_table, pg_table_key, editable_cols)
    deleted_count = _handle_deletes(edited_dict, orig_df, pg_table, pg_table_key)

    # Log summary
    _log_changes(pg_table, updated_count, inserted_count, deleted_count, t0)


def _handle_updates(edited_dict, orig_df, pg_table, pg_table_key, de_col_config):
    # Process all UPDATE operations in a single batch.
    if 'edited_rows' not in edited_dict or not edited_dict['edited_rows']:
        return 0

    edited_rows = edited_dict['edited_rows']

    # Build UPDATE query
    set_clause = ', '.join([f"{col} = %s" for col in de_col_config])
    update_sql = f"UPDATE {pg_table} SET {set_clause} WHERE {pg_table_key} = %s"


    # Prepare all params for batch execution
    params_list = []
    for row_idx, changes in edited_rows.items():
        pk_value = orig_df.iloc[row_idx][pg_table_key]
        params = [
            changes.get(col, orig_df.iloc[row_idx][col])
            for col in de_col_config
        ]
        params.append(pk_value)
        params_list.append(tuple(params))

    # Execute all updates in one batch
    if params_list:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.executemany(update_sql, params_list)
        conn.commit()
        cursor.close()
        conn.close()

    return len(params_list)


def _handle_inserts(edited_dict, pg_table, pg_table_key, de_col_config):
    # Process all INSERT operations in a single batch.
    if 'added_rows' not in edited_dict or not edited_dict['added_rows']:
        return 0

    added_rows = edited_dict['added_rows']

    # Determine if primary key is provided or auto-generated
    first_row = added_rows[0]
    include_pk = pg_table_key in first_row and first_row[pg_table_key] is not None

    # Build INSERT query
    cols = [pg_table_key] + de_col_config if include_pk else de_col_config
    cols_clause = ', '.join(cols)
    placeholders = ', '.join(['%s'] * len(cols))
    insert_sql = f"INSERT INTO {pg_table} ({cols_clause}) VALUES ({placeholders})"

    # Prepare all params for batch execution
    params_list = []
    for new_row in added_rows:
        if include_pk:
            params = [new_row.get(pg_table_key)] + [new_row.get(col) for col in de_col_config]
        else:
            params = [new_row.get(col) for col in de_col_config]
        params_list.append(tuple(params))

    # Execute all inserts in one batch
    if params_list:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.executemany(insert_sql, params_list)
        conn.commit()
        cursor.close()
        conn.close()

    return len(params_list)


def _handle_deletes(edited_dict, orig_df, pg_table, pg_table_key):
    # Process all DELETE operations in a single batch.
    if 'deleted_rows' not in edited_dict or not edited_dict['deleted_rows']:
        return 0

    deleted_row_indices = edited_dict['deleted_rows']

    # Build DELETE query
    delete_sql = f"DELETE FROM {pg_table} WHERE {pg_table_key} = %s"

    # Prepare all params for batch execution
    params_list = [
        (orig_df.iloc[row_idx][pg_table_key],)
        for row_idx in deleted_row_indices
    ]

    # Execute all deletes in one batch
    if params_list:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.executemany(delete_sql, params_list)
        conn.commit()
        cursor.close()
        conn.close()

    return len(params_list)


def _log_changes(pg_table, updated_count, inserted_count, deleted_count, t0):
    # Log summary of all changes.
    changes = []
    if updated_count > 0:
        changes.append(f"{updated_count} updated")
    if inserted_count > 0:
        changes.append(f"{inserted_count} inserted")
    if deleted_count > 0:
        changes.append(f"{deleted_count} deleted")

    if changes:
        log_app_event(
            cat="Admin",
            desc=f"{pg_table} changes: {', '.join(changes)}",
            exec_time=elapsed_ms(t0)
        )