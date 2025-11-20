import numpy as np
import streamlit as st
from streamlit import session_state as ss
import pandas as pd
import time

from backend_functions.backend_tasks import backup_database
from backend_functions.credential_management import encrypt_dict
from backend_functions.database_functions import get_conn, qec, sql_to_dict, get_sproc_list
from backend_functions.helper_functions import reverse_key_lookup, list_to_dict_by_key, set_keys_to_none
from backend_functions.logging_functions import log_app_event, start_timer, elapsed_ms
from backend_functions.service_logins import test_login, get_service_list
from backend_functions.task_execution import task_executioner
from frontend_functions.streamlit_helpers import reconcile_with_postgres


def admin_button_dict():
    d = {"passwords": ":material/key_vertical:",
         "tasks": ":material/checklist:",
         "services": ":material/api:",
         "db_backup": ":material/database_upload:"}
    return d


def render_admin_module():
    st.write("Admin Module")
    abd = admin_button_dict()
    ss.admin_nav_selection = st.pills(label="top_nav_selection",
                                    label_visibility="hidden",
                                    options=list(set(abd.values())),
                                    default=abd.get("home"))

    simple_selection = reverse_key_lookup(abd, ss.admin_nav_selection)
    if simple_selection == 'passwords':
        render_password_submodule()
    elif simple_selection == 'tasks':
        render_task_submodule()
    elif simple_selection == 'services':
        render_service_submodule()
    elif simple_selection == 'db_backup':
        render_db_backup()

def render_db_backup():
    if st.button("Create a backup of the database"):
        t0 = start_timer()
        try:
            backup_database()
            log_app_event(cat='DB', desc='Backup Created', exec_time=elapsed_ms(t0))
            st.success("Backup Created")

            time.sleep(1.5)
        except Exception as e:
            st.error(f"DB Backup failed: {e}")
            log_app_event(cat='DB', desc='Backup Created', exec_time=elapsed_ms(t0), err=e)


def render_service_submodule():
    # Read in any existing services
    st.subheader("API Service Management")
    t0 = None
    ss.service_df = pd.read_sql('SELECT * FROM api_services.api_service_list', get_conn(alchemy=True))
    if not ss.service_df.empty:
        col_config = {"api_service_name": st.column_config.TextColumn(label="Service",
                                                                      pinned=True, disabled=True),
                      "api_service_function": st.column_config.TextColumn(label="Login Functions",
                                                                           pinned=False,
                                                                           disabled=False),
                      "api_credential_requirements": st.column_config.TextColumn(label="Credentials Needed",
                                                                                 pinned=False,
                                                                                 disabled=False)}
        st.write("Known Services")
        st.data_editor(ss.service_df,
                        hide_index=True,
                        column_config=col_config,
                        key = "service_editor",
                       on_change=reconcile_with_postgres,
                       args=('service_df', "service_editor", 'api_services.api_service_list', 'api_service_name', col_config)
        )





def render_password_submodule():
    st.subheader("API Credential Management")
    # Enables the user to store the credentials required for a specific service
    t0 = None
    # Get the list of credentials into a dictionary
    cred_sql = """SELECT api_service_name, api_credential_requirements from api_services.api_service_list"""
    service_dict = list_to_dict_by_key(list_of_dicts=sql_to_dict(cred_sql),
                                       primary_key="api_service_name")

    # Step 1: Let user pick a service
    service_list = list(service_dict.keys())
    ss.selected_service = st.selectbox("Select a service", service_list)

    # Step 2: Extract credential fields for that service
    if "selected_service" in ss and ss.selected_service:
        cred_str = service_dict[ss.selected_service].get("api_credential_requirements")
        creds_needed = [c.strip() for c in cred_str.split(",") if c.strip()]

        # Step 3: Dynamically generate inputs
        ss.user_inputs = {}
        st.subheader(f"Enter credentials for {ss.selected_service}")
        for cred in creds_needed:
            ss.user_inputs[cred] = st.text_input(cred)

        # Step 4: Return structure only when all credentials are filled
        if all(ss.user_inputs.values()):
            t0 = start_timer()
            if st.button(f":material/experiment: Click to test {ss.selected_service} connection"):
                ss.credential_test_proceed = True


        if "credential_test_proceed" in ss and ss.credential_test_proceed:
            # Encrypt the results,
            enc_input = encrypt_dict(ss.user_inputs)
            insert_sql = """INSERT INTO api_services.credentials (api_service_name, api_credentials)
                        VALUES (%s, %s)
                        ON CONFLICT (api_service_name) 
                        DO UPDATE SET api_credentials = EXCLUDED.api_credentials;"""
            params = (ss.selected_service, enc_input)
            qec(insert_sql, params)
            log_app_event(cat='Admin', desc=f"Credential Saved: {ss.selected_service}", exec_time=elapsed_ms(t0))
            if test_login(ss.selected_service):
                st.balloons()
                st.success('Successful connection established')
                ss.selected_service = None
                ss.user_inputs = None
                ss.credential_test_proceed = None
                time.sleep(1)
                st.rerun()
            else:
                st.error('Unable to establish connection with those credentials')
                delete_sql = f"""DELETE FROM api_services.credentials WHERE api_service_name = ?"""
                params = (ss.selected_service, )
                qec(delete_sql, params)
                ss.user_inputs = None
                ss.credential_test_proceed = None
                time.sleep(1)
                st.rerun()


    return None


def render_task_submodule():
    st.write("__Task Configuration__")
    t0 = None
    if "existing tasks" not in ss:
        ss.existing_tasks_df = pd.read_sql('SELECT * FROM tasks.vw_task_execution', get_conn(alchemy=True))

    # Display the header information

    if "svc_list" not in ss:
        ss.svc_list = get_service_list(append_option=None)
        ss.sproc_list = get_sproc_list(append_option=None)

    parent_col_config = {"task_name": st.column_config.TextColumn(label="Name",
                                                                  pinned=True, disabled=False),
                         "task_description": st.column_config.TextColumn(label='Description',
                                                                         disabled=False,
                                                                         default=None),
                         "task_frequency": st.column_config.SelectboxColumn(label="Frequency",
                                                                            default='Retired',
                                                                            pinned=False,
                                                                            disabled=False,
                                                                            options=['Hourly',
                                                                                    'Daily',
                                                                                    'Weekly',
                                                                                    'Monthly',
                                                                                    'Retired']),
                         "task_interval": st.column_config.NumberColumn(label="Interval",
                                                                        default=2,
                                                                        pinned=False,
                                                                        disabled=False,
                                                                        min_value=0,
                                                                        max_value=28),
                         "task_priority": st.column_config.NumberColumn(label="Priority",
                                                                        default=999,
                                                                        pinned=False,
                                                                        disabled=False,
                                                                        min_value=0,
                                                                        max_value=999),
                         "api_function": st.column_config.TextColumn(label='API Function',
                                                              pinned=False,
                                                              disabled=False),
                        "api_service_name": st.column_config.SelectboxColumn(label='API',
                                                                       default=None,
                                                                       pinned=False,
                                                                       options=ss.svc_list,
                                                                       ),
                        "api_loop_type": st.column_config.SelectboxColumn(label="Loop Type",
                                                                    pinned=False,
                                                                    disabled=False,
                                                                    options=['Day', 'Range', 'Next', 'N/A'],
                                                                    default='N/A'),
                        'api_post_processing': st.column_config.SelectboxColumn(label='SPROC',
                                                                          pinned=False,
                                                                          disabled=False,
                                                                          options=ss.sproc_list,
                                                                          default=None),
                        'api_parameters': st.column_config.TextColumn(label='API Parameters',
                                                                      pinned=False,
                                                                      disabled=False),
                        "python_function": st.column_config.TextColumn(label="Python Function",
                                                                     pinned=False,
                                                                     disabled=False,
                                                                     default=None),
                        "api_service_function": None,
                        "last_calendar_field": st.column_config.TextColumn(label="Postgres As-of Column",
                                                                      pinned=False,
                                                                      disabled=False,
                                                                      default=None),
                        "total_attempts": st.column_config.ProgressColumn(
                            label="Total Attempts",
                            pinned=False,
                            format='%d',
                            min_value=0,
                            max_value=int(ss.existing_tasks_df['total_attempts'].max()) if not ss.existing_tasks_df.empty and pd.notna(ss.existing_tasks_df['total_attempts'].max()) else 100
                        ),
                         "data_requires_catchup": st.column_config.CheckboxColumn(label="Catchup?",
                                                                       disabled=True),
                        "do_execute": st.column_config.CheckboxColumn(label="Will execute",
                                                                        disabled=True),
                        "execution_logic": st.column_config.TextColumn(label="Logic used:",
                                                                       disabled=True),
                        "updated_through_utc": st.column_config.DateColumn(label="Value Recency",
                                                                            pinned=False,
                                                                            disabled=True),
                         "last_success_utc": st.column_config.DatetimeColumn(label="Last Success",
                                                                              disabled=True),
                         "earliest_execution_utc": st.column_config.DatetimeColumn(label="Earliest Possible Execution",
                                                                             disabled=True),
                         "last_execution_utc": st.column_config.DatetimeColumn(label="Last Execution",
                                                                               disabled=True),
                         "next_execution_utc": st.column_config.DatetimeColumn(label="Next Scheduled for:",
                                                                              disabled=True)
                         }
    task_col_config = set_keys_to_none(parent_col_config, ['task_name',
                                                           'task_description',
                                                           'api_function',
                                                           'api_service_name',
                                                           'api_loop_type',
                                                           'api_post_processing',
                                                           'api_parameters',
                                                           'python_function',
                                                           'last_calendar_field'])
    task_config_key = 'admin_task_config'
    st.data_editor(ss.existing_tasks_df,
                    hide_index=True,
                    column_config=task_col_config,
                    num_rows="dynamic",
                    key = task_config_key,
                    on_change = reconcile_with_postgres,
                    args = ('existing_tasks_df', task_config_key, 'tasks.task_config', 'task_name', task_col_config)
    )

    # Display the task scheduling table
    st.write("__Task Scheduling__")

    sched_col_config = set_keys_to_none(parent_col_config, ['task_name',
                                                            'task_priority',
                                                           'task_frequency',
                                                           'task_interval',
                                                           'next_execution_utc',
                                                            'data_requires_catchup',
                                                           'do_execute',
                                                            'updated_through_utc',
                                                            'total_attempts'])
    task_schedule_config_key = 'admin_schedule_config'
    st.data_editor(ss.existing_tasks_df,
                    hide_index=True,
                    column_config=sched_col_config,
                    num_rows="fixed",
                    key = task_schedule_config_key,
                    on_change = reconcile_with_postgres,
                    args = ('existing_tasks_df',
                            task_schedule_config_key,
                            'tasks.task_config',
                            'task_name',
                            sched_col_config)
    )

    st.divider()
    st.write("Force Task Execution")
    task_list = ss.existing_tasks_df['task_name'].to_list()
    # Sort the task list
    sorted_tasks = sorted(task_list)

    # Create rows with maximum 5 columns each
    max_cols = 5
    for row_start in range(0, len(sorted_tasks), max_cols):
        # Get tasks for this row (up to 5)
        row_tasks = sorted_tasks[row_start:row_start + max_cols]

        # Create columns for this row
        cols = st.columns(len(row_tasks))

        # Add buttons to columns
        for idx, task in enumerate(row_tasks):
            with cols[idx]:
                if st.button(task, key=f"btn_{task}", type='secondary'):
                    task_executioner(force_task_name=task, force_task=True)
                    st.rerun()


