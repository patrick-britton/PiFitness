import numpy as np
import streamlit as st
from streamlit import session_state as ss
import pandas as pd
import time

from backend_functions.admin_functions import get_runtime_status
from backend_functions.backend_tasks import backup_database
from backend_functions.credential_management import encrypt_dict
from backend_functions.database_functions import get_conn, qec, sql_to_dict, get_sproc_list, get_log_data, \
    get_log_tables, sql_to_list, performance_profiling
from backend_functions.helper_functions import reverse_key_lookup, list_to_dict_by_key, set_keys_to_none, \
    add_time_ago_column, col_value
from backend_functions.logging_functions import log_app_event, start_timer, elapsed_ms
from backend_functions.service_logins import test_login, get_service_list
from backend_functions.task_execution import task_executioner
from backend_functions.viz_factory.db_size import render_db_size_dashboard
from backend_functions.viz_factory.task_summary import render_task_summary_dashboard
from frontend_functions.admin_task_management import render_task_id_management
from frontend_functions.nav_buttons import nav_button, nav_widget
from frontend_functions.streamlit_helpers import reconcile_with_postgres

# Admin Page
# 'admin': {"admin_charting": {'icon': 'show_chart'},
#           "task_mgmt": {'icon': "discover_tune", 'label': 'Task Mgmt'},
#           "task_exec": {'icon': "motion_play", 'label': 'Task Exec'},
#           "passwords": {'icon': "key_vertical", 'label': 'Passwords'},
#           "services": {'icon': "api", 'label': 'API Mgmt'}
#           },
#
# 'admin_charting': {'task_summary': {'icon': 'checklist', 'label': 'Tasks'},
#                    'db_size': {'icon': 'database', 'label': 'DB Size'}},


def render_admin_module():
    nav_selection = nav_widget('admin', 'Admin Options:')

    if not nav_selection:
        nav_selection = 'admin_charting'

    if nav_selection == 'admin_charting':
        render_admin_charting()
    elif nav_selection == 'service_status':
        render_service_status()
    elif nav_selection == 'passwords':
        render_password_submodule()
    elif nav_selection == 'task_mgmt':
        render_task_mgmt_submodule()
    elif nav_selection == 'task_exec':
        render_task_exec_submodule()
    elif nav_selection == 'services':
        render_service_submodule()
    elif nav_selection == 'task_management':
        render_task_id_management()
    else:
        st.error('Uncaught admin nav selection')


    return


def render_admin_charting():
    nav_selection = nav_widget('admin_charting', 'Chart Options')

    if not nav_selection:
        nav_selection = 'task_summary'

    if nav_selection == 'task_summary':
        with st.spinner('v2', show_time=True):
            render_task_summary_dashboard(is_dark_mode=ss.get("is_dark_mode"), is_mobile=ss.get("is_mobile"))

    elif nav_selection == 'db_size':
        render_db_size_dashboard(is_dark_mode=ss.get("is_dark_mode"), is_mobile=ss.get("is_mobile"))

    elif nav_selection == 'log_review':
        render_log_file()
    else:
        st.error(f'Uncaught admin charting navigation: {nav_selection}')
    return


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
    return



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


def render_task_mgmt_submodule():
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
                         "consecutive_failures": None,
                         "consecutive_successes": None,
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
    return


def render_task_exec_submodule():
    st.write("__Select a task to execute:__")
    task_list = sql_to_list('SELECT DISTINCT task_name FROM tasks.vw_task_execution ORDER by task_name')

    if not task_list:
        st.info('No tasks found')
        return

    # Create rows with maximum 5 columns each
    max_cols = 5
    for row_start in range(0, len(task_list), max_cols):
        # Get tasks for this row (up to 5)
        row_tasks = task_list[row_start:row_start + max_cols]

        # Create columns for this row
        cols = st.columns(len(row_tasks))

        # Add buttons to columns
        for idx, task in enumerate(row_tasks):
            with cols[idx]:
                if st.button(task, key=f"btn_{task}", type='secondary'):
                    task_executioner(force_task_name=task, force_task=True)
                    st.rerun()


    return


def render_service_status():
    status = get_runtime_status()
    if status["mode"] == "local":
        st.success(status["message"])

    elif status["mode"] == "pi5":
        st.subheader("Timer Status")
        st.text(status["timer"].get("Active:", "Active: not found"))
        st.text(status["timer"].get("Trigger:", "Trigger: not found"))

        st.subheader("Service Status")
        st.text(status["service"].get("Loaded:", "Loaded: not found"))
        st.text(status["service"].get("Active:", "Active: not found"))

        st.subheader("Recent Logs")
        for line in status["service_logs"]:
            st.text(line)

    else:
        st.error(status["message"])
    return

def render_log_file():
    st.write("__Log Display__")
    base_sql = "SELECT * FROM logging.vw_all_event_history WHERE 1=1 "

    search_col, type_col, ig_skip_col = st.columns(spec=[2,2,1], border=False, gap="small")

    with search_col:
        search_val = st.text_input("Search for:", value=None)


    with ig_skip_col:
        errors_only = st.checkbox(label='Errors only', value=False)
        ignore_skips = st.checkbox(label='Ignore Skips', value=False)

    with type_col:
        type_val = st.segmented_control(label='Event Type',
                                        options=['Login', 'App Event', 'Task Execution', 'All'],
                                        default='All')

    if search_val and len(search_val) > 2:
        base_sql = f"""{base_sql} AND COALESCE(event_type,'') || COALESCE(description,'') || COALESCE(error_text,'') LIKE '%%{search_val}%%' """

    if errors_only:
        base_sql = f"{base_sql} AND is_error "

    if ignore_skips:
        base_sql = f"{base_sql} AND not_skip_row  "

    if type_val and type_val!= 'All':
        base_sql = f"{base_sql} AND event_type = '{type_val}' "

    base_sql = f"{base_sql} LIMIT 250"
    print(base_sql)
    df = pd.read_sql(base_sql, con=get_conn(alchemy=True))

    if df.empty:
        st.info('No events found')
        return


    cols = ['event_time_utc', 'event_type', 'description', 'error_text']
    col_config = {'event_time_utc': st.column_config.DatetimeColumn(label='@',
                                                                    format='distance',
                                                                    pinned=True,
                                                                    disabled=True,
                                                                    width="small"),
                  'event_type': st.column_config.TextColumn(label='Event Type',
                                                            pinned=False,
                                                            disabled=True,
                                                            width="small"),
                  'description': st.column_config.TextColumn(label='Description',
                                                             width="large",
                                                             disabled=True),
                  'error_text': st.column_config.TextColumn(label='Error Text',
                                                             width="large",
                                                             disabled=True
                                                            )}
    st.dataframe(df, column_order=cols, column_config=col_config, hide_index=True, on_select="ignore")
    return
