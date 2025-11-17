import numpy as np
import streamlit as st
from streamlit import session_state as ss
import pandas as pd
import time
from backend_functions.credential_management import encrypt_dict
from backend_functions.database_functions import get_conn, qec, sql_to_dict, get_sproc_list
from backend_functions.helper_functions import reverse_key_lookup, list_to_dict_by_key
from backend_functions.logging_functions import log_app_event, start_timer, elapsed_ms
from backend_functions.service_logins import test_login, get_service_list
from frontend_functions.streamlit_helpers import reconcile_with_postgres


def admin_button_dict():
    d = {"passwords": ":material/key_vertical:",
         "tasks": ":material/checklist:",
         "services": ":material/api:"}
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


def render_service_submodule():
    # Read in any existing services
    st.subheader("API Service Management")
    t0 = None
    df = pd.read_sql('SELECT * FROM api_services.api_service_list', get_conn(alchemy=True))
    if not df.empty:
        col_config = {"api_service_name": st.column_config.TextColumn(label="Service",
                                                                      pinned=True, disabled=True),
                      "api_service_function": st.column_config.TextColumn(label="Login Functions",
                                                                           pinned=False,
                                                                           disabled=False),
                      "api_credential_requirements": st.column_config.TextColumn(label="Credentials Needed",
                                                                                 pinned=False,
                                                                                 disabled=False)}
        st.write("Known Services")
        st.data_editor(df,
                        hide_index=True,
                        column_config=col_config,
                        key = "service_editor",
                        on_change = handle_service_changes,
                        args = (df,)
        )

    if st.button(":material/add: Add New Service"):
        ss.admin_service_add = True

    if "admin_service_add" in ss and ss.admin_service_add:
        ss.new_service_name = st.text_input(label="Name:")
        ss.new_service_functions = st.text_input(label="Login Functions",
                                              placeholder="comma separated list")
        ss.new_credential_requirements = st.text_input(label="Credential Requirements",
                                              placeholder="comma separated list")
        if st.button(":material/save: Submit"):
            ss.new_service_submission = True
            t0 = start_timer()

    if "new_service_submission" in ss and ss.new_service_submission:
        insert_sql = """INSERT INTO api_services.api_service_list (api_service_name, 
                       api_service_function, api_credential_requirements)
                       VALUES (%s, %s, %s);"""
        params = (ss.new_service_name, ss.new_service_functions.lower(), ss.new_credential_requirements.lower())
        qec(insert_sql, params)
        log_app_event(cat="Admin", desc=f"New Service Creation: {ss.new_service_name}", exec_time=elapsed_ms(t0))
        ss.new_service_submission = False
        ss.admin_service_add = False
        st.rerun()


def handle_service_changes(original_df):
    # used in conjunction with data editor, records changes to postgres

    t0 = start_timer()

    # Get the edited data from session state
    edited_data = ss.service_editor

    # Convert to DataFrame if it's a dict with edited_rows metadata
    if isinstance(edited_data, dict):
        # st.data_editor returns edited data in a special format
        # Use the 'edited_rows' key to get actual changes
        if 'edited_rows' in edited_data and edited_data['edited_rows']:
            edited_rows = edited_data['edited_rows']

            update_sql = """
                    UPDATE api_services.api_service_list 
                    SET api_service_function = %s,
                        api_credential_requirements = %s
                    WHERE api_service_name = %s;
                """

            # edited_rows is a dict where keys are row indices
            for row_idx, changes in edited_rows.items():
                # Get the service name from original df
                service_name = original_df.iloc[row_idx]['api_service_name']

                # Get updated values (use original if not changed)
                api_functions = changes.get('api_service_function',
                                            original_df.iloc[row_idx]['api_service_function'])
                api_credentials = changes.get('api_credential_requirements',
                                              original_df.iloc[row_idx]['api_credential_requirements'])

                params = (api_functions, api_credentials, service_name)
                qec(update_sql, params)

            log_app_event(
                cat="Admin",
                desc=f"Service Updates: {len(edited_rows)} rows changed",
                exec_time=elapsed_ms(t0)
            )
        return

    # If it's already a DataFrame (older Streamlit versions)
    edited_df = edited_data

    if original_df.equals(edited_df):
        return

    check_cols = ['api_service_function', 'api_credential_requirements']

    # Find changed rows
    changed_indices = []
    for idx in range(len(edited_df)):
        for col in check_cols:
            orig_val = str(original_df.iloc[idx][col]) if pd.notna(original_df.iloc[idx][col]) else ''
            edit_val = str(edited_df.iloc[idx][col]) if pd.notna(edited_df.iloc[idx][col]) else ''
            if orig_val != edit_val:
                changed_indices.append(idx)
                break

    if not changed_indices:
        return

    update_sql = """
            UPDATE api_services.api_service_list 
            SET api_service_function = %s,
                api_credential_requirements = %s
            WHERE api_service_name = %s;
        """

    for idx in changed_indices:
        params = (
            edited_df.iloc[idx]['api_service_function'],
            edited_df.iloc[idx]['api_credential_requirements'],
            edited_df.iloc[idx]['api_service_name']
        )
        qec(update_sql, params)

    log_app_event(
        cat="Admin",
        desc=f"Service Updates: {len(changed_indices)} rows changed",
        exec_time=elapsed_ms(t0)
    )


def handle_task_changes(original_df):
    # used in conjunction with data editor, records changes to postgres

    t0 = start_timer()

    # Get the edited data from session state
    edited_data = ss.service_editor

    # Convert to DataFrame if it's a dict with edited_rows metadata
    if isinstance(edited_data, dict):
        # st.data_editor returns edited data in a special format
        # Use the 'edited_rows' key to get actual changes
        if 'edited_rows' in edited_data and edited_data['edited_rows']:
            edited_rows = edited_data['edited_rows']

            update_sql = """
                    UPDATE tasks.task_config 
                    SET task_description = %s,
                        task_priority = %s,
                        task_frequency = %s,
                        task_interval = %s,
                        api_function = %s,
                        api_service_name = %s,
                        api_loop_type = %s,
                        api_post_processing = %s,
                        python_function = %s
                    WHERE task_name = %s;
                """

            # edited_rows is a dict where keys are row indices
            for row_idx, changes in edited_rows.items():
                # Get the service name from original df
                task_description = original_df.iloc[row_idx]['task_description']
                task_priority = original_df.iloc[row_idx]['task_priority']
                task_frequency = original_df.iloc[row_idx]['task_frequency']
                task_interval = original_df.iloc[row_idx]['task_interval']
                api_function = original_df.iloc[row_idx]['api_function']
                api_service_name = original_df.iloc[row_idx]['api_service_name']
                api_loop_type = original_df.iloc[row_idx]['api_loop_type']
                api_post_processing = original_df.iloc[row_idx]['api_post_processing']
                python_function = original_df.iloc[row_idx]['python_function']
                task_name = original_df.iloc[row_idx]['task_name']

                params = (task_description, task_priority, task_frequency,
                          task_interval,
                          api_function,
                          api_service_name,
                          api_loop_type,
                          api_post_processing,
                          python_function,
                          task_name)
                qec(update_sql, params)

            log_app_event(
                cat="Admin",
                desc=f"Task Updates: {len(edited_rows)} rows changed",
                exec_time=elapsed_ms(t0)
            )
            #Regen Dataframe
            ss.existing_tasks_df = pd.read_sql('SELECT * FROM tasks.task_config', get_conn(alchemy=True))
        return
    else:
        st.info("Did nothing with the task changes")
        time.sleep(3)



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
    st.subheader("Task Configuration")
    t0 = None
    if "existing tasks" not in ss:
        ss.existing_tasks_df = pd.read_sql('SELECT * FROM tasks.task_config', get_conn(alchemy=True))

    # Display the header information

    if "svc_list" not in ss:
        ss.svc_list = get_service_list(append_option='N/A')
        ss.sproc_list = get_sproc_list(append_option='N/A')

    config_col_config = {"task_name": st.column_config.TextColumn(label="Name",
                                                                  pinned=True, disabled=True),
                  "task_description": st.column_config.TextColumn(label="Description",
                                                                       pinned=False,
                                                                       disabled=False),
                  "api_function": st.column_config.TextColumn(label='API Function',
                                                              pinned=False,
                                                              disabled=False),
                  "api_service_name": st.column_config.SelectboxColumn(label='API',
                                                                       default='N/A',
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
                                                                          default='N/A'),
                  "python_function": st.column_config.TextColumn(label="Python Function",
                                                                 pinned=False,
                                                                 disabled=False,
                                                                 default=None),
                  "last_calendar_field": st.column_config.TextColumn(label="Postgres As-of Column",
                                                                  pinned=False,
                                                                  disabled=False,
                                                                  default=None)}
    config_key = 'admin_task_config'
    st.data_editor(ss.existing_tasks_df,
                    hide_index=True,
                    column_config=config_col_config,
                    num_rows="dynamic",
                    key = config_key,
                    on_change = reconcile_with_postgres,
                    args = ('existing_tasks_df', config_key, 'tasks.task_config', 'task_name', config_col_config)
    )

    sched_col_config = {"task_name": st.column_config.TextColumn(label="Name",
                                                                  pinned=True, disabled=True),
                  "task_priority": st.column_config.NumberColumn(label="Priority",
                                                                 pinned=False,
                                                                 disabled=False,
                                                                 default=999,
                                                                 format='%d'),
                  "task_frequency": st.column_config.SelectboxColumn(label="Frequency",
                                                                     pinned=False,
                                                                     options=['Hourly',
                                                                              'Daily',
                                                                              'Weekly',
                                                                              'Monthly',
                                                                              'Retired'],
                                                                     disabled=False,
                                                                     default='Daily'),
                  "task_interval": st.column_config.NumberColumn(label="Interval",
                                                                 pinned=False,
                                                                 default=23,
                                                                 format='%d',
                                                                 disabled=False),
                  "api_function": st.column_config.TextColumn(label='API Function',
                                                              pinned=False,
                                                              disabled=False),
                  "api_service_name": st.column_config.SelectboxColumn(label='API',
                                                                       default='N/A',
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
                                                                          default='N/A'),
                  "python_function": st.column_config.TextColumn(label="Python Function",
                                                                 pinned=False,
                                                                 disabled=False,
                                                                 default=None),
                  "last_calendar_date_col": st.column_config.TextColumn(label="Current Through",
                                                                    disabled=True,
                                                                    pinned=False)}
    task_sched_key = 'admin_task_config'
    st.data_editor(ss.existing_tasks_df,
                    hide_index=True,
                    column_config=sched_col_config,
                    num_rows="dynamic",
                    key = config_key,
                    on_change = reconcile_with_postgres,
                    args = ('existing_tasks_df', task_sched_key, 'tasks.task_config', 'task_name', sched_col_config)
    )



