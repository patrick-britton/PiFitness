import time
from sqlalchemy import text
from streamlit import session_state as ss, column_config
import streamlit as st
import pandas as pd

from backend_functions.database_functions import sql_to_dict, one_sql_result, qec, get_sproc_list, get_conn, \
    get_api_function_list
from backend_functions.service_logins import get_service_list
from backend_functions.ultimate_task_executioner import ultimate_task_executioner, reconcile_task_dates
from frontend_functions.nav_buttons import nav_widget, update_nav, clear_nav
from frontend_functions.streamlit_helpers import sse, ss_pop


def render_task_selection():
    if "selected_task_id" not in ss:
        ss.selected_task_id = None

    if sse("selected_task_id"):
        return

    st.info('Select a task below')
    sql = "SELECT task_id, task_name, display_icon, task_frequency FROM tasks.task_configuration order by task_name"
    task_dict_list = sql_to_dict(sql)

    # Categorize tasks by frequency
    freq_columns = {
        "Hourly": [],
        "Daily": [],
        "Weekly/Monthly": [],
        "Inactive": []
    }

    for task in task_dict_list:
        freq = task.get("task_frequency", "Inactive")
        if freq == "Hourly":
            freq_columns["Hourly"].append(task)
        elif freq == "Daily":
            freq_columns["Daily"].append(task)
        elif freq in ("Weekly", "Monthly"):
            freq_columns["Weekly/Monthly"].append(task)
        else:
            freq_columns["Inactive"].append(task)

    # Create columns in Streamlit
    col_labels = ["Hourly", "Daily", "Weekly/Monthly", "Inactive"]
    valid_cols = []
    col_count = 0
    for idx, label in enumerate(col_labels):
        if len(freq_columns[label]) != 0:
            valid_cols.append(label)
            col_count += 1

    cols = st.columns(spec=col_count, gap="small", border=False)

    for idx, label in enumerate(valid_cols):
        if len(freq_columns[label]) == 0:
            continue

        with cols[idx]:
            st.write(f"__{label}__")
            for task in freq_columns[label]:
                icon = task.get("display_icon")
                task_name = task.get("task_name")
                btn_lbl = f":material/{icon}: {task_name}" if icon else task_name
                btn_key = f"sel_button_{task.get('task_id')}"

                if st.button(btn_lbl, key=btn_key, type='tertiary'):
                    ss.selected_task_id = task.get("task_id")
                    st.rerun()

    # Display selected task (optional)
    if st.session_state.selected_task_id:
        st.write(f"Selected Task ID: {st.session_state.selected_task_id}")


    return

def render_task_id_management():
    if sse('bypass_task_mgmt_nav', bool_flag=True):
        nav_selection = 'edit_task'
        ss_pop(["selected_task_id", 'selected_staging_id'])
        clear_nav('task_management')
        ss.bypass_task_mgmt_nav = False
    else:
        nav_selection = nav_widget('task_management', 'Task Options')

    if not nav_selection:
        nav_selection = 'edit_task'

    if nav_selection == 'task_reset':
        ss.bypass_task_mgmt_nav = True

        ss[f"task_management_active"] = ':material/edit:'
        key_val = f"key_task_management_nav_{ss.n_counter}"
        update_nav(pn='task_management', key_val=key_val, custom_dict=None, force_change=True)
        # st.info("Select above to create or edit a task")
        nav_selection = 'edit_task'
        st.rerun()

    if nav_selection == 'create_task' and not sse("selected_task_id"):
        insta_task_create()
        return


        return
    if nav_selection == 'edit_task':
        if not sse("selected_task_id"):
            render_task_selection()
            return
        render_task_edit(ss.selected_task_id)
    elif nav_selection == 'reschedule_tasks':
        render_task_rescheduling()


    return

def insta_task_create():
    sel_sql = "SELECT MIN(task_id) FROM tasks.task_configuration where task_name = 'placeholder_task';"
    id_val = one_sql_result(sel_sql)

    if id_val:
        ss.selected_task_id = id_val[0]
        st.rerun()

    ins_sql = "INSERT INTO tasks.task_configuration (task_name) VALUES (%s)"
    qec(ins_sql,['placeholder_task',])
    st.rerun()
    return


def render_task_edit(task_id):
    if not task_id:
        st.error('Somehow editing with no task id')
        return

    if not sse("sproc_list"):
        ss.sproc_list = get_sproc_list(append_option='N/A')

    if not sse('service_list'):
        ss.service_list = get_service_list(append_option='N/A')

    if not sse('function_list'):
        ss.function_list = get_api_function_list(append_option='N/A')

    sel_sql = f"SELECT * FROM tasks.task_configuration where task_id = {task_id}"
    d = sql_to_dict(sel_sql)[0]

    if d.get('task_name') == 'placeholder_task':
        msg = f":blue[###New task creation###: :gray[*ID# {task_id}*]"
        default_name = None
        st.write(msg)
    else:
        header_col, execute_col = st.columns(spec=[1,2 ], gap="small", border=False)
        with header_col:
            default_name = d.get('task_name')
            msg = f":blue[Editing: __{d.get("task_name")}__] :gray[*ID# {task_id}*]  \n"
            msg = f"{msg}Last Executed: {d.get('last_executed_utc')}  \n"
            if d.get('last_executed_utc') == d.get('last_succeeded_utc'):
                is_failure = False

            else:
                is_failure = True
                msg = f"{msg}Last Succeeded: {d.get('last_succeeded_utc')}  \n"
            msg = f"{msg}Next planned execution: {d.get('next_planned_execution_utc')}  \n"
            if d.get('last_failed_utc'):
                if not is_failure:
                    msg = f"{msg}Last Failed: {d.get('last_failed_utc')}  \n"
                else:
                    msg = f"{msg}:red["
                    msg = f"{msg}__Most recent failure: {d.get('last_failure_message')}__  \n"
                    msg = f"{msg}__Consecutive Failures: {d.get('consecutive_failures')}__ "
                if is_failure:
                    msg = f"{msg}]"
            else:
                msg = f"{msg} No recorded failures"
            st.write(msg)
        with execute_col:
            if st.button(':material/motion_play: Run Task Now', type='primary'):
                ss.show_df=False
                with st.spinner('Executing Task...', show_time=True):
                    ultimate_task_executioner(force_task_id=task_id)
                st.toast('Completed', duration=5)
                time.sleep(3)
                st.rerun()

    basic_col, schedule_col = st.columns(spec=[2,1], border=False, gap="small")

    with basic_col:
        with st.container(border=True):
            st.write("__:material/info: Basic Information__")
            task_name = st.text_input(label='Name',
                                      value=default_name)
            task_description = st.text_area(label='Description',
                                       value=d.get('task_description'))
            display_icon = st.text_input(label='Icon',
                                         value=d.get('display_icon'))
            if display_icon:
                st.write(f"__:material/{display_icon}:__")

            python_execution_function = st.text_input(label='Python function?',
                                            value=d.get('python_execution_function'))

    with schedule_col:
        schedule_cont = st.container(border=True)
        with schedule_cont:
            st.write('__:material/calendar_clock: Scheduling__')
            task_frequency = st.segmented_control(label='Frequency',
                                                  options=['Hourly','Daily','Weekly', 'Monthly', 'Inactive'],
                                                  default=d.get('task_frequency'))
            task_priority = st.number_input(label='Priority',
                                            min_value=1,
                                            max_value=999,
                                            value=d.get('task_priority'))
            if task_frequency == 'Hourly':
                task_interval = st.number_input(label='Hours between tasks?',
                                                min_value=1,
                                                max_value=11,
                                                value=d.get('task_interval'))
                start_msg = 'Do not run before:'
            else:
                start_msg = 'Execution Hour:'
                task_interval=1
            task_start_hour = st.number_input(label=start_msg,
                                              min_value=1,
                                              max_value=23,
                                              value=d.get('task_start_time', 1))
            if task_frequency == 'Hourly':
                task_stop_hour = st.number_input(label='Do not run after:',
                                                min_value=1,
                                                max_value=23,
                                                value=d.get('task_end_time', 23))
            else:
                task_stop_hour=23

    with st.container(border=True):
        st.write('__:material/file_json: Extraction__')
        api_function_name = st.segmented_control(label='Data Extraction (API Function)',
                                        options=ss.function_list,
                                        default=d.get('api_function_name'))


    if len(task_name) < 4:
        st.info("Name must be more than 4 characters")
    else:
        if st.button(':material/Save: Save Changes', type="primary"):
            update_sql = """UPDATE tasks.task_configuration SET 
                            task_name = %s,
                            display_icon = %s,
                            task_description = %s,
                            task_priority = %s,
                            task_frequency = %s,
                            task_start_hour = %s,
                            task_stop_hour = %s,
                            task_interval = %s,
                            api_function_name = %s,
                            python_execution_function = %s
                            WHERE task_id = %s
                            """
            params = [task_name,
                      display_icon,
                      task_description,
                      int(task_priority),
                      task_frequency,
                      int(task_start_hour),
                      int(task_stop_hour),
                      int(task_interval),
                      api_function_name,
                      python_execution_function,
                      int(task_id)]
            returns = qec(update_sql, params)
            if returns:
                st.warning(returns)
            else:
                st.toast('Saved Successfully', duration=5)
                time.sleep(3)
                st.rerun()

    recent_execution_log(task_id)
    return



def update_fact_df(task_id, staging_id):
    d = ss.get('fact_df_updates')
    dr = d.get('deleted_rows')
    er = d.get('edited_rows')
    ar = d.get('added_rows')

    if dr:
        for row in dr:
            fact_id = ss.fact_df['fact_id'].iloc[row]
            sql = f"""DELETE FROM tasks.fact_configuration WHERE fact_id = {int(fact_id)};"""
            qec(sql)
    if ar:
        for col in ar:
            ins_sql = "INSERT INTO tasks.fact_configuration (task_id, staging_id"
            values_sql = "VALUES (%s, %s"
            params = [task_id, staging_id]
            for key, key_val in col.items():
                ins_sql = ins_sql + f", {key}"
                values_sql = values_sql + ", %s"
                params.append(key_val)
            final_sql = f"{ins_sql}) {values_sql});"
            rf = qec(final_sql, params)
            if rf:
                st.write(rf)

    if er:
        for row_index, updates in er.items():
            fact_id = ss.fact_df['fact_id'].iloc[row_index]
            up_sql = "UPDATE tasks.fact_configuration SET "
            values_sql = ''
            where_sql = "WHERE fact_id = %s"
            params=[]
            for key, key_val in updates.items():
                  values_sql = f"{values_sql}{key} = %s, "
                  params.append(key_val)
            params.append(int(fact_id))
            if values_sql.endswith(', '):
                values_sql = values_sql[:-2]
            final_sql = f"{up_sql} {values_sql} {where_sql};"
            rf = qec(final_sql, params)
            if rf:
                st.write(rf)

    ss_pop(ss.fact_df)
    st.success('Values saved!')
    time.sleep(10)
    st.rerun()
    return


def recent_execution_log(task_id):
    sel_sql = f"""SELECT * FROM logging.application_events
                    where event_category like '%Task #{task_id}%'
                    order by event_time_utc desc
                    LIMIT 10"""
    recent_df = pd.read_sql(text(sel_sql), con=get_conn(alchemy=True))
    if not recent_df.empty:
        max_time = int(recent_df['execution_time_ms'].max())
        max_time = 1 if max_time == 0 else max_time
        cols = ['event_time_utc', 'event_description', 'execution_time_ms', 'error_text']
        col_config = {'event_time_utc': st.column_config.DatetimeColumn(label='Age', format='distance', width="small"),
                      'event_description': st.column_config.TextColumn(label='Desc', width="medium"),
                      'execution_time_ms': st.column_config.ProgressColumn(label='ms',
                                                                           min_value=0,
                                                                           max_value=max_time,
                                                                           format='%d', width=40
                                                                           ),
                      'error_text': st.column_config.TextColumn(label='Error', width='medium')}
        st.dataframe(recent_df, column_order=cols, column_config=col_config, hide_index=True)
    return


def render_task_rescheduling():
    if st.button(':material/recycling: Reschedule all active tasks', type='primary'):
        task_reschedule()
        st.rerun()

    st.write(f"__Current Schedule__")
    task_sql = """SELECT 
                task_name,
                last_executed_utc,
                next_planned_execution_utc
                FROM tasks.task_configuration
                ORDER BY next_planned_execution_utc asc"""

    task_df = pd.read_sql(text(task_sql), con=get_conn(alchemy=True))
    column_config = {'task_name': st.column_config.TextColumn(label='Name'),
                     'last_executed_utc': st.column_config.DatetimeColumn(label='Last', format='distance'),
                     'next_planned_execution_utc': st.column_config.DatetimeColumn(label='Next', format='distance')}
    st.dataframe(task_df, column_config=column_config, hide_index=True)
    return


def task_reschedule():
    sql = "SELECT * FROM tasks.vw_task_info WHERE task_frequency != 'Inactive'"
    sql = f"{sql} ORDER BY api_service_name, next_planned_execution_utc"

    task_list = sql_to_dict(sql)
    with st.spinner(f"Rescheduling {len(task_list)} tasks...", show_time=True):
        for task_dict in task_list:
            reconcile_task_dates(task_dict)
    st.toast('Rescheduling Complete', duration=3)
    return