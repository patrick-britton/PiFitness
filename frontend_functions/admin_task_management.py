import time

from fsspec.registry import default
from streamlit import session_state as ss, column_config
import streamlit as st
import pandas as pd

from backend_functions.database_functions import sql_to_dict, one_sql_result, qec, get_sproc_list, get_conn
from backend_functions.service_logins import get_service_list
from frontend_functions.nav_buttons import nav_widget
from frontend_functions.streamlit_helpers import sse, ss_pop


def render_task_selection():
    if "selected_task_id" not in ss:
        ss.selected_task_id = None

    if sse("selected_task_id"):
        return

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
    nav_selection = nav_widget('task_management', 'Task Options')

    if not nav_selection:
        st.info("Select above to create or edit a task")
        return

    if nav_selection == 'task_reset':
        ss_pop("selected_task_id")
        st.info("Select above to create or edit a task")
        return

    if nav_selection == 'create_task' and not sse("selected_task_id"):
        insta_task_create()
        return

    if not sse("selected_task_id"):
        render_task_selection()
        return

    render_task_edit(ss.selected_task_id)
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



def render_task_edit(task_id):
    if not task_id:
        st.error('Somehow editing with no task id')
        return

    if not sse("sproc_list"):
        ss.sproc_list = get_sproc_list(append_option='N/A')
        ss.service_list = get_service_list(append_option='N/A')

    sel_sql = f"SELECT * FROM tasks.task_configuration where task_id = {task_id}"
    d = sql_to_dict(sel_sql)[0]

    if d.get('task_name') == 'placeholder_task':
        msg = f":blue[###New task creation###: :gray[*ID# {task_id}*]"
        default_name = None
    else:
        default_name = d.get('task_name')
        msg = f":blue[Editing: __{d.get("task_name")}__] :gray[*ID# {task_id}*]  \n"
        msg = f"{msg}Last Executed: {d.get('last_executed_utc')}  \n"
        if d.get('last_executed_utc') == d.get('last_success_utc'):
            is_failure = False

        else:
            is_failure = True
            msg = f"{msg}Last Executed: {d.get('last_succeeded_utc')}  \n"
        msg = f"{msg}Next planned execution: {d.get('next_planned_execution_utc')}  \n"
        if d.get('last_failed_utc'):
            if not is_failure:
                msg = f"{msg}Last Failed: {d.get('last_failed_utc')}  \n"
            else:
                msg = f"{msg}:red[__"
            msg = f"{msg}Most recent failure: {d.get('last_failure_message')}  \n"
            msg = f"{msg}Consecutive Failures: {d.get('consecutive_failures')} "
            if is_failure:
                msg = f"{msg}__]"
        else:
            msg = f"{msg} No recorded failures"

    st.write(msg)

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

            python_function = st.text_input(label='Python function?',
                                            value=d.get('python_function'))

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
        st.write('__:material/rebase: Data Processing__')
        col1, col2 = st.columns(spec=[1,3], gap="medium", border=False)
        with col1:
            api_service_name = st.segmented_control(label='API Service',
                                            options=ss.service_list,
                                            default=d.get('api_service_name'))
            api_loop_type = st.segmented_control(label='API Loop Type',
                                                 options=['Day', 'Range', 'Next', 'N/A'],
                                                 default=d.get('api_loop_type'))

        with col2:
            api_function_name = st.text_input(label='API Function',
                                              value=d.get('api_function_name'))
            api_parameters = st.text_input(label='API Parameters',
                                           value=d.get('api_parameters'))


        api_post_processing_sproc = st.segmented_control(label='API Post-Processing',
                                                         options=ss.sproc_list,
                                                         default=d.get('api_post_processing_sproc'))
        cross_join_condition = st.text_input(label='Cross join condition:',
                                             value=d.get('cross_join_condition'))
        filter_condition = st.text_input(label='Filter Condition:',
                                             value=d.get('filter_condition'))
        timestamp_extraction_sql = st.text_input(label='Timestamp Extraction SQL:',
                                         value=d.get('timestamp_extraction_sql'))

    if len(task_name) < 4:
        st.info("Name must be more than 4 characters")
    else:
        if st.button(':material/Save: Save Changes'):
            update_sql = """UPDATE tasks.task_configuration SET 
                            task_name = %s,
                            display_icon = %s,
                            task_description = %s,
                            task_priority = %s,
                            task_frequency = %s,
                            task_start_hour = %s,
                            task_stop_hour = %s,
                            task_interval = %s,
                            api_service_name = %s,
                            api_function_name = %s,
                            api_loop_type = %s,
                            api_post_processing_sproc = %s,
                            api_parameters = %s,
                            python_function = %s,
                            cross_join_condition = %s,
                            filter_condition = %s,
                            timestamp_extraction_sql = %s
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
                      api_service_name,
                      api_function_name,
                      api_loop_type,
                      api_post_processing_sproc,
                      api_parameters,
                      python_function,
                      cross_join_condition,
                      filter_condition,
                      timestamp_extraction_sql,
                      int(task_id)]
            returns = qec(update_sql, params)
            st.write(returns)
            # st.write(params)
            # ss_pop("selected_task_id")
            # st.rerun()
    st.divider()
    st.write(f"__Fact Management__")
    render_fact_management(task_id)

    return


def render_fact_management(task_id):
    if not task_id:
        st.error('Cannot render subtasks without task_id')
        return

    sel_query = f"""SELECT * FROM tasks.fact_configuration WHERE task_id = {task_id}"""
    ss.fact_df = pd.read_sql(sel_query, con=get_conn(alchemy=True))



    cols = ['fact_id',
            'fact_name',
            'data_type',
            'extraction_sql',
            'infer_values',
            'forecast_values']

    if ss.fact_df.empty:
        ss.fact_df = pd.DataFrame(columns=cols)

    col_config = {'fact_id': st.column_config.NumberColumn(label='ID',
                                                     width=20,
                                                     pinned=True,
                                                     disabled=True),
                  'fact_name': st.column_config.TextColumn(label='Name',
                                                           pinned=True,
                                                           disabled=False),
                  'data_type': st.column_config.TextColumn(label='Data Type',
                                                           pinned=False,
                                                           disabled=False),
                  'extraction_sql': st.column_config.TextColumn(label='Extraction SQL',
                                                           pinned=False,
                                                           disabled=False),
                  'infer_values': st.column_config.CheckboxColumn(label='Infer before interpolation?',
                                                                  disabled=False, width="small"),
                  'forecast_values': st.column_config.CheckboxColumn(label='Forecast Values?',
                                                                  disabled=False, width="small")
                  }

    st.data_editor(ss.fact_df,
                 column_order=cols,
                 column_config=col_config,
                 num_rows="dynamic",
                    key='fact_df_updates',
                   hide_index=True,
                   )
    st.write(ss.get('fact_df_updates'))
    if st.button(':material/save: Save Fact Changes'):
        update_fact_df(task_id)

    return

def update_fact_df(task_id):
    d = ss.get('fact_df_updates')
    ar = d.get('added_rows')
    st.write('Added Rows:')
    st.write(ar)
    if ar:
        for col in ar:
            ins_sql = "INSERT INTO tasks.fact_configuration (task_id "
            values_sql = "VALUES (%s"
            params = [task_id]
            for key, key_val in col.items():
                ins_sql = ins_sql + f", {key}"
                values_sql = values_sql + ", %s"
                params.append(key_val)
            final_sql = f"{ins_sql}) {values_sql});"
            rf = qec(final_sql, params)
            st.write(rf)


    ss_pop(ss.fact_df)
    st.success('Values saved!')
    time.sleep(1)
    st.rerun()




    return