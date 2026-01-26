import time


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
        ss_pop(["selected_task_id", 'selected_staging_id'])
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
    return


def inst_staging_create(d):
    task_id = int(d.get("task_id"))
    sel_sql = f"""SELECT MIN(staging_id) 
                FROM tasks.staging_configuration
                where staging_name = 'placeholder' and task_id = {task_id};"""
    id_val = one_sql_result(sel_sql)

    if id_val:
        ss.selected_staging_id = id_val[0]
        st.rerun()

    ins_sql = "INSERT INTO tasks.staging_configuration (task_id, staging_name) VALUES (%s, %s)"
    qec(ins_sql, [task_id, 'placeholder'])
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
                msg = f"{msg}:red["
            msg = f"{msg}__Most recent failure: {d.get('last_failure_message')}__  \n"
            msg = f"{msg}__Consecutive Failures: {d.get('consecutive_failures')}__ "
            if is_failure:
                msg = f"{msg}]"
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
        st.write('__:material/file_json: Extraction__')
        col1, col2, col3 = st.columns(spec=[1,2,1], gap="medium", border=False)
        with col1:
            api_service_name = st.segmented_control(label='API Service',
                                            options=ss.service_list,
                                            default=d.get('api_service_name'))
            api_loop_type = st.segmented_control(label='API Loop Type',
                                                 options=['Day', 'Range', 'Next', 'N/A'],
                                                 default=d.get('api_loop_type'))
        with col3:
            interpolate_values = st.checkbox(label='Interpolate Values?',
                                             value=d.get('interpolate_values'))
            forecast_values = st.checkbox(label='Forecast? Values',
                                             value=d.get('forecast_values'))

        with col2:
            api_function_name = st.text_input(label='API Function',
                                              value=d.get('api_function_name'))
            api_parameters = st.text_input(label='API Parameters',
                                           value=d.get('api_parameters'))

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
                            api_service_name = %s,
                            api_function_name = %s,
                            api_loop_type = %s,
                            api_parameters = %s,
                            python_function = %s,
                            interpolate_values = %s,
                            forecast_values = %s
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
                      api_parameters,
                      python_function,
                      interpolate_values,
                      forecast_values,
                      int(task_id)]
            returns = qec(update_sql, params)
            st.write(returns)

    if api_service_name:
        render_staging_config(d)


    return

def render_staging_config(task_dict):
    task_id = task_dict.get('task_id')
    staging_sql = f"SELECT * FROM tasks.staging_configuration WHERE task_id = {task_id} ORDER BY staging_id"
    staging_dict = sql_to_dict(staging_sql)

    # if st.button(':material/add: Add Staging Step:')
    if staging_dict:
        render_iterative_staging(staging_dict)

    if st.button(':material/add_circle: Add new staging step'):
        inst_staging_create(task_dict)

    if sse('selected_task_id') and sse('selected_staging_id'):
        render_fact_management(ss.selected_task_id, ss.selected_staging_id)
    return


def update_staging(col_id, key_prefix, stg_d):
    key_str = f"{key_prefix}_{col_id}"
    key_val = ss.get(key_str)
    if not key_val:
        key_val = 'N/A'

    up_sql = f"""UPDATE tasks.staging_configuration SET {col_id} = %s WHERE task_id = %s and staging_id = %s"""
    params = [key_val, int(stg_d.get('task_id')), int(stg_d.get('staging_id'))]
    qec(up_sql, params)
    st.toast(f"{col_id} saved", duration=3)
    st.rerun()
    return


def render_iterative_staging(staging_dict):
    stage_count = 0
    for s in staging_dict:
        stage_count += 1
        with st.container(border=True, key=f"cont_staging_{s.get('staging_name')}"):
            msg= f":blue[#{stage_count}]: __{s.get('staging_name')}__"
            st.write(msg)
            key_prefix = f"{s.get('task_id')}_{s.get('staging_id')}_"
            st.text_input(label='Name:',
                         value=s.get('staging_name'),
                         on_change=update_staging,
                         args=('staging_name', key_prefix, s),
                         key=f"{key_prefix}_staging_name")
            st.text_area(label='Description:',
                         value=s.get('staging_description'),
                         on_change=update_staging,
                         args=('staging_description', key_prefix, s),
                         key=f"{key_prefix}_staging_description")
            st.text_input(label='Source Table:',
                          value=s.get('source_table'),
                          on_change=update_staging,
                          args=('source_table', key_prefix, s),
                          key=f"{key_prefix}_source_table")
            st.text_input(label='Destination Table:',
                          value=s.get('destination_table'),
                          on_change=update_staging,
                          args=('destination_table', key_prefix, s),
                          key=f"{key_prefix}_destination_table")
            st.text_input(label='Cross join condition:',
                          value=s.get('cross_join_condition'),
                          key=f"{key_prefix}_cross_join_condition",
                          on_change=update_staging,
                          args=('cross_join_condition', key_prefix, s))
            st.text_input(label='Filter Condition:',
                          value=s.get('filter_condition'),
                          key=f"{key_prefix}_filter_condition",
                          on_change=update_staging,
                          args=('filter_condition', key_prefix, s))
            if st.button(':material/convert_to_text: Load relevant facts', key=f'btn_{s.get("task_id")}_{s.get("staging_id")}'):
                ss.selected_task_id = s.get('task_id')
                ss.selected_staging_id = s.get('staging_id')
    return

def render_fact_management(task_id, staging_id):
    if not task_id:
        st.error('Cannot render subtasks without task_id')
        return

    if not staging_id:
        st.error('Cannot render subtasks without staging_id')
        return

    sel_query = f"""SELECT * FROM tasks.fact_configuration WHERE task_id = {task_id} and staging_id = {staging_id}"""
    ss.fact_df = pd.read_sql(sel_query, con=get_conn(alchemy=True))

    cols = ['fact_id',
            'fact_name',
            'data_type',
            'extraction_sql',
            'is_unique_constraint',
            'interpolate_values',
            'infer_values',
            'interpolation_ts',
            'interpolation_destination_table',
            'forecast_values']

    if ss.fact_df.empty:
        ss.fact_df = pd.DataFrame(columns=cols)

    col_config = {'fact_id': st.column_config.NumberColumn(label='ID',
                                                     width=40,
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
                  'interpolate_values': st.column_config.CheckboxColumn(label='Interpolate?',
                                                                          width=40,
                                                                          disabled=False),
                  'is_unique_constraint': st.column_config.CheckboxColumn(label='PK?',
                                                                          width=40,
                                                                          disabled=False),
                  'interpolation_ts': st.column_config.CheckboxColumn(label='Timestamp Column?',
                                                                  disabled=False, width="small"),
                  'infer_values': st.column_config.CheckboxColumn(label='Infer before interpolation?',
                                                                  disabled=False, width="small"),
                  'interpolation_destination_table': st.column_config.TextColumn(label='Interpolation Destination Table'),
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

    if st.button(':material/save: Save Fact Changes'):
        update_fact_df(task_id, staging_id)
    # fu = ss.get('fact_df_updates')
    # er = fu.get('edited_rows')
    # for row_index, updates in er.items():
    #     st.write(row_index)
    #     st.write(updates)
    #     for key, key_val in updates.items():
    #         st.write(key)
    #         st.write(key_val)

    st.write(ss.get('fact_df_updates'))
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