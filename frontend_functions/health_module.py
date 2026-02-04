import os
import streamlit as st
from streamlit import session_state as ss, column_config
from datetime import datetime
from backend_functions.database_functions import qec, get_conn
from backend_functions.file_handlers import body_photo_path
from backend_functions.viz_factory.body_comp import render_weight_viz
from frontend_functions.nav_buttons import nav_widget, clear_nav_and_rerun
import pandas as pd

from frontend_functions.streamlit_helpers import data_editor_reconcile


def render_health_module():
    health_selection = nav_widget('health', 'Health Controls')

    if not health_selection:
        health_selection = 'health_charting'

    if health_selection == 'health_charting':
        render_health_charting()
    elif health_selection == 'photo_intake':
        render_photo_intake()
    elif health_selection == 'dimension_intake':
        render_dimension_intake()
    elif health_selection == 'weight_target':
        render_weight_target()
    else:
        st.info(f'Uncaught health navigation choice: {health_selection}')
    return

def render_weight_target():
    tgt_sql = 'SELECT ts_utc, round(weight_total_g*0.00220462,1) as weight_lb FROM health.weight_target order by ts_utc desc'
    ss.tgt_df = pd.read_sql_query(tgt_sql, con=get_conn(alchemy=True))
    cols = ['ts_utc', 'weight_lb']
    if ss.tgt_df.empty:
        st.info('No weight targets yet')
        ss.tgt_df = pd.DataFrame(columns=cols)


    st.write('Add new target:')
    date = st.date_input('Date')
    weight = st.number_input('Weight', min_value=150, max_value=300, value=None)
    if date or weight:
        if st.button(':material/save: Save New Target'):
            date = str(date)
            weight = int(round(weight/0.00220462,0))
            sql = f"""INSERT INTO health.weight_target (ts_utc, weight_total_g)
                        VALUES (%s::TIMESTAMPTZ, %s)
                        ON CONFLICT (ts_utc) DO UPDATE SET
                        weight_total_g = EXCLUDED.weight_total_g;"""
            returns = qec(sql,[date,weight])
            st.write(returns)


    # data_editor_reconcile(df_key=None, chg_key=None, dest_table=None, pk_col=None)
    return



def render_health_charting():
    col1, col2, col3 = st.columns(spec=[3,2,1], gap=None, border=False)
    with col1:
        chart_scale = st.segmented_control(label='Scale',
                                  options=['YoY', 'Last 30', 'Last 90', 'Composition'],
                                   default='YoY')
    with col2:
        chart_measure = st.segmented_control(label='Measure',
                                           options=['Total', 'Fat', 'Muscle'],
                                           default='Total')
    with col3:
        history_limit = st.number_input('Periods',
                                        min_value=1,
                                        max_value=10,
                                        value=4,
                                        step=1)


    if chart_measure == 'Total':
        yaxis = ['total_lb', 'tgt_lb']
        ylabel = 'Weight (lb)'
    elif chart_measure == 'Fat':
        yaxis = ['fat_lb']
        ylabel = 'Fat (lb)'
    elif chart_measure == 'Muscle':
        yaxis = ['muscle_lb']
        ylabel = 'Muscle (lb)'
    else:
        yaxis = ['total_lb', 'tgt_lb']
        ylabel = 'Weight (lb)'

    if chart_scale == 'YoY':
        xaxis = 'day_of_year'
        xlimit = 'relative_year'
    elif chart_scale == 'Last 30':
        xaxis = 'dm30'
        xlimit = 'relative_30'
    elif chart_scale == 'Last 90':
        xaxis = 'dm90'
        xlimit = 'relative_90'
    else:
        xaxis = 'date_val'
        xlimit = 'relative_day'
        history_limit = 90
        yaxis = ['muscle_lb', 'fat_lb', 'bone_lb', 'water_lb']

    sql = f"SELECT {xaxis}, {xlimit}"
    for y in yaxis:
        sql = f"{sql}, {y}"

    sql = f"{sql} FROM health.vw_weight_viz WHERE {xlimit} > -{history_limit} ORDER BY {xlimit} ASC"
    # st.write(sql)
    # st.write(xaxis, xlimit, yaxis, ylabel)
    render_weight_viz(sql, chart_scale, xaxis, xlimit, history_limit, yaxis, ylabel)

    return


def render_photo_intake():

    uff = st.file_uploader(label=f'__Front Image__:',
                     type=["jpg", "jpeg", "png"],
                     accept_multiple_files=False,
                     key='key_front_photo',
                     width = 400)

    if uff is None:
        return

    ufs = st.file_uploader(label=f'__Side Image__:',
                          type=["jpg", "jpeg", "png"],
                          accept_multiple_files=False,
                          key='key_side_photo',
                          width=400)

    if ufs is None:
        return

    if st.button(':material/save: Save images'):
        process_photo(uff, ufs)
        st.toast(f"Image saved successfully", duration=3)
        uff = None
        ufs = None
        st.rerun()

    return

def process_photo(front_file=None, side_file=None):

    if not front_file or not side_file:
        return

    file_list = [front_file, side_file]
    photo_type = 'front'
    for uploaded_file in [front_file, side_file]:
        # ---------- Extension extraction ----------
        _, ext = os.path.splitext(uploaded_file.name)
        ext = ext.lower().lstrip(".")

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        fn = f"{photo_type}_{timestamp}.{ext}"
        fp = body_photo_path()
        save_path = os.path.join(fp,fn)

        # ---------- Save file byte-for-byte ----------
        file_bytes = uploaded_file.getbuffer()

        with open(save_path, "wb") as f:
            f.write(file_bytes)

        ins_sql = f"""INSERT INTO health.photo_metadata (photo_type, file_name)
                        VALUES (%s, %s)"""
        params = [photo_type, fn]
        qec(ins_sql,params)
        st.toast(f"{photo_type} saved to {save_path}", duration=3)
        photo_type = 'side'

    return

def render_dimension_intake():
    st.write('__Dimension Input__')

    butt_cm = st.number_input('Butt',
                              min_value=30,
                              max_value=200, width=200, value=None)
    waist_cm = st.number_input('Waist',
                              min_value=30,
                              max_value=200, width=200, value=None)
    stomach_cm = st.number_input('Stomach',
                              min_value=30,
                              max_value=200, width=200, value=None)
    chest_cm = st.number_input('Chest',
                              min_value=30,
                              max_value=200, width=200, value=None)
    neck_cm = st.number_input('Neck',
                              min_value=30,
                              max_value=200, width=200, value=None)

    if butt_cm and waist_cm and stomach_cm and chest_cm and neck_cm:
        ins_sql = """INSERT INTO health.body_dimensions(butt_cm, waist_cm, stomach_cm, chest_cm, neck_cm)
                    VALUES (%s, %s, %s, %s, %s)"""
        params = [butt_cm, waist_cm, stomach_cm, chest_cm, neck_cm]
        qec(ins_sql, params)
        st.toast('Dimensions Saved', duration=3)
        st.balloons()
        clear_nav_and_rerun('health')
    return