import os
import streamlit as st
from streamlit import session_state as ss
from datetime import datetime
from backend_functions.database_functions import qec
from backend_functions.file_handlers import body_photo_path
from frontend_functions.nav_buttons import nav_widget, clear_nav_and_rerun


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
    else:
        st.info(f'Uncaught health navigation choice: {health_selection}')
    return

def render_health_charting():
    st.info('No health charting yet')
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