import streamlit as st
from streamlit import session_state as ss
import pandas as pd

from backend_functions.database_functions import get_conn, qec, sql_to_dict, one_sql_result
from backend_functions.task_execution import task_executioner
from backend_functions.viz_factory.run_list import render_course_list
from backend_functions.viz_factory.segment_compare import render_segment_compare, get_segment_sql
from frontend_functions.music_module import render_playlist_shuffle
from frontend_functions.music_widgets import playlist_config_table
from frontend_functions.nav_buttons import nav_widget
from frontend_functions.streamlit_helpers import sse, ss_pop


def render_running_module():
    nav_selection = nav_widget('running', 'Run Options')

    render_segment_notice_widgets()

    # if nav_selection is None:
    #     render_segment_notice_widgets()

    if nav_selection == 'run_charting':
        render_run_charting()
    elif nav_selection == 'new_run_process':
        process_new_run()
    elif nav_selection == 'run_forecast':
        render_run_forecast()
    elif nav_selection == 'course_review':
        course_review()
    elif nav_selection == 'segment_compare':
        segment_compare()
    else:
        st.info('Select an option above')
    return


def segment_compare():
    ss_list=['seg_dict', 'seg_list', 'man_seg_dict']
    comp_choice = st.segmented_control('What type of comparison do you want?',
                                       options=['Race',
                                                'Verify Segment',
                                                'Merge Segment',
                                                'Manual Segment Compare',
                                                'Course Review'],
                                       on_change=ss_pop,
                                       args=(ss_list,))

    if not comp_choice:
        return

    comp_container = st.container(width=800)

    if comp_choice == 'Race':
        sql = "SELECT * FROM activities.vw_segment_racing"
        df = pd.read_sql(sql, get_conn(alchemy=True))
        if df.empty:
            st.info('No Matches found')
            return

        dist_max = int(df['distance_mi'].max())
        effort_max = int(df['effort_count'].max())
        col_keep = ['segment_name', 'distance_mi','effort_count', 'last_effort']
        col_config = {'segment_name': st.column_config.TextColumn(label='Segment Name',
                                                                  disabled=True
                                                                  ),
                      'distance_mi': st.column_config.ProgressColumn('Miles',
                                                                     min_value=0,
                                                                     max_value=dist_max,
                                                                     format='%.1d'),
                      'effort_count': st.column_config.ProgressColumn('Attempts',
                                                                     min_value=0,
                                                                     max_value=effort_max,
                                                                     format='%d'),
                      'last_effort': st.column_config.DatetimeColumn(label='Last Effort',
                                                                     format='distance')
                      }
        st.dataframe(df, column_order=col_keep, column_config=col_config,
                     hide_index=True, selection_mode='single-row', key='kv_segment_selection',
                     on_select='rerun')



        sel_dict = ss.get('kv_segment_selection').get('selection').get('rows')
        if sel_dict:
            sel = df['segment_id'].iloc[sel_dict[0]]
            st.write(sel)
            leaderboard_sql = f"""SELECT
                                sl.segment_id,
                                sl.segment_name,
                                sl.activity_id,
                                sl.distance_mi,
                                sl.elapsed_time_s,
                                sl.all_time_rank,
                                sl.last_365_rank,
                                sl.current_cycle_rank,
                                sl.effort_label,
                                sm.activity_path
                                FROM activities.vw_segment_leaderboard sl
                                    INNER JOIN activities.segment_matches sm on sm.segment_id = {sel} and sm.activity_id = sl.activity_id
                                where sl.segment_id ={sel}
                                and effort_label != 'other'"""

            with comp_container:
                with st.spinner('Pulling Race Comparisons', show_time=True):
                    render_segment_compare(sql_to_dict(leaderboard_sql), comp_choice)

    elif comp_choice == 'Merge Segment':
        with comp_container:
            match_num = st.number_input('Match # to retrieve',
                                        min_value=1,
                                        max_value=10,
                                        step=1,
                                        value=1,
                                        on_change=reset_seg_dict)
            match_sql = f"""SELECT * FROM activities.vw_overlapped_segments WHERE match_rank = {match_num}"""

            if 'seg_dict' not in ss:
                with st.spinner('Pulling Potential Segment Overlaps', show_time=True):
                    ss.seg_dict = sql_to_dict(match_sql)
            with st.spinner('Rendering maps', show_time=True):
                render_segment_compare(ss.seg_dict, comp_choice)
    elif comp_choice == 'Manual Segment Compare':
        id1_col, id2_col = st.columns(spec=[1,1], gap="small", border=False)
        with id1_col:
            id1 = st.number_input('Segment id #1',
                                  min_value=1,
                                  value=None,
                                  on_change=reset_seg_dict)
        with id2_col:
            id2 = st.number_input('Segment id #2',
                                  min_value=1,
                                  value=None,
                                  on_change=reset_seg_dict)
        if id1 and id2:
            if 'man_seg_dict' not in ss:
                with st.spinner('Pulling Potential Segment Overlaps', show_time=True):
                    ss.man_seg_dict = sql_to_dict(get_segment_sql(id1, id2))
            with st.spinner('Rendering maps', show_time=True):
                render_segment_compare(ss.man_seg_dict, 'Merge Segment')
    elif comp_choice == 'Verify Segment':
        v_sql = """SELECT * FROM activities.vw_possible_segment_matches WHERE match_rank = 1"""
        render_segment_compare(sql_to_dict(v_sql), comp_choice)

    elif comp_choice == 'Course Review':
        name_col, min_dist_col, max_dist, item_col = st.columns(spec=[2,1,1,1], gap="small", border=False)

        with name_col:
            name_str = st.text_input('Search by Name',
                                     value='',
                                     on_change=ss_pop,
                                     args=['seg_list'])
        with min_dist_col:
            min_dist = st.number_input('Minimum Distance',
                                       value=0,
                                       min_value=0,
                                       max_value=200,
                                       step=1,
                                       on_change=ss_pop,
                                       args=['seg_list']
                                       )
        with max_dist:
            max_dist = st.number_input('Maximum Distance',
                                       value=200,
                                       min_value=1,
                                       max_value=500,
                                       step=1,
                                       on_change=ss_pop,
                                       args=['seg_list']
                                       )

        sel_sql = f"""SELECT * FROM activities.vw_course_review
                    WHERE lower(segment_name) like LOWER('%{name_str}%')
                    AND distance_mi >= {int(min_dist)} and distance_mi < {int(max_dist)}"""

        if not sse('seg_list'):
            ss.seg_list = sql_to_dict(sel_sql)

        with item_col:
            max_id = ss.seg_list[0].get('max_course_idx')
            course_item = st.number_input('Course #',
                                          min_value=1,
                                          max_value=max_id,
                                          key=f"key_course_{name_str}_{max_id}_{min_dist}_{max_dist}",
                                          value=1)

        # st.write(f"Course #: {course_item}, {[ss.seg_list[course_item-1]]}")
        render_segment_compare([ss.seg_list[course_item-1]], comp_choice)



    else:
        st.info('not built yet')

    return


def reset_seg_dict():
    ss_pop(['seg_dict', 'man_seg_dict'])
    return



def course_review():
    render_course_list()
    return



def render_run_charting():
    st.info('Run Charting not yet built')
    return


def render_run_forecast():
    st.info('Run forecasting not yet built')
    return


def process_new_run():

    # Sync All activities
    if ss.get("new_run_synced") is None:
        with st.spinner('Making sure I have all known activities', show_time=True):
            task_executioner(force_task_name='Sync Garmin Activities', force_task=True)
            qec("REFRESH MATERIALIZED VIEW activities.vw_run_timing")
            ss.new_run_synced = True


    # Get most recent activity details
    if ss.get("listens_df") is None:
        sel_sql = """SELECT * FROM activities.vw_watch_music_heard;"""
        ss.listens_df = pd.read_sql(sel_sql, get_conn(alchemy=True))

    df = ss.listens_df.copy()
    options = df['playlist_name'].unique().tolist()
    options.append('No playlist')

    sel = st.segmented_control(label='Which playlist did you listen to?',
                               options=options,
                               key='sc_pl_selection')
    if not sel:
        st.write(':gray[*make your selection above*]')
        return

    if sel == 'No playlist':
        st.info('What did you even come here for?')
        return


    filtered_df = df[df['playlist_name'] == sel].copy()
    cols = ['track_order', 'track_name_clean', 'artist_display_name', 'played_at_utc']
    col_config = {'track_order': st.column_config.NumberColumn(label='#',
                                                               pinned=True,
                                                               disabled=True,
                                                               format='%d'),
                'track_name_clean': st.column_config.TextColumn(label='Title',
                                                                pinned=False,
                                                                disabled=True),
                  'artist_display_name': st.column_config.TextColumn(label='Artist',
                                                                     pinned=False,
                                                                     disabled=True),
                  'played_at_utc': st.column_config.DatetimeColumn(label='Played At',
                                                                   format='distance',
                                                                   pinned=False,
                                                                   disabled=True)
                  }

    st.write('You heard these songs')
    st.dataframe(filtered_df, column_order=cols, column_config=col_config, hide_index=True, on_select='ignore')
    cols = ['played_at_utc', 'isrc', 'playlist_id']
    if st.button(':material/database_upload: Insert into listening history'):
        with st.spinner('Loading to SQL', show_time=True):
            narrow_df = filtered_df[cols]
            narrow_df.to_sql(schema='music', name='listening_history', con=get_conn(alchemy=True), if_exists='append', index=False )
            st.toast(f"{len(narrow_df)} tracks uploaded to SQL", duration=3)
            ss.rp_new_order = True
            ss.target_id = narrow_df['playlist_id'].iloc[0]

    if not ss.get("rp_new_order"):
        return

    st.write('New playlist order will be:')
    render_playlist_shuffle(list_id=ss.target_id)

    return


def render_segment_notice_widgets():
    sql = """SELECT COUNT(*) FROM activities.segment_matches where not match_confirmed"""

    need_confirm = int(one_sql_result(sql))

    if need_confirm == 1:
        st.info('1 Segment needs confirmation')
    elif need_confirm >0:
        st.info(f"{need_confirm} Segments need confirmation")

    sql = """SELECT COUNT(*) FROM activities.segments where segment_name like 'New%';"""

    new_courses = int(one_sql_result(sql))
    if new_courses == 1:
        st.info('1 New Course')
    elif new_courses >0:
        st.info(f"{new_courses} New Courses")

    sql = """SELECT COUNT(*) FROM activities.overlap_queue;"""

    overlaps = int(one_sql_result(sql))
    if overlaps == 1:
        st.info('1 Overlap to review')
    elif overlaps > 0:
        st.info(f"{new_courses} Segments with overlaps")


    if need_confirm + new_courses + overlaps == 0:
        st.info('No segments to review')
    return



