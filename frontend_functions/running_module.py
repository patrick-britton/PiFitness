import time

import streamlit as st
from streamlit import session_state as ss
import pandas as pd

from backend_functions.database_functions import get_conn, qec, sql_to_dict, one_sql_result
from backend_functions.music_functions import auto_shuffle_playlists
from backend_functions.running_functions import leaderboard_update
from backend_functions.ultimate_task_executioner import ultimate_task_executioner
from backend_functions.viz_factory.leaderboards import render_leaderboard
from backend_functions.viz_factory.run_list import render_course_list
from backend_functions.viz_factory.segment_compare import render_segment_compare, get_segment_sql

from frontend_functions.nav_buttons import nav_widget
from frontend_functions.segment_creation import render_segment_creation, render_segment_matches, \
    render_segment_leaderboard, plot_route_map
from frontend_functions.streamlit_helpers import sse, ss_pop


def render_running_module():
    nav_selection = nav_widget('running', 'Run Options')
    # if nav_selection is None:
    #     render_segment_notice_widgets()

    if nav_selection == 'run_charting':
        display_last_run()
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

    # render_topo()
    return


def segment_compare():
    ss_list=['seg_dict', 'seg_list', 'man_seg_dict']
    comp_choice = st.segmented_control('What type of comparison do you want?',
                                       options=['Race',
                                                'Create Segment',
                                                'Match Activities',
                                                'Leaderboards',
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

    # elif comp_choice == 'Merge Segment':
    #     with comp_container:
    #         match_num = st.number_input('Match # to retrieve',
    #                                     min_value=1,
    #                                     max_value=10,
    #                                     step=1,
    #                                     value=1,
    #                                     on_change=reset_seg_dict)
    #         match_sql = f"""SELECT * FROM activities.vw_overlapped_segments WHERE match_rank = {match_num}"""
    #
    #         if 'seg_dict' not in ss:
    #             with st.spinner('Pulling Potential Segment Overlaps', show_time=True):
    #                 ss.seg_dict = sql_to_dict(match_sql)
    #         with st.spinner('Rendering maps', show_time=True):
    #             render_segment_compare(ss.seg_dict, comp_choice)
    elif comp_choice == 'Leaderboards':
        render_segment_leaderboard()
    elif comp_choice == 'Match Activities':
        render_segment_matches()

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

    elif comp_choice == 'Create Segment':
        render_segment_creation()

    else:
        st.info('not built yet')

    return


def reset_seg_dict():
    ss_pop(['seg_dict', 'man_seg_dict'])
    return



def course_review():
    render_course_list()
    return



def render_run_forecast():
    st.info('Run forecasting not yet built')
    return


def process_new_run():

    if ss.get('pnr_processing_complete') is None or ss.get('pnr_processing_complete') is False:
        if ss.get('pnr_pl_key') is None:
            st.segmented_control('Music?',
                                 options=['Running', 'Jogging', 'No Playlist'],
                                 key='pnr_pl_key')
            return


        if ss.get("new_run_synced") is None:
            # Sync Activities
            with st.spinner('Making sure I have all known activities', show_time=True):
                ultimate_task_executioner(force_task_id=4)

            # Sync Activity Details
            with st.spinner('Grabbing activity Details', show_time=True):
                ultimate_task_executioner(force_task_id=19)
                time.sleep(0.5)

            # Smooth and match activities
            with st.spinner('Matching Segments', show_time=True):
                ultimate_task_executioner(force_task_id=21)
            ss.new_run_synced = True



        if ss.get('pnr_pl_key') != 'No Playlist':
            sel_sql = f"""SELECT * FROM activities.vw_watch_music_heard WHERE playlist_name = '{ss.get('pnr_pl_key')}'""";
            df = pd.read_sql(sel_sql, get_conn(alchemy=True))
            song_count = len(df)
            first_song = df['track_name_clean'].iloc[0]
            last_song = df['track_name_clean'].iloc[song_count - 1]
            st.write(f"You heard __{song_count}__ songs: __{first_song}__ to __{last_song}__")

            with st.spinner('Inserting history & reshuffling playlist...', show_time=True):
                cols = ['played_at_utc', 'isrc', 'playlist_id']
                narrow_df = df[cols]
                narrow_df.to_sql(schema='music', name='temp_listening_history', con=get_conn(alchemy=True), if_exists='replace',
                                 index=False)
                reconcile_sql = """INSERT INTO music.listening_history (
                played_at_utc, isrc, playlist_id)
                SELECT played_at_utc::TIMESTAMPTZ, isrc, playlist_id
                FROM music.temp_listening_history
                ON CONFLICT(played_at_utc, isrc) DO NOTHING;"""
                qec(reconcile_sql)
                target_id = narrow_df['playlist_id'].iloc[0]
                auto_shuffle_playlists(target_id, limit_minutes=True)

        ss.pnr_processing_complete = True
        st.success('Processing complete!')
        st.balloons()

    return

def display_last_run():
    opt = st.segmented_control('Display:',
                               options=['Last Run', 'Last Activity'],
                               key='dlr_type_choice',
                               on_change=ss_pop,
                               args=(['aid', 'sm_leaderboard_df', 'sm_seg_df', 'df_race','summary_df'],))

    typ = ss.get('dlr_type_choice')
    if typ == 'Last Run':
            ss.aid = one_sql_result("""SELECT MAX(activity_id) FROM activities.activities where activity_type_name like '%run%'""")
    elif typ == 'Last Activity':
        ss.aid = one_sql_result(
                """SELECT MAX(activity_id) FROM activities.activities""")

    if not sse('aid'):
        return

    display_activity()
    return

def display_activity():
    if not sse('aid'):
        st.warning('No activity id specified, need to build selection')
        return
    act_sql = f"""SELECT * from activities.vw_activity_summary where activity_id = {ss.aid}"""


    if not sse('summary_df'):
        with st.spinner('Loading activity stats...', show_time=True):
            ss.summary_df = pd.read_sql(act_sql, get_conn(alchemy=True))

    was_course = ss.summary_df['is_course'].iloc[0]
    if was_course:
        effort_name = ss.summary_df['segment_name'].iloc[0]
        dist_str = ss.summary_df['segment_distance_mi'].iloc[0]
        pace_str = ss.summary_df['segment_pace'].iloc[0]
    else:
        effort_name = ss.summary_df['activity_start_utc'].iloc[0]
        dist_str = ss.summary_df['distance_mi'].iloc[0]
        pace_str = ss.summary_df['activity_pace'].iloc[0]

    st.write(f""":blue[__{effort_name}__] : __{dist_str}__ miles @ __{pace_str}/mi__""")

    # st.write(was_course)
    if was_course:

        delta_pace_r = ss.summary_df['prior_delta_s'].iloc[0]
        if delta_pace_r > 0:
            pace_dir = 'seconds faster'
            delta_pace_r = abs(delta_pace_r)
            color='blue'
        else:
            pace_dir = 'seconds slower'
            color='red'
        msg1 = f":material/route: :{color}[__{delta_pace_r} {pace_dir}__] than your last attempt"

        delta_pace_r = ss.summary_df['best_delta_s'].iloc[0]
        all_time_rank = ss.summary_df['all_time_rank'].iloc[0]
        if int(all_time_rank) == 1:
            msg2 = f"and # 1 all time!"
        else:
            msg2 = f"and :red[__{abs(delta_pace_r)} seconds__] behind your best effort (#{all_time_rank} all time)."

        st.write(f"{msg1} {msg2}")
    seg_dict = {}
    seg_count = ss.summary_df['segment_id'].nunique()
    if seg_count > 0:
        if was_course and seg_count > 0:
            st.write(f"__{seg_count-1}__ additional segments crossed:")
        elif seg_count > 0:
            st.write(f"__{seg_count}__ segments detected:")
        else:
            return

        display_msg = ''
        for idx, row in ss.summary_df.iterrows():
            seg_name = row['segment_name']
            seg_id = row['segment_id']
            seg_dict[seg_name] = seg_id
            if row['is_course']:
                continue

            # st.write(row)
            delta_pace_r = row['prior_delta_s']
            if delta_pace_r > 0:
                pace_dir = 's faster'
                delta_pace_r = delta_pace_r
                color = 'blue'
            else:
                pace_dir = 's slower'
                color = 'red'
            msg1 = f":{color}[__{delta_pace_r}__ {pace_dir}] than your last attempt"

            delta_pace_r = row['best_delta_s']
            all_time_rank = row['all_time_rank']
            if int(all_time_rank) == 1:
                msg2 = f"and # 1 all time!"
            else:
                msg2 = f"and :red[__{abs(delta_pace_r)} s__] behind your best effort (#{all_time_rank} all time)."
            seg_msg = f"__:material/conversion_path: {seg_name}__: {msg1} {msg2}"
            display_msg = f"{display_msg}  \n{seg_msg}"

        st.write(display_msg)
        seg_list = list(seg_dict)
        st.segmented_control('Leaderboard Display:',
                             options=seg_list,
                             default=seg_list[0],
                             key='ad_seg_ldr_value',
                             on_change=ss_pop,
                             args=('sm_leaderboard_df',))
        # st.write(seg_dict.get(ss.get('ad_seg_ldr_value')))
        leaderboard_update(segment_id=seg_dict.get(ss.get('ad_seg_ldr_value')))
        leader_sql = f"""SELECT * FROM activities.vw_segment_leaderboard WHERE segment_id = 
                    {seg_dict.get(ss.get('ad_seg_ldr_value'))}"""
        if not sse('sm_leaderboard_df'):
            ss.sm_leaderboard_df = pd.read_sql(leader_sql, con=get_conn(alchemy=True))
        w = int(700)
        h = int(900 * (w / 1600))

        st.write(f"__Leaderboard for {ss.get('ad_seg_ldr_value')}__")
        render_leaderboard(ss.sm_leaderboard_df)
    else:
        st.info('Create a segment/course to create a leaderboard')

    if st.button(':material/reset_settings: Reset'):
        ss_pop(['aid', 'sm_leaderboard_df', 'sm_seg_df', 'df_race','summary_df'])
        st.rerun()
    st.caption(f"Activity ID# {ss.aid}")
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



