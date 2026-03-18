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

from frontend_functions.nav_buttons import nav_widget
from frontend_functions.segment_creation import render_segment_creation, render_segment_matches
from frontend_functions.streamlit_helpers import sse, ss_pop


def render_running_module():
    nav_selection = nav_widget('running', 'Run Options')
    # if nav_selection is None:
    #     render_segment_notice_widgets()

    if nav_selection == 'run_charting':
        display_last_run()
    elif nav_selection == 'leaderboards':
        display_segment_leaderboard()
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

    st.caption(nav_selection)
    return

def display_segment_leaderboard():
    get_segment_id_for_leaderboard()

    if not sse('seg_id_df'):
        return

    if ss.seg_id_df.empty:
        st.info('No Segments matching criteria')
        return

    display_cols = ['segment_name', 'last_effort', 'distance_mi', 'matched_activity_count', 'elevation_gain']
    max_dist = int(ss.seg_id_df['distance_mi'].max())
    max_elev = int(ss.seg_id_df['elevation_gain'].max())
    max_count = int(ss.seg_id_df['matched_activity_count'].max())
    min_dist = int(ss.seg_id_df['distance_mi'].min())
    min_elev = int(ss.seg_id_df['elevation_gain'].min())
    min_count = int(ss.seg_id_df['matched_activity_count'].min())

    if max_dist == min(min_dist, 0):
        max_dist += 1
    if max_elev == min(min_elev,0):
        max_elev += 1
    if max_count == min(min_count,0):
        max_count += 1

    display_confg = {'segment_name': st.column_config.TextColumn('Name'),
                     'last_effort': st.column_config.DateColumn('Last Effort',
                                                                format='yyyy-MMM-DD'),
                     'distance_mi': st.column_config.ProgressColumn('Distance (mi)',
                                                                    min_value=0,
                                                                    max_value = max_dist,
                                                                    format='%.1f'),
                     'matched_activity_count': st.column_config.ProgressColumn('Attempts',
                                                                    min_value=0,
                                                                    max_value = max_count,
                                                                    format='%d'),
                     'elevation_gain': st.column_config.ProgressColumn('Ascent (m)',
                                                                    min_value=0,
                                                                    max_value = max_elev,
                                                                    format='%d')}
    st.dataframe(ss.seg_id_df,
                 column_config=display_confg,
                 column_order=display_cols,
                 key='sl_sel_df',
                 selection_mode='single-row',
                 on_select='rerun')

    if len(ss.get('sl_sel_df').get('selection').get('rows')) == 0:
        return

    idx = ss.get('sl_sel_df').get('selection').get('rows')[0]
    ss.sl_id_val = ss.seg_id_df['segment_id'].iloc[idx]
    ss.seg_name = ss.seg_id_df['segment_name'].iloc[idx]
    st.write(ss.sl_id_val)

    if st.button(f'Generate Leaderboard for {ss.seg_name}'):

        leaderboard_update(segment_id=ss.sl_id_val)
        leader_sql = f"""SELECT * FROM activities.vw_segment_leaderboard WHERE segment_id = 
                    {ss.sl_id_val}"""
        ss.sm_leaderboard_df = pd.read_sql(leader_sql, con=get_conn(alchemy=True))
    w = int(700)
    h = int(900 * (w / 1600))

    if not sse('sm_leaderboard_df'):
        return

    elif ss.sm_leaderboard_df.empty:
        return
    else:
        st.write(f"__Leaderboard for {ss.seg_name}__")
        render_leaderboard(ss.sm_leaderboard_df)

        if st.button('Reset Leaderboard'):
            ss_pop(['seg_id_df', 'sm_leaderboard_df', 'seg_id_df', 'seg_name'])
            st.rerun()
    return



def segment_compare():
    ss_list=['seg_dict', 'seg_list', 'man_seg_dict']
    comp_choice = st.segmented_control('Option?',
                                       label_visibility='collapsed',
                                       options=['Create Segment',
                                                'Match Activities'],
                                       on_change=ss_pop,
                                       args=(ss_list,))

    if not comp_choice:
        return

    comp_container = st.container(width=800)

    if comp_choice == 'Match Activities':
        render_segment_matches()
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
    sql = "SELECT * FROM activities.vw_pr_hunter ORDER BY prestige_score desc"
    df = pd.read_sql(sql, con=get_conn(alchemy=True))
    if ss.is_mobile:
        col_ord = ['attempt_label',
               'segment_name',
               'weight_delta',
                   'vo2_delta']
    else:
        col_ord = ['attempt_label',
                   'segment_name',
                   'vs_date',
                   'prestige_score',
                   'weight_delta',
                   'vo2_delta',
                   'acute_load_delta',
                   'load_pct_delta']

    cfg = {'attempt_label':st.column_config.TextColumn('PR'),
               'segment_name': st.column_config.TextColumn('Course'),
               'vs_date': st.column_config.DateColumn('Date',
                                                      format='yyyy-MMM-DD'),
               'prestige_score': st.column_config.ProgressColumn('#',
                                                                 min_value=0,
                                                                 max_value=df['prestige_score'].max(),
                                                                 format='%f'),
               'weight_delta': st.column_config.ProgressColumn('Weight',
                                                                 min_value=0,
                                                                 max_value=df['prestige_score'].max(),
                                                                 format='%f'),
               'vo2_delta': st.column_config.ProgressColumn('VO2',
                                                                 min_value=df['vo2_delta'].min(),
                                                                 max_value=df['vo2_delta'].max(),
                                                                 format='%f'),
               'acute_load_delta': st.column_config.ProgressColumn('Load',
                                                                 min_value=df['acute_load_delta'].min(),
                                                                 max_value=df['acute_load_delta'].max(),
                                                                 format='%f'),
               'load_pct_delta': st.column_config.ProgressColumn('Load%',
                                                                 min_value=df['load_pct_delta'].min(),
                                                                 max_value=df['load_pct_delta'].max(),
                                                                 format='%f')}

    st.dataframe(df, column_config=cfg, column_order=col_ord, hide_index=True)
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



def get_segment_id_for_leaderboard():
    sql = """SELECT * FROM activities.vw_segments_effort_stats"""


    nm_col, course_col,  dist_col, type_col = st.columns(spec=[3,2,2,4], border=False, gap="small")

    pop_list = ['seg_id_df']

    with nm_col:
        st.text_input('Name:',
                             key='sl_seg_name',
                      value=None,
                             on_change=ss_pop,
                             args=(pop_list,))

    with course_col:
        st.segmented_control('Course/Segment',
                                         options=['Course', 'Segment'],
                                         key='sl_seg_is_course',
                                   on_change=ss_pop,
                                   args=(pop_list,))

    with dist_col:
        st.number_input('Min Distance (mi)',
                                   min_value=0,
                                   value=0,
                                   max_value=50,
                                   step=1,
                                   key='sm_min_dist_val',
                                   on_change=ss_pop,
                                   args=(pop_list,))
    with type_col:
        st.segmented_control(label='Select Activity Type',
                             default=None,
                             key='sm_act_type',
                             options=['Run', 'Trail Run', 'Hike', 'Walk', 'Bike', 'Ski'],
                                   on_change=ss_pop,
                                   args=(pop_list,))

    if ss.get('sl_seg_name') is not None and len(ss.get('sl_seg_name')) > 1:
        f = f"{sql} WHERE LOWER(segment_name) LIKE '%{ss.get('sl_seg_name').lower()}%'"
    else:
        f = f"{sql} WHERE 1=1"

    if ss.get('sm_seg_is_course') == 'Course':
        f = f"{f} AND is_course"
    elif ss.get('sm_seg_is_course') == 'Segment':
        f = f"{f} AND NOT is_course"

    if ss.get('sm_min_dist_val') >0:
        f= f"{f} AND distance_mi >= {int(ss.get('sm_min_dist_val'))}"

    act = ss.get('sm_act_type')
    if act == 'Run':
        f = f"{f} and activity_type_name like '%run%' and activity_type name not like '%trail%'"
    elif act == 'Trail Run':
        f = f"{f} and activity_type_name like '%trail%run%' "
    elif act == 'Hike':
        f = f"{f} and activity_type_name like '%hik%' "
    elif act == 'Walk':
        f = f"{f} and activity_type_name like '%walk%' "
    elif act == 'Bike':
        f = f"{f} and activity_type_name like '%bik%' "
    elif act == 'Ski':
        f = f"{f} and activity_type_name like '%ski%' "


    # st.code(f, language='sql')
    if not sse('seg_id_df'):
        ss.seg_id_df = pd.read_sql(sql, con=get_conn(alchemy=True))
    return

