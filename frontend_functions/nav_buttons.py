import time
import streamlit as st
from streamlit import session_state as ss

from frontend_functions.streamlit_helpers import ss_pop


def nav_dictionary():
    d = {
        # Home page
        'main':
            {"home": {'icon': "home"},
             'music': {'icon':"music_cast"},
             "running": {'icon': "sprint"},
             "food": {'icon': "local_dining"},
             "admin": {'icon':"shield_person"},
             "health": {'icon': "cardiology"}

                  },

        # Admin Page
        'admin': {"admin_charting": {'icon': 'show_chart'},
                  "task_mgmt": {'icon': "discover_tune", 'label': 'Task Mgmt'},
                  "task_exec": {'icon': "motion_play", 'label': 'Task Exec'},
                  "passwords": {'icon': "key_vertical", 'label': 'Passwords'},
                  "services": {'icon': "api", 'label': 'API Mgmt'}
                  },

        'admin_charting': {'task_summary': {'icon': 'checklist', 'label': 'Tasks'},
                           'db_size': {'icon': 'database', 'label': 'DB Size'}},
        # Music Page
        'music': {'now_playing': {'icon': 'radio', 'label': 'Now Playing'},
                  'listen_history': {'icon': 'download', 'label': 'Sync History'},
                  'list_config': {'icon': 'tune', 'label': 'Playlist Config'},
                  'list_shuffle': {'icon': 'shuffle', 'label': 'Playlist Shuffle'},
                  'track_ratings': {'icon': 'voting_chip', 'label': 'Ratings'},
                  'isrc_clean': {'icon': 'cleaning_services', 'label': 'Review ISRCs'},
                  'sync_playlists': {'icon': 'queue_music', 'label': 'Playlist Sync'},
                    },

        # Running
        'running': {},
        # Food page
        'food': {},

        # Health Page
        'health': {'health_charting': {'icon': 'show_chart'},
                   'photo_intake': {'icon': 'photo_camera' },
                   'dimension_intake': {'icon': 'pregnancy'}}
    }
    return d

def build_options(d):
    # Builds the button options from the dictionary provided
    opts = []
    for nav_opt in d.values():
        icon = nav_opt.get("icon")
        if not icon:
            continue

        lbl = nav_opt.get("label")
        if lbl:
            opts.append(f":material/{icon}: {lbl}")
        else:
            opts.append(f":material/{icon}:")
        continue
    return opts


def update_nav(pn=None, key_val=None, custom_dict=None):
    if not pn:
        return

    curr_value_var = f"{pn}_current"
    old_value = ss.get(curr_value_var)
    selected_var_name = f"{pn}_active"
    new_value = ss.get(key_val)

    if new_value != old_value:
        ss[selected_var_name] = new_value
        ss[curr_value_var] = new_value
        ss[f"{pn}_active_decode"] = decode_nav(pn, custom_dict)
    return


def decode_nav(pn, custom_dict=None):
    if custom_dict:
        nav_dict = custom_dict
    else:
        d = nav_dictionary()
        nav_dict = d.get(pn)
    btn_selection = ss[f"{pn}_active"]
    if not btn_selection:
        return None
    icon = btn_selection.split(':material/')[1].split(':')[0]
    for key, item in nav_dict.items():
        if icon == item.get("icon"):
            return key

    return None


def inc_nav_counter():
    if "n_counter" not in ss:
        ss.n_counter =0
        return

    ss.n_counter += 1
    return


def nav_widget(nav_key, nav_title, custom_dict=None):

    nav_button(nav_key, nav_title, custom_dict)

    return ss.get(f"{nav_key}_active_decode")

def nav_button(page_name=None, nav_title=None, custom_dict=None):
    if not page_name:
        return

    if custom_dict:
        nav_dict = custom_dict
    else:
        all_d = nav_dictionary()
        nav_dict = all_d.get(page_name)

    if not nav_dict:
        st.error('Navigation dictionary unassigned')
        time.sleep(5)
        return

    if nav_title:
        nav_title = f"__{nav_title}__:"
        nav_vis = 'visible'
    else:
        nav_title = ''
        nav_vis = 'collapsed'
    opts = build_options(nav_dict)
    key_val = f"key_{page_name}_nav_{ss.n_counter}"
    prior_val = f"{page_name}_current"
    st.segmented_control(label=nav_title,
                         label_visibility=nav_vis,
                         options=opts,
                         default=ss.get(prior_val),
                         key=key_val,
                         on_change=update_nav,
                         args=(page_name, key_val, custom_dict))
    return

def clear_nav(page_name):
    var_list = [f"{page_name}_current", f"{page_name}_active_decode", f"{page_name}_active"]
    ss_pop(var_list)
    return

def clear_nav_and_rerun(page_name):
    clear_nav(page_name)
    st.rerun()
    return