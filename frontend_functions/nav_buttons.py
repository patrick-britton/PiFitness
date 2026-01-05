import time
import streamlit as st
from streamlit import session_state as ss

def nav_dictionary():
    d = {
        # Home page
        'main':
            {"home": {'icon': "home"},
             'music': {'icon':"music_cast"},
             "running": {'icon': "sprint"},
             "food": {'icon': "local_dining"},
             "admin": {'icon':"shield_person"},

                  },

        # Admin Page
        'admin': {"passwords": {'icon': "key_vertical"},
                    "tasks": {'icon': "checklist"},
                    "services": {'icon': "api"},
                    "db_backup": {'icon': "database_upload"}
                    },

        # Music Page
        'music': {'now_playing': {'icon': 'radio', 'label': 'Now Playing'},
                    'sync_history': {'icon': 'download', 'label': 'Sync History'},
                    'tune': {'icon': 'tune', 'label': 'Playlist Config'},
                    'sync_playlists': {'icon': 'queue_music', 'label': 'Playlist Sync'},
                    'shuffle': {'icon': 'shuffle', 'label': 'Playlist Shuffle'},
                    'voting_chip': {'icon': 'voting_chip', 'label': 'Ratings'},
                    'clean_dupes': {'icon': 'cleaning_services', 'label': 'Review ISRCs'}
                    },

        # Running
        'running': {},

        'food': {}
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



def nav_button(page_name=None):
    if not page_name:
        return

    all_d = nav_dictionary()
    nav_dict = all_d.get(page_name)

    if not nav_dict:
        st.error('Navigation dictionary unassigned')
        time.sleep(5)
        return

    opts = build_options(nav_dict)
    key_val = f"key_{page_name}_nav_{ss.n_counter}"
    prior_val = f"{page_name}_current"
    st.segmented_control(label='',
                         label_visibility='hidden',
                         options=opts,
                         default=ss.get(prior_val),
                         key=key_val,
                         on_change=update_nav,
                         args=(page_name, key_val))
    return


def update_nav(pn=None, key_val=None):
    if not pn:
        return

    curr_value_var = f"{pn}_current"
    old_value = ss.get(curr_value_var)
    selected_var_name = f"{pn}_active"
    new_value = ss.get(key_val)

    if new_value != old_value:
        ss[selected_var_name] = new_value
        ss[curr_value_var] = new_value
        ss[f"{pn}_active_decode"] = decode_nav(pn)
    return


def decode_nav(pn):
    d = nav_dictionary()
    nav_dict = d.get(pn)
    btn_selection = ss[f"{pn}_active"]
    icon = btn_selection[10:-1]
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
