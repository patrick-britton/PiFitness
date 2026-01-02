




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



    #          button_dict = {
    #     ":material/radio: Now Playing": "now_playing",
    #     ":material/download: Get Listen History": "sync_history",
    #     ":material/queue_music: Synchronize Playlists": "sync_playlists",
    #     ":material/tune: Configure Playlists": "playlist_config",
    #     ":material/voting_chip: Ratings": "ratings",
    #     ":material/shuffle: Playlist Shuffle": "shuffle",
    #     ":material/cleaning_services: Clean Dupes": "dupes"
    # }
    #


    d = {':material/home:': 'home',
         ':material/music_cast:': 'music',
             ':material/key:': 'passwords',
             ':material/shield_person:': 'admin',
         'food': 'calories', # Display eating, track new calories
         ':material/scale:': 'weight', # Sync & display weight, manage targets
         ':material/watch:': 'activity_sync', # sync recent activities
         'running': 'running', # Reconcile new run (music), # Preview a run, top 10 lists, training load, vo2 max
         'health': 'health'  # Resting heart rates, sleep,
         }