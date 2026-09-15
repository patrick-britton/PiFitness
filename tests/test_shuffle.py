"""000-001 T11-2 note: this is a MANUAL live script, not a test module.

Per the Bug 000-001-T08-4 recurrence-prevention rule, live scripts must not
execute at import time (pytest collection would trigger a real Spotify
autoshuffle). Run manually: python tests/test_shuffle.py
"""
from backend_functions.music_functions import auto_shuffle_playlists

if __name__ == '__main__':
    auto_shuffle_playlists()