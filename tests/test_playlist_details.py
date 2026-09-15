"""Manual live script — not a pytest test module (000-001 Bug T08-1).

Run explicitly with:
    python tests/test_playlist_details.py
The live Spotify sync was previously executed at *import* time, which made
`pytest tests` fail during collection with
`AttributeError: 'function' object has no attribute 'playlist_items'`
(the extractor's no-client path assigned the function object instead of the
client dict's client — fixed in 000-001 T08). Guarding the call keeps the
manual capability while leaving pytest collection green.
"""
from backend_functions.service_logins import get_spotify_client
from backend_functions.json_extractors import extract_json_playlist_details


id_val = '7LPPIzdYgJZgj2QTSXCCNy'


def main():
    extract_json_playlist_details(list_id=id_val)


if __name__ == '__main__':
    main()