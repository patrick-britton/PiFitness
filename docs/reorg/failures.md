#### Current examples of the failures from the test script:

## Terminal dump:

```
mod = importlib.import_module(s)
                    parts.pop(0)
                    modname = s
                except ImportError:
                    break
        # if we reach this point, mod is the module, already imported, and
        # parts is the list of parts in the object hierarchy to be traversed, or
        # an empty list if just the module is wanted.
        result = mod
        for p in parts:
>           result = getattr(result, p)
                     ^^^^^^^^^^^^^^^^^^
E           AttributeError: module 'backend.api' has no attribute 'music'

..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\pkgutil.py:473: AttributeError
________________________________ test_rescan_route_runs_off_the_event_loop _________________________________

    def test_rescan_route_runs_off_the_event_loop():
        # T09 / Bug 008-005-T08-1: the blocking ~25 s CALL must run in FastAPI's
        # threadpool, never awaited on the event loop. Starlette dispatches
        # non-coroutine endpoints via run_in_threadpool, so the registered handler
        # must be a plain sync function.
        #
        # NOTE: app.routes cannot be introspected here — this FastAPI version stores
        # included routers as lazy `_IncludedRouter` wrappers with no reachable
        # routes. So the music router itself is checked; the URL contract is covered
        # separately by the TestClient tests above (200/500 on this same path).
>       from backend.api.music import router as music_router
E       ModuleNotFoundError: No module named 'backend.api.music'

tests\test_isrc_dupe_endpoints.py:179: ModuleNotFoundError
_______________________________ test_verify_spotify_step_exists_in_pipeline ________________________________

    def test_verify_spotify_step_exists_in_pipeline():
        """The verify_spotify re-read step must be wired into the pipeline between
        send_to_spotify and report_shuffle (FR-8.6), with the frontend contract
        including the id and a label."""
>       src = SERVICE_PY.read_text(encoding='utf-8')
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

tests\test_verify_spotify_isrc_refresh.py:61: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\pathlib\__init__.py:787: in read_text
    with self.open(mode='r', encoding=encoding, errors=errors, newline=newline) as f:
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = WindowsPath('C:/Users/rickb/vsProjects/PiFitness/pifitness/activities/services.py'), mode = 'r'
buffering = -1, encoding = 'utf-8', errors = None, newline = None

    def open(self, mode='r', buffering=-1, encoding=None,
             errors=None, newline=None):
        """
        Open the file pointed to by this path and return a file object, as
        the built-in open() function does.
        """
        if "b" not in mode:
            encoding = io.text_encoding(encoding)
>       return io.open(self, mode, buffering, encoding, errors, newline)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E       FileNotFoundError: [Errno 2] No such file or directory: 'C:\\Users\\rickb\\vsProjects\\PiFitness\\pifitness\\activities\\services.py'

..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\pathlib\__init__.py:771: FileNotFoundError
_________________________________ test_verify_spotify_fails_without_client _________________________________

name = 'backend_functions.activities_service', package = None

    def import_module(name, package=None):
        """Import a module.
    
        The 'package' argument is required when performing a relative import. It
        specifies the package to use as the anchor point from which to resolve the
        relative import to an absolute import.
    
        """
        level = 0
        if name.startswith('.'):
            if not package:
                raise TypeError("the 'package' argument is required to perform a "
                                f"relative import for {name!r}")
            for character in name:
                if character != '.':
                    break
                level += 1
>       return _bootstrap._gcd_import(name[level:], package, level)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E       ModuleNotFoundError: No module named 'backend_functions.activities_service'

..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\importlib\__init__.py:88: ModuleNotFoundError

The above exception was the direct cause of the following exception:

monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x000002BF80170FA0>

    def test_verify_spotify_fails_without_client(monkeypatch):
        """With no usable client the step must raise instead of soft-succeeding
        (the original T10-6 bug silently no-oped)."""
        from pifitness.activities.services import verify_spotify
    
        for broken in (None, {"token": "x", "token_age": 0, "client": None}):
>           monkeypatch.setattr("backend_functions.activities_service.get_spotify_client", lambda b=broken: b)

tests\test_verify_spotify_isrc_refresh.py:110: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
.venv\Lib\site-packages\_pytest\monkeypatch.py:107: in derive_importpath
    target = resolve(module)
             ^^^^^^^^^^^^^^^
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

name = 'backend_functions.activities_service'

    def resolve(name: str) -> object:
        # Simplified from zope.dottedname.
        parts = name.split(".")
    
        used = parts.pop(0)
        found: object = importlib.import_module(used)
        for part in parts:
            used += "." + part
            try:
                found = getattr(found, part)
            except AttributeError:
                pass
            else:
                continue
            # We use explicit un-nesting of the handling block in order
            # to avoid nested exceptions.
            try:
                importlib.import_module(used)
            except ImportError as ex:
                expected = str(ex).split()[-1]
                if expected == used:
                    raise
                else:
>                   raise ImportError(f"import error in {used}: {ex}") from ex
E                   ImportError: import error in backend_functions.activities_service: No module named 'backend_functions.activities_service'

.venv\Lib\site-packages\_pytest\monkeypatch.py:88: ImportError
============================================= warnings summary =============================================
tests/test_verify_spotify_isrc_refresh.py::test_spotify_client_is_usable
tests/test_verify_spotify_isrc_refresh.py::test_verify_spotify_refreshes_playlist_isrcs
tests/test_verify_spotify_isrc_refresh.py::test_stored_order_matches_spotify
  C:\Users\rickb\vsProjects\PiFitness\.venv\Lib\site-packages\spotipy\oauth2.py:307: DeprecationWarning: Specifying cache_path or username as arguments to SpotifyOAuth will be deprecated. Instead, please create a CacheFileHandler instance with the desired cache_path and username and pass it to SpotifyOAuth as the cache_handler. For example:
  
        from spotipy.oauth2 import CacheFileHandler
        handler = CacheFileHandler(cache_path=cache_path, username=username)
        sp = spotipy.SpotifyOAuth(client_id, client_secret, redirect_uri, cache_handler=handler)
    warnings.warn("Specifying cache_path or username as arguments to SpotifyOAuth " +

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
========================================= short test summary info ==========================================
FAILED tests/test_food_search_backend.py::test_search_returns_trimmed_saved_first_results - AttributeError: <fastapi.routing.APIRouter object at 0x000002BF9F2E7BF0> has no attribute 'search_saved_...
FAILED tests/test_food_search_backend.py::test_search_suppresses_usda_hit_when_fdcid_saved - AttributeError: <fastapi.routing.APIRouter object at 0x000002BF9F2E7BF0> has no attribute 'search_saved_...
FAILED tests/test_food_search_backend.py::test_rename_persisted_then_duplicate_routes_to_existing - AttributeError: <fastapi.routing.APIRouter object at 0x000002BF9F2E7BF0> has no attribute 'persist_api_f...
FAILED tests/test_food_search_backend.py::test_persist_on_log_creates_food_then_entry - AttributeError: <fastapi.routing.APIRouter object at 0x000002BF9F2E7BF0> has no attribute 'persist_api_f...
FAILED tests/test_food_search_backend.py::test_log_api_entry_persists_feed_serving - AttributeError: <fastapi.routing.APIRouter object at 0x000002BF9F2E7BF0> has no attribute 'persist_api_f...
FAILED tests/test_food_search_backend.py::test_persist_on_log_requires_payload - AttributeError: 'APIRouter' object has no attribute 'router'. Did you mean: 'route'?
FAILED tests/test_food_search_backend.py::test_confirm_logs_picked_rank - AttributeError: <fastapi.routing.APIRouter object at 0x000002BF9F2E7BF0> has no attribute 'log_diary_entry'
FAILED tests/test_food_search_backend.py::test_confirm_without_pick_does_not_log - AttributeError: <fastapi.routing.APIRouter object at 0x000002BF9F2E7BF0> has no attribute 'log_diary_entry'
FAILED tests/test_isrc_dupe_endpoints.py::test_count_returns_pair_count - AttributeError: module 'backend.api' has no attribute 'music'
FAILED tests/test_isrc_dupe_endpoints.py::test_match_shapes_row_to_camel_case - AttributeError: module 'backend.api' has no attribute 'music'
FAILED tests/test_isrc_dupe_endpoints.py::test_match_empty_queue_returns_null - AttributeError: module 'backend.api' has no attribute 'music'
FAILED tests/test_isrc_dupe_endpoints.py::test_decision_accept_calls_helper_and_confirms - AttributeError: module 'backend.api' has no attribute 'music'
FAILED tests/test_isrc_dupe_endpoints.py::test_decision_reject_calls_helper_and_confirms - AttributeError: module 'backend.api' has no attribute 'music'
FAILED tests/test_isrc_dupe_endpoints.py::test_rescan_runs_finder_and_reports_count - AttributeError: module 'backend.api' has no attribute 'music'
FAILED tests/test_isrc_dupe_endpoints.py::test_rescan_empty_reports_no_dupes - AttributeError: module 'backend.api' has no attribute 'music'
FAILED tests/test_isrc_dupe_endpoints.py::test_decision_write_failure_returns_500 - AttributeError: module 'backend.api' has no attribute 'music'
FAILED tests/test_isrc_dupe_endpoints.py::test_rescan_failure_returns_500 - AttributeError: module 'backend.api' has no attribute 'music'
FAILED tests/test_isrc_dupe_endpoints.py::test_rescan_route_runs_off_the_event_loop - ModuleNotFoundError: No module named 'backend.api.music'
FAILED tests/test_verify_spotify_isrc_refresh.py::test_verify_spotify_step_exists_in_pipeline - FileNotFoundError: [Errno 2] No such file or directory: 'C:\\Users\\rickb\\vsProjects\\PiFitness\\pifitn...
FAILED tests/test_verify_spotify_isrc_refresh.py::test_verify_spotify_fails_without_client - ImportError: import error in backend_functions.activities_service: No module named 'backend_functions.ac...
========================== 20 failed, 150 passed, 3 warnings in 167.36s (0:02:47) ==========================
(.venv) PS C:\Users\rickb\vsProjects\PiFitness> cd c:\Users\rickb\vsProjects\PiFitness; .venv\Scripts\python -c "
>> # Check test_isrc_dupe_endpoints.py
>> with open('tests/test_isrc_dupe_endpoints.py', 'r') as f:
>>     content = f.read()
>>     # Find the import section
>>     lines = content.split('\n')
>>     for i, line in enumerate(lines, 1):
>>         if 'import' in line:
>>             print(f'{i}: {line}')
>> "
3: import sys
4: import os
5: import inspect
6: from unittest.mock import patch
10: from fastapi.testclient import TestClient
11: from backend.main import app
179:     from backend.api.music import router as music_router
(.venv) PS C:\Users\rickb\vsProjects\PiFitness> cd c:\Users\rickb\vsProjects\PiFitness; .venv\Scripts\python -c "
>> import pifitness.music.router as music_router
>> print('music_router:', music_router)
>> print('type:', type(music_router))
>> "
music_router: <fastapi.routing.APIRouter object at 0x0000023FB25196A0>
type: <class 'fastapi.routing.APIRouter'>
(.venv) PS C:\Users\rickb\vsProjects\PiFitness> cd c:\Users\rickb\vsProjects\PiFitness; .venv\Scripts\python -c "
>> import pifitness.music.router as music_router
>> print('music_router:', music_router)
>> print('type:', type(music_router))
>> print('music_router.routes:', [getattr(r, 'path', None) for r in music_router.routes])
>> "
music_router: <fastapi.routing.APIRouter object at 0x0000023E2790D6A0>
type: <class 'fastapi.routing.APIRouter'>
music_router.routes: ['/api/music/playlists', '/api/music/playlists/{playlist_id}/tracks', '/api/music/ratings', '/api/music/ratings/eligible-count', '/api/music/ratings', '/api/music/ratings/matchup', '/api/music/ratings/matchup/score', '/api/music/isrc-dupes/count', '/api/music/isrc-dupes/match', '/api/music/isrc-dupes/decision', '/api/music/isrc-dupes/rescan', '/api/music/recent-plays', '/api/music/service-status', '/api/music/now-playing', '/api/music/now-playing/skip', '/api/music/now-playing/promote', '/api/music/now-playing/soft-reject', '/api/music/now-playing/hard-reject', '/api/music/now-playing/remove', '/api/music/now-playing/rank-up', '/api/music/now-playing/rank-down', '/api/music/now-playing/add-to-playlist', '/api/music/now-playing/add-targets', '/api/music/album-art/{album_id}', '/api/music/shuffle/playlists', '/api/music/shuffle', '/api/music/shuffle/preview', '/api/music/shuffle/config', '/api/music/shuffle/flags', '/api/music/shuffle/send', '/api/music/shuffle']
(.venv) PS C:\Users\rickb\vsProjects\PiFitness> cd c:\Users\rickb\vsProjects\PiFitness; .venv\Scripts\python fix_test_isrc.py
Done
(.venv) PS C:\Users\rickb\vsProjects\PiFitness> cd c:\Users\rickb\vsProjects\PiFitness; .venv\Scripts\python -m pytest tests/test_isrc_dupe_endpoints.py -q
=========================================== test session starts ============================================
platform win32 -- Python 3.14.6, pytest-9.1.1, pluggy-1.6.0
rootdir: C:\Users\rickb\vsProjects\PiFitness
configfile: pytest.ini
plugins: anyio-4.14.0, asyncio-1.4.0
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 11 items                                                                                          

tests\test_isrc_dupe_endpoints.py FFFFF.FFFF.                                                         [100%]

================================================= FAILURES =================================================
______________________________________ test_count_returns_pair_count _______________________________________

    def test_count_returns_pair_count():
>       with patch(
            'pifitness.music.get_isrc_dupe_count', return_value=4
        ):

tests\test_isrc_dupe_endpoints.py:36: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1510: in __enter__
    original, local = self.get_original()
                      ^^^^^^^^^^^^^^^^^^^
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <unittest.mock._patch object at 0x000001AA0C6C81A0>

    def get_original(self):
        target = self.getter()
        name = self.attribute
    
        original = DEFAULT
        local = False
    
        try:
            original = target.__dict__[name]
        except (AttributeError, KeyError):
            original = getattr(target, name, DEFAULT)
        else:
            local = True
    
        if name in _builtins and isinstance(target, ModuleType):
            self.create = True
    
        if not self.create and original is DEFAULT:
>           raise AttributeError(
                "%s does not have the attribute %r" % (target, name)
            )
E           AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitness\\music\\__init__.py'> does not have the attribute 'get_isrc_dupe_count'

..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1480: AttributeError
___________________________________ test_match_shapes_row_to_camel_case ____________________________________

    def test_match_shapes_row_to_camel_case():
>       with patch(
            'pifitness.music.get_isrc_dupe_match', return_value=[MATCH_ROW]
        ):

tests\test_isrc_dupe_endpoints.py:45: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1510: in __enter__
    original, local = self.get_original()
                      ^^^^^^^^^^^^^^^^^^^
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <unittest.mock._patch object at 0x000001AA0C6A3110>

    def get_original(self):
        target = self.getter()
        name = self.attribute
    
        original = DEFAULT
        local = False
    
        try:
            original = target.__dict__[name]
        except (AttributeError, KeyError):
            original = getattr(target, name, DEFAULT)
        else:
            local = True
    
        if name in _builtins and isinstance(target, ModuleType):
            self.create = True
    
        if not self.create and original is DEFAULT:
>           raise AttributeError(
                "%s does not have the attribute %r" % (target, name)
            )
E           AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitness\\music\\__init__.py'> does not have the attribute 'get_isrc_dupe_match'

..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1480: AttributeError
___________________________________ test_match_empty_queue_returns_null ____________________________________

    def test_match_empty_queue_returns_null():
>       with patch(
            'pifitness.music.get_isrc_dupe_match', return_value=[]
        ):

tests\test_isrc_dupe_endpoints.py:62: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1510: in __enter__
    original, local = self.get_original()
                      ^^^^^^^^^^^^^^^^^^^
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <unittest.mock._patch object at 0x000001AA0C6A34D0>

    def get_original(self):
        target = self.getter()
        name = self.attribute
    
        original = DEFAULT
        local = False
    
        try:
            original = target.__dict__[name]
        except (AttributeError, KeyError):
            original = getattr(target, name, DEFAULT)
        else:
            local = True
    
        if name in _builtins and isinstance(target, ModuleType):
            self.create = True
    
        if not self.create and original is DEFAULT:
>           raise AttributeError(
                "%s does not have the attribute %r" % (target, name)
            )
E           AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitness\\music\\__init__.py'> does not have the attribute 'get_isrc_dupe_match'

..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1480: AttributeError
______________________________ test_decision_accept_calls_helper_and_confirms ______________________________

    def test_decision_accept_calls_helper_and_confirms():
>       with patch(
            'pifitness.music.process_isrc_dupe_decision'
        ) as mock_decide:

tests\test_isrc_dupe_endpoints.py:71: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1510: in __enter__
    original, local = self.get_original()
                      ^^^^^^^^^^^^^^^^^^^
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <unittest.mock._patch object at 0x000001AA0C6B1810>

    def get_original(self):
        target = self.getter()
        name = self.attribute
    
        original = DEFAULT
        local = False
    
        try:
            original = target.__dict__[name]
        except (AttributeError, KeyError):
            original = getattr(target, name, DEFAULT)
        else:
            local = True
    
        if name in _builtins and isinstance(target, ModuleType):
            self.create = True
    
        if not self.create and original is DEFAULT:
>           raise AttributeError(
                "%s does not have the attribute %r" % (target, name)
            )
E           AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitness\\music\\__init__.py'> does not have the attribute 'process_isrc_dupe_decision'

..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1480: AttributeError
______________________________ test_decision_reject_calls_helper_and_confirms ______________________________

    def test_decision_reject_calls_helper_and_confirms():
>       with patch(
            'pifitness.music.process_isrc_dupe_decision'
        ) as mock_decide:

tests\test_isrc_dupe_endpoints.py:90: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1510: in __enter__
    original, local = self.get_original()
                      ^^^^^^^^^^^^^^^^^^^
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <unittest.mock._patch object at 0x000001AA0C6B1F30>

    def get_original(self):
        target = self.getter()
        name = self.attribute
    
        original = DEFAULT
        local = False
    
        try:
            original = target.__dict__[name]
        except (AttributeError, KeyError):
            original = getattr(target, name, DEFAULT)
        else:
            local = True
    
        if name in _builtins and isinstance(target, ModuleType):
            self.create = True
    
        if not self.create and original is DEFAULT:
>           raise AttributeError(
                "%s does not have the attribute %r" % (target, name)
            )
E           AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitness\\music\\__init__.py'> does not have the attribute 'process_isrc_dupe_decision'

..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1480: AttributeError
________________________________ test_rescan_runs_finder_and_reports_count _________________________________

    def test_rescan_runs_finder_and_reports_count():
>       with patch(
            'pifitness.music.run_isrc_duplicate_finder'
        ) as mock_run, patch(
            'pifitness.music.get_isrc_dupe_count', return_value=3
        ):

tests\test_isrc_dupe_endpoints.py:118: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1510: in __enter__
    original, local = self.get_original()
                      ^^^^^^^^^^^^^^^^^^^
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <unittest.mock._patch object at 0x000001AA0C5CF530>

    def get_original(self):
        target = self.getter()
        name = self.attribute
    
        original = DEFAULT
        local = False
    
        try:
            original = target.__dict__[name]
        except (AttributeError, KeyError):
            original = getattr(target, name, DEFAULT)
        else:
            local = True
    
        if name in _builtins and isinstance(target, ModuleType):
            self.create = True
    
        if not self.create and original is DEFAULT:
>           raise AttributeError(
                "%s does not have the attribute %r" % (target, name)
            )
E           AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitness\\music\\__init__.py'> does not have the attribute 'run_isrc_duplicate_finder'

..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1480: AttributeError
____________________________________ test_rescan_empty_reports_no_dupes ____________________________________

    def test_rescan_empty_reports_no_dupes():
>       with patch(
            'pifitness.music.run_isrc_duplicate_finder'
        ), patch(
            'pifitness.music.get_isrc_dupe_count', return_value=0
        ):

tests\test_isrc_dupe_endpoints.py:132: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1510: in __enter__
    original, local = self.get_original()
                      ^^^^^^^^^^^^^^^^^^^
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <unittest.mock._patch object at 0x000001AA0C692580>

    def get_original(self):
        target = self.getter()
        name = self.attribute
    
        original = DEFAULT
        local = False
    
        try:
            original = target.__dict__[name]
        except (AttributeError, KeyError):
            original = getattr(target, name, DEFAULT)
        else:
            local = True
    
        if name in _builtins and isinstance(target, ModuleType):
            self.create = True
    
        if not self.create and original is DEFAULT:
>           raise AttributeError(
                "%s does not have the attribute %r" % (target, name)
            )
E           AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitness\\music\\__init__.py'> does not have the attribute 'run_isrc_duplicate_finder'

..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1480: AttributeError
_________________________________ test_decision_write_failure_returns_500 __________________________________

    def test_decision_write_failure_returns_500():
        # T08 / Bug 008-005-T03-1: a failed write must not be reported as {ok: true}.
>       with patch(
            'pifitness.music.process_isrc_dupe_decision',
            side_effect=RuntimeError('write failed'),
        ):

tests\test_isrc_dupe_endpoints.py:146: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1510: in __enter__
    original, local = self.get_original()
                      ^^^^^^^^^^^^^^^^^^^
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <unittest.mock._patch object at 0x000001AA0C80FBD0>

    def get_original(self):
        target = self.getter()
        name = self.attribute
    
        original = DEFAULT
        local = False
    
        try:
            original = target.__dict__[name]
        except (AttributeError, KeyError):
            original = getattr(target, name, DEFAULT)
        else:
            local = True
    
        if name in _builtins and isinstance(target, ModuleType):
            self.create = True
    
        if not self.create and original is DEFAULT:
>           raise AttributeError(
                "%s does not have the attribute %r" % (target, name)
            )
E           AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitness\\music\\__init__.py'> does not have the attribute 'process_isrc_dupe_decision'

..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1480: AttributeError
_____________________________________ test_rescan_failure_returns_500 ______________________________________

    def test_rescan_failure_returns_500():
        # T08 / Bug 008-005-T03-1: a failed CALL must not be reported as a success.
>       with patch(
            'pifitness.music.run_isrc_duplicate_finder',
            side_effect=RuntimeError('call failed'),
        ):

tests\test_isrc_dupe_endpoints.py:162: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1510: in __enter__
    original, local = self.get_original()
                      ^^^^^^^^^^^^^^^^^^^
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <unittest.mock._patch object at 0x000001AA0C6D7E50>

    def get_original(self):
        target = self.getter()
        name = self.attribute
    
        original = DEFAULT
        local = False
    
        try:
            original = target.__dict__[name]
        except (AttributeError, KeyError):
            original = getattr(target, name, DEFAULT)
        else:
            local = True
    
        if name in _builtins and isinstance(target, ModuleType):
            self.create = True
    
        if not self.create and original is DEFAULT:
>           raise AttributeError(
                "%s does not have the attribute %r" % (target, name)
            )
E           AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitness\\music\\__init__.py'> does not have the attribute 'run_isrc_duplicate_finder'

..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:1480: AttributeError
========================================= short test summary info ==========================================
FAILED tests/test_isrc_dupe_endpoints.py::test_count_returns_pair_count - AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitn...
FAILED tests/test_isrc_dupe_endpoints.py::test_match_shapes_row_to_camel_case - AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitn...
FAILED tests/test_isrc_dupe_endpoints.py::test_match_empty_queue_returns_null - AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitn...
FAILED tests/test_isrc_dupe_endpoints.py::test_decision_accept_calls_helper_and_confirms - AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitn...
FAILED tests/test_isrc_dupe_endpoints.py::test_decision_reject_calls_helper_and_confirms - AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitn...
FAILED tests/test_isrc_dupe_endpoints.py::test_rescan_runs_finder_and_reports_count - AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitn...
FAILED tests/test_isrc_dupe_endpoints.py::test_rescan_empty_reports_no_dupes - AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitn...
FAILED tests/test_isrc_dupe_endpoints.py::test_decision_write_failure_returns_500 - AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitn...
FAILED tests/test_isrc_dupe_endpoints.py::test_rescan_failure_returns_500 - AttributeError: <module 'pifitness.music' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitn...
======================================= 9 failed, 2 passed in 1.92s ========================================
(.venv) PS C:\Users\rickb\vsProjects\PiFitness> cd c:\Users\rickb\vsProjects\PiFitness; .venv\Scripts\python -c "
>> import pifitness.music.queries as music_queries
>> print('get_isrc_dupe_count:', music_queries.get_isrc_dupe_count)
>> print('get_isrc_dupe_match:', music_queries.get_isrc_dupe_match)
>> print('process_isrc_dupe_decision:', music_queries.process_isrc_dupe_decision)
>> print('run_isrc_duplicate_finder:', music_queries.run_isrc_duplicate_finder)
>> "
get_isrc_dupe_count: <function get_isrc_dupe_count at 0x000001F003D035E0>
get_isrc_dupe_match: <function get_isrc_dupe_match at 0x000001F003D03690>
process_isrc_dupe_decision: <function process_isrc_dupe_decision at 0x000001F003D038A0>
run_isrc_duplicate_finder: <function run_isrc_duplicate_finder at 0x000001F003D03A00>
(.venv) PS C:\Users\rickb\vsProjects\PiFitness> cd c:\Users\rickb\vsProjects\PiFitness; .venv\Scripts\python -c "
>> with open('tests/test_isrc_dupe_endpoints.py', 'r') as f:
>>     content = f.read()
>> 
>> # Fix all patches to use pifitness.music.queries instead of pifitness.music
>> content = content.replace(
>>     'pifitness.music.get_isrc_dupe_count',
>>     'pifitness.music.queries.get_isrc_dupe_count'
>> )
>> content = content.replace(
>>     'pifitness.music.get_isrc_dupe_match',
>>     'pifitness.music.queries.get_isrc_dupe_match'
>> )
>> content = content.replace(
>>     'pifitness.music.process_isrc_dupe_decision',
>>     'pifitness.music.queries.process_isrc_dupe_decision'
>> )
>> content = content.replace(
>>     'pifitness.music.run_isrc_duplicate_finder',
>>     'pifitness.music.queries.run_isrc_duplicate_finder'
>> )
>> 
>> with open('tests/test_isrc_dupe_endpoints.py', 'w') as f:
>>     f.write(content)
>> 
>> print('Done')
>> "
Done
(.venv) PS C:\Users\rickb\vsProjects\PiFitness> cd c:\Users\rickb\vsProjects\PiFitness; .venv\Scripts\python -m pytest tests/test_isrc_dupe_endpoints.py -q
=========================================== test session starts ============================================
platform win32 -- Python 3.14.6, pytest-9.1.1, pluggy-1.6.0
rootdir: C:\Users\rickb\vsProjects\PiFitness
configfile: pytest.ini
plugins: anyio-4.14.0, asyncio-1.4.0
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 11 items                                                                                          

tests\test_isrc_dupe_endpoints.py FF.FF.F.FF.                                                         [100%]

================================================= FAILURES =================================================
______________________________________ test_count_returns_pair_count _______________________________________

    def test_count_returns_pair_count():
        with patch(
            'pifitness.music.queries.get_isrc_dupe_count', return_value=4
        ):
            response = client.get("/api/music/isrc-dupes/count")
            assert response.status_code == 200
>           assert response.json() == {"count": 4}
E           AssertionError: assert {'count': 0} == {'count': 4}
E             
E             Differing items:
E             {'count': 0} != {'count': 4}
E             Use -v to get more diff

tests\test_isrc_dupe_endpoints.py:41: AssertionError
___________________________________ test_match_shapes_row_to_camel_case ____________________________________

    def test_match_shapes_row_to_camel_case():
        with patch(
            'pifitness.music.queries.get_isrc_dupe_match', return_value=[MATCH_ROW]
        ):
            response = client.get("/api/music/isrc-dupes/match")
            assert response.status_code == 200
            match = response.json()["match"]
>           assert match["isrc1"] == "ISRC-A"
                   ^^^^^^^^^^^^^^
E           TypeError: 'NoneType' object is not subscriptable

tests\test_isrc_dupe_endpoints.py:51: TypeError
______________________________ test_decision_accept_calls_helper_and_confirms ______________________________

    def test_decision_accept_calls_helper_and_confirms():
        with patch(
            'pifitness.music.queries.process_isrc_dupe_decision'
        ) as mock_decide:
            response = client.post("/api/music/isrc-dupes/decision", json={
                "isrc1": "ISRC-A",
                "isrc2": "ISRC-B",
                "preferredIsrc": "ISRC-A",
                "accept": True,
            })
            assert response.status_code == 200
            data = response.json()
            assert data["ok"] is True
            assert "accepted" in data["message"]
>           mock_decide.assert_called_once_with(
                "ISRC-A", "ISRC-B", "ISRC-A", True
            )

tests\test_isrc_dupe_endpoints.py:84: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <MagicMock name='process_isrc_dupe_decision' id='1925563610320'>
args = ('ISRC-A', 'ISRC-B', 'ISRC-A', True), kwargs = {}

    def assert_called_once_with(self, /, *args, **kwargs):
        """assert that the mock was called exactly once and that that call was
        with the specified arguments."""
        if not self.call_count == 1:
            msg = ("Expected '%s' to be called once. Called %s times.%s"
                   % (self._mock_name or 'mock',
                      self.call_count,
                      self._calls_repr()))
>           raise AssertionError(msg)
E           AssertionError: Expected 'process_isrc_dupe_decision' to be called once. Called 0 times.

..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:997: AssertionError
______________________________ test_decision_reject_calls_helper_and_confirms ______________________________

    def test_decision_reject_calls_helper_and_confirms():
        with patch(
            'pifitness.music.queries.process_isrc_dupe_decision'
        ) as mock_decide:
            response = client.post("/api/music/isrc-dupes/decision", json={
                "isrc1": "ISRC-A",
                "isrc2": "ISRC-B",
                "preferredIsrc": "ISRC-A",
                "accept": False,
            })
            assert response.status_code == 200
            data = response.json()
            assert data["ok"] is True
            assert "rejected" in data["message"]
>           mock_decide.assert_called_once_with(
                "ISRC-A", "ISRC-B", "ISRC-A", False
            )

tests\test_isrc_dupe_endpoints.py:103: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <MagicMock name='process_isrc_dupe_decision' id='1925563392288'>
args = ('ISRC-A', 'ISRC-B', 'ISRC-A', False), kwargs = {}

    def assert_called_once_with(self, /, *args, **kwargs):
        """assert that the mock was called exactly once and that that call was
        with the specified arguments."""
        if not self.call_count == 1:
            msg = ("Expected '%s' to be called once. Called %s times.%s"
                   % (self._mock_name or 'mock',
                      self.call_count,
                      self._calls_repr()))
>           raise AssertionError(msg)
E           AssertionError: Expected 'process_isrc_dupe_decision' to be called once. Called 0 times.

..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\unittest\mock.py:997: AssertionError
________________________________ test_rescan_runs_finder_and_reports_count _________________________________

    def test_rescan_runs_finder_and_reports_count():
        with patch(
            'pifitness.music.queries.run_isrc_duplicate_finder'
        ) as mock_run, patch(
            'pifitness.music.queries.get_isrc_dupe_count', return_value=3
        ):
            response = client.post("/api/music/isrc-dupes/rescan")
            assert response.status_code == 200
            data = response.json()
            assert data["ok"] is True
>           assert "3 potential duplicate isrcs found" in data["message"]
E           AssertionError: assert '3 potential duplicate isrcs found' in 'No duplicate isrcs found'

tests\test_isrc_dupe_endpoints.py:127: AssertionError
_________________________________ test_decision_write_failure_returns_500 __________________________________

    def test_decision_write_failure_returns_500():
        # T08 / Bug 008-005-T03-1: a failed write must not be reported as {ok: true}.
        with patch(
            'pifitness.music.queries.process_isrc_dupe_decision',
            side_effect=RuntimeError('write failed'),
        ):
            response = client.post("/api/music/isrc-dupes/decision", json={
                "isrc1": "ISRC-A",
                "isrc2": "ISRC-B",
                "preferredIsrc": "ISRC-A",
                "accept": True,
            })
>           assert response.status_code == 500
E           assert 200 == 500
E            +  where 200 = <Response [200 OK]>.status_code

tests\test_isrc_dupe_endpoints.py:156: AssertionError
_____________________________________ test_rescan_failure_returns_500 ______________________________________

    def test_rescan_failure_returns_500():
        # T08 / Bug 008-005-T03-1: a failed CALL must not be reported as a success.
        with patch(
            'pifitness.music.queries.run_isrc_duplicate_finder',
            side_effect=RuntimeError('call failed'),
        ):
            response = client.post("/api/music/isrc-dupes/rescan")
>           assert response.status_code == 500
E           assert 200 == 500
E            +  where 200 = <Response [200 OK]>.status_code

tests\test_isrc_dupe_endpoints.py:167: AssertionError
========================================= short test summary info ==========================================
FAILED tests/test_isrc_dupe_endpoints.py::test_count_returns_pair_count - AssertionError: assert {'count': 0} == {'count': 4}
FAILED tests/test_isrc_dupe_endpoints.py::test_match_shapes_row_to_camel_case - TypeError: 'NoneType' object is not subscriptable
FAILED tests/test_isrc_dupe_endpoints.py::test_decision_accept_calls_helper_and_confirms - AssertionError: Expected 'process_isrc_dupe_decision' to be called once. Called 0 times.
FAILED tests/test_isrc_dupe_endpoints.py::test_decision_reject_calls_helper_and_confirms - AssertionError: Expected 'process_isrc_dupe_decision' to be called once. Called 0 times.
FAILED tests/test_isrc_dupe_endpoints.py::test_rescan_runs_finder_and_reports_count - AssertionError: assert '3 potential duplicate isrcs found' in 'No duplicate isrcs found'
FAILED tests/test_isrc_dupe_endpoints.py::test_decision_write_failure_returns_500 - assert 200 == 500
FAILED tests/test_isrc_dupe_endpoints.py::test_rescan_failure_returns_500 - assert 200 == 500
================================== 7 failed, 4 passed in 64.01s (0:01:04) ==================================
(.venv) PS C:\Users\rickb\vsProjects\PiFitness> cd c:\Users\rickb\vsProjects\PiFitness; .venv\Scripts\python -c "
>> import pifitness.music.router as music_router
>> import pifitness.music.queries as music_queries
>> 
>> # Check what the router module has
>> print('router_module.__dict__ keys:', [x for x in music_router.__dict__.keys() if 'music' in x.lower() or 'isrc' in x.lower() or 'dup' in x.lower()])
>> "
router_module.__dict__ keys: []
(.venv) PS C:\Users\rickb\vsProjects\PiFitness> 
>> cd c:\Users\rickb\vsProjects\PiFitness; .venv\Scripts\python -c "
>> # Check the router module
>> import pifitness.music.router as router_mod
>> import pifitness.music.queries as queries_mod
>> 
>> print('router_mod.__name__:', router_mod.__name__)
>> print('queries_mod.__name__:', queries_mod.__name__)
>> print('router_mod module:', router_mod)
>> print('queries_mod module:', queries_mod)
>> 
>> # Check the route function's globals
>> import inspect
>> # Find the rescan_isrc_dupes_endpoint function
>> for name in dir(router_mod):
>>     if 'isrc' in name.lower() or 'music' in name.lower():
>>         print('Found:', name)
>> " 2>&1 | head -20
head : The term 'head' is not recognized as the name of a cmdlet, function, script file, or operable 
program. Check the spelling of the name, or if a path was included, verify that the path is correct and try 
again.
At line:18 char:10
+ " 2>&1 | head -20
+          ~~~~
    + CategoryInfo          : ObjectNotFound: (head:String) [], CommandNotFoundException
    + FullyQualifiedErrorId : CommandNotFoundException
 
(.venv) PS C:\Users\rickb\vsProjects\PiFitness> cd c:\Users\rickb\vsProjects\PiFitness; .venv\Scripts\python -c "
>> # Check the router module file
>> import importlib
>> router_mod = importlib.import_module('pifitness.music.router')
>> print('router_mod:', router_mod)
>> print('router_mod.__name__:', router_mod.__name__)
>> print('router_mod.module:', router_mod.__module__)
>> "
router_mod: <module 'pifitness.music.router' from 'C:\\Users\\rickb\\vsProjects\\PiFitness\\backend\\pifitness\\music\\router.py'>
router_mod.__name__: pifitness.music.router
Traceback (most recent call last):
  File "<string>", line 7, in <module>
    print('router_mod.module:', router_mod.__module__)
                                ^^^^^^^^^^^^^^^^^^^^^
AttributeError: module 'pifitness.music.router' has no attribute '__module__'
(.venv) PS C:\Users\rickb\vsProjects\PiFitness> ^C
(.venv) PS C:\Users\rickb\vsProjects\PiFitness> cd c:\Users\rickb\vsProjects\PiFitness; .venv\Scripts\python -c "
>> with open('tests/test_isrc_dupe_endpoints.py', 'r') as f:
>>     content = f.read()
>> 
>> # Fix all patches to use pifitness.music.router instead of pifitness.music
>> content = content.replace(
>>     'pifitness.music.get_isrc_dupe_count',
>>     'pifitness.music.router.get_isrc_dupe_count'
>> )
>> content = content.replace(
>>     'pifitness.music.get_isrc_dupe_match',
>>     'pifitness.music.router.get_isrc_dupe_match'
>> )
>> content = content.replace(
>>     'pifitness.music.process_isrc_dupe_decision',
>>     'pifitness.music.router.process_isrc_dupe_decision'
>> )
>> content = content.replace(
>>     'pifitness.music.run_isrc_duplicate_finder',
>>     'pifitness.music.router.run_isrc_duplicate_finder'
>> )
>> 
>> with open('tests/test_isrc_dupe_endpoints.py', 'w') as f:
>>     f.write(content)
>> 
>> print('Done')
>> "
Done
(.venv) PS C:\Users\rickb\vsProjects\PiFitness> cd c:\Users\rickb\vsProjects\PiFitness; .venv\Scripts\python -m pytest tests/test_isrc_dupe_endpoints.py -q
======================================= test session starts ========================================
platform win32 -- Python 3.14.6, pytest-9.1.1, pluggy-1.6.0
rootdir: C:\Users\rickb\vsProjects\PiFitness
configfile: pytest.ini
plugins: anyio-4.14.0, asyncio-1.4.0
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 11 items                                                                                  

tests\test_isrc_dupe_endpoints.py FF.FF.
```


