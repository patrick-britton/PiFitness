I'm building out an automatic bootstrapper piece-by-piece and trying to stay as true to the `autodeploy_objectives.md` as possible.

I only ever test by running the pi shell script and relying on the reported output to fix. 

This is a personal-use, and single-developer application, so don't over engineer.

# Your Task
- We're working on getting the test-suite clean in dev so we can progress with the automated bootstrap scrip. Review the latest pytest output (at the bottom of this prompt) -- we need to identify the causes of these test errors and address them. Do not assume the tests are valid -- read through `adp/test_cleanup.md` and delete any tests you think are invalid. If you believe the bug/test failure points to a true application flaw, raise it to the user to discuss the implications and options to fix it.

# Ground rules:
1. autodeploy_objectives.md is the specification. Stay as true to it as possible. Change course only when something critical is discovered, and say so explicitly when you do.
2. Do not assume anything. If you need a file or a fact, ask for it. Say clearly what is verified and what is untested.
3. TEST and PRD must run the same code in the same kind of environment. The only intended differences are the branch pulled (handled by the launcher), the env file used, and TEST restoring previously active services. step_dependencies.py must contain no TEST/PRD branches. Anything that differs must come from values in the Pi env file (VENV_DIR, APP_DIR, ...). If you find yourself writing if environment == ..., stop and raise it with the user.
4. Minimal configuration. The user dislikes proliferating config keys and "self-referential paths". Values that never vary (for example where the app reads its env file) are constants in the helper. Things the script can work out at run time should be worked out at run time. Add a config key only when a value truly varies.
5. Guard against over-engineering. Prefer the canonical tools (uv, npm ci, systemctl, ...) over reimplementing them. If something in the objectives or in your plan looks like more machinery than it is worth, say so.
Be honest about what could not be tested.
6. If you have built new files, always inform the user which directory they go in. Do not produce files that do not belong in the repo (except updated objectives file)
7. If the user must execute manual steps, you must lay them out in clear, step-by-step, idiot-proof instructions, including which machine, file, and values to change or execute.

# Latest Pytest output:
(.venv) PS C:\Users\rickb\vsProjects\PiFitness> & "C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe" -m pytest --junitxml=backend_report.xml
============================================================ test session starts ============================================================
platform win32 -- Python 3.14.6, pytest-9.1.1, pluggy-1.6.0
rootdir: C:\Users\rickb\vsProjects\PiFitness
configfile: pytest.ini
testpaths: tests
plugins: anyio-4.14.0, platformdirs-4.14.0, asyncio-1.4.0
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 333 items                                                                                                                          

tests\test_activities.py ......F.....                                                                                                  [  3%]
tests\test_admin.py FFFFF                                                                                                              [  5%]
tests\test_admin_failure_message.py ..                                                                                                 [  5%]
tests\test_api_endpoint_contract.py .                                                                                                  [  6%]
tests\test_auth_status.py .....                                                                                                        [  7%]
tests\test_auth_status_contract.py ....                                                                                                [  8%]
tests\test_auto_shuffle_pandas_free.py .                                                                                               [  9%]
tests\test_config.py ...                                                                                                               [  9%]
tests\test_create_segment_details_write.py ..                                                                                          [ 10%]
tests\test_empty_queue_noop.py ...                                                                                                     [ 11%]
tests\test_env_helpers.py ......................                                                                                       [ 18%]
tests\test_failure_propagation.py ..............                                                                                       [ 22%]
tests\test_food_api_contract.py ............................                                                                           [ 30%]
tests\test_food_search_backend.py ....................                                                                                 [ 36%]
tests\test_garmin_auth.py .......                                                                                                      [ 38%]
tests\test_garmin_login_flow.py ......                                                                                                 [ 40%]
tests\test_health.py .                                                                                                                 [ 40%]
tests\test_inspect_test_db.py ............F................                                                                            [ 49%]
tests\test_integration_protocol.py ..                                                                                                  [ 50%]
tests\test_isrc_dupe_endpoints.py ..........                                                                                           [ 53%]
tests\test_isrc_dupes.py ........                                                                                                      [ 55%]
tests\test_json_extractors_garmin_data.py ...                                                                                          [ 56%]
tests\test_music_actions.py .....................................                                                                      [ 67%]
tests\test_no_prd_default.py ...........                                                                                               [ 70%]
tests\test_playlist_details_href.py .                                                                                                  [ 71%]
tests\test_playlist_relationships.py ..                                                                                                [ 71%]
tests\test_process_generator_events.py .                                                                                               [ 72%]
tests\test_ratchet.py .......                                                                                                          [ 74%]
tests\test_restore_test_db.py ....Fs.F.FFFFFFFFFF..                                                                                    [ 80%]
tests\test_route_smoke.py FFFFFFFFFFFFFFFFFF                                                                                           [ 85%]
tests\test_schema_compare.py ....F.............                                                                                        [ 91%]
tests\test_spotify_oauth_flow.py ...                                                                                                   [ 92%]
tests\test_test_db_policy.py .................                                                                                         [ 97%]
tests\test_test_state.py .s..                                                                                                          [ 98%]
tests\test_token_db_persistence.py ....                                                                                                [ 99%]
tests\test_verify_spotify_isrc_refresh.py .                                                                                            [100%]Traceback (most recent call last):
  File "<frozen runpy>", line 203, in _run_module_as_main
  File "<frozen runpy>", line 88, in _run_code
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\pytest\__main__.py", line 9, in <module>
    raise SystemExit(_console_main())
                     ~~~~~~~~~~~~~^^
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\_pytest\config\__init__.py", line 253, in _console_main
    code = _main(prog=_get_prog_name(sys.argv))
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\_pytest\config\__init__.py", line 229, in _main
    ret: ExitCode | int = config.hook.pytest_cmdline_main(config=config)
                          ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\pluggy\_hooks.py", line 512, in __call__
    return self._hookexec(self.name, self._hookimpls.copy(), kwargs, firstresult)
           ~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\pluggy\_manager.py", line 120, in _hookexec
    return self._inner_hookexec(hook_name, methods, kwargs, firstresult)
           ~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\pluggy\_callers.py", line 167, in _multicall
    raise exception
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\pluggy\_callers.py", line 121, in _multicall
    res = hook_impl.function(*args)
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\_pytest\main.py", line 377, in pytest_cmdline_main
    return wrap_session(config, _main)
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\_pytest\main.py", line 365, in wrap_session
    config.hook.pytest_sessionfinish(
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^
        session=session, exitstatus=session.exitstatus
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\pluggy\_hooks.py", line 512, in __call__
    return self._hookexec(self.name, self._hookimpls.copy(), kwargs, firstresult)
           ~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\pluggy\_manager.py", line 120, in _hookexec
    return self._inner_hookexec(hook_name, methods, kwargs, firstresult)
           ~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\pluggy\_callers.py", line 167, in _multicall
    raise exception
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\pluggy\_callers.py", line 139, in _multicall
    teardown.throw(exception)
    ~~~~~~~~~~~~~~^^^^^^^^^^^
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\_pytest\logging.py", line 888, in pytest_sessionfinish
    return (yield)
            ^^^^^
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\pluggy\_callers.py", line 139, in _multicall
    teardown.throw(exception)
    ~~~~~~~~~~~~~~^^^^^^^^^^^
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\_pytest\terminal.py", line 961, in pytest_sessionfinish
    result = yield
             ^^^^^
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\pluggy\_callers.py", line 139, in _multicall
    teardown.throw(exception)
    ~~~~~~~~~~~~~~^^^^^^^^^^^
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\_pytest\warnings.py", line 119, in pytest_sessionfinish
    return (yield)
            ^^^^^
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\pluggy\_callers.py", line 121, in _multicall
    res = hook_impl.function(*args)
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\_pytest\tmpdir.py", line 340, in pytest_sessionfinish
    tmp_path_factory._exit_stack.close()
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^
  File "C:\Users\rickb\AppData\Local\Python\pythoncore-3.14-64\Lib\contextlib.py", line 627, in close
    self.__exit__(None, None, None)
    ~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^
  File "C:\Users\rickb\AppData\Local\Python\pythoncore-3.14-64\Lib\contextlib.py", line 619, in __exit__
    raise exc
  File "C:\Users\rickb\AppData\Local\Python\pythoncore-3.14-64\Lib\contextlib.py", line 604, in __exit__
    if cb(*exc_details):
       ~~^^^^^^^^^^^^^^
  File "C:\Users\rickb\AppData\Local\Python\pythoncore-3.14-64\Lib\contextlib.py", line 482, in _exit_wrapper
    callback(*args, **kwds)
    ~~~~~~~~^^^^^^^^^^^^^^^
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\_pytest\pathlib.py", line 371, in cleanup_numbered_dir
    cleanup_dead_symlinks(root)
    ~~~~~~~~~~~~~~~~~~~~~^^^^^^
  File "C:\users\rickb\vsprojects\pifitness\.venv\Lib\site-packages\_pytest\pathlib.py", line 357, in cleanup_dead_symlinks
    left_dir.unlink()
    ~~~~~~~~~~~~~~~^^
  File "C:\Users\rickb\AppData\Local\Python\pythoncore-3.14-64\Lib\pathlib\__init__.py", line 1042, in unlink
    os.unlink(self)
    ~~~~~~~~~^^^^^^
PermissionError: [WinError 5] Access is denied: 'C:\\Users\\rickb\\AppData\\Local\\Temp\\pytest-of-rickb\\pytest-current'