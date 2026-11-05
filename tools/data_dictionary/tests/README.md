# Tests

Run from the repository root with `& $py -m unittest discover -s tools/data_dictionary/tests -v` (where `$py` is your configured project interpreter). The test files add their parent tool directory to `sys.path`, so standard unittest discovery works from the root. Tests use temporary synthetic files only and never connect to PostgreSQL.
