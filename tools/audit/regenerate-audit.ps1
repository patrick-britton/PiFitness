# Wrapper around repo_audit all --with-external
# Replaces the legacy multi-tool regenerator; delegates to stdlib-only repo_audit.
python scripts\repo_audit all --with-external
exit $LASTEXITCODE
