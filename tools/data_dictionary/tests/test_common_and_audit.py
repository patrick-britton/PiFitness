import os
import tempfile
import unittest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from unittest.mock import patch
from common import database_config, safe_category, sensitive_name, validate_connection_identity
from audit import migration_comments

class DatabaseGuardTests(unittest.TestCase):
    def test_exact_dev_target_is_accepted(self):
        env = "PG_HOST=localhost\nPG_PORT=15432\nPG_DB=pifitness_dev\nPG_USER=dev\nPGSSLMODE=disable\n"
        with tempfile.TemporaryDirectory() as d, patch.dict(os.environ, {}, clear=True):
            p = Path(d) / '.env'; p.write_text(env, encoding='utf-8')
            cfg = database_config(p)
            self.assertEqual((cfg['host'], cfg['port'], cfg['dbname'], cfg['user']), ('localhost',15432,'pifitness_dev','dev'))

    def test_nonlocal_target_is_rejected(self):
        env = "PG_HOST=db.example.com\nPG_PORT=15432\nPG_DB=pifitness_dev\nPG_USER=dev\n"
        with tempfile.TemporaryDirectory() as d, patch.dict(os.environ, {}, clear=True):
            p = Path(d) / '.env'; p.write_text(env, encoding='utf-8')
            with self.assertRaises(RuntimeError): database_config(p)

    def test_production_database_is_rejected_even_on_localhost(self):
        env = "PG_HOST=localhost\nPG_PORT=15432\nPG_DB=pfpi\nPG_USER=dev\n"
        with tempfile.TemporaryDirectory() as d, patch.dict(os.environ, {}, clear=True):
            p = Path(d) / '.env'; p.write_text(env, encoding='utf-8')
            with self.assertRaises(RuntimeError): database_config(p)

class ConnectionIdentityTests(unittest.TestCase):
    def test_docker_internal_listener_port_is_not_compared_to_host_port(self):
        cfg = {'host': 'localhost', 'port': 15432}
        identity = {
            'database': 'pifitness_dev', 'role': 'dev',
            'server_address': '172.18.0.2', 'server_port': 5432,
            'transaction_read_only': 'on',
        }
        validate_connection_identity(identity, cfg)

    def test_wrong_database_identity_is_rejected_with_diagnostics(self):
        cfg = {'host': 'localhost', 'port': 15432}
        identity = {
            'database': 'pfpi', 'role': 'dev',
            'server_address': '172.18.0.2', 'server_port': 5432,
            'transaction_read_only': 'on',
        }
        with self.assertRaisesRegex(RuntimeError, 'pfpi'):
            validate_connection_identity(identity, cfg)

class PrivacyClassifierTests(unittest.TestCase):
    def test_sensitive_fields_are_never_safe_categories(self):
        self.assertFalse(safe_category('heartrate_status', 'text'))
        self.assertFalse(safe_category('access_token_type', 'text'))
        self.assertTrue(safe_category('activity_type_id', 'smallint'))
        self.assertTrue(safe_category('is_active', 'boolean'))
        self.assertTrue(sensitive_name('gps_latitude'))

class SqlCommentTests(unittest.TestCase):
    def test_comment_statements_and_null_comment_removals_are_counted(self):
        sql = "COMMENT ON TABLE staging.a IS 'Staging table.';\nCOMMENT ON TABLE staging.b IS NULL;\nCOMMENT ON COLUMN staging.a.id IS 'Identifier.';\n"
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'migration.sql'; p.write_text(sql, encoding='utf-8')
            got = migration_comments(p)
            self.assertEqual(got['table_comment_statement_count'], 2)
            self.assertEqual(got['table_comment_null_count'], 1)
            self.assertEqual(got['column_comment_statement_count'], 1)
            self.assertEqual(got['generic_count'], 2)

if __name__ == '__main__': unittest.main()
