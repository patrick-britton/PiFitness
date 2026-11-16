import tempfile, unittest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from schema_inventory import parse_schema, compare

class InventoryTests(unittest.TestCase):
    def test_parses_objects_columns_and_foreign_keys(self):
        src="""### public.alpha (TABLE)\n| column | type | nullable |\n|---|---|---|\n| id | integer | NO |\n| code | text | YES |\n**Foreign keys:**\n- `id` -> `public.beta.id` (alpha_beta_fkey)\n### public.v_alpha (VIEW)\n| column | type | nullable |\n|---|---|---|\n| id | integer | NO |\n"""
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'schema.md'; p.write_text(src,encoding='utf-8')
            got=parse_schema(p)
            self.assertEqual(got['object_count'],2); self.assertEqual(got['column_count'],3)
            self.assertEqual(got['objects'][0]['foreign_keys'],['`id` -> `public.beta.id` (alpha_beta_fkey)'])

    def test_compare_detects_missing_and_duplicates(self):
        schema="""### public.a (TABLE)\n| column | type | nullable |\n|---|---|---|\n| id | integer | NO |\n### public.b (VIEW)\n| column | type | nullable |\n|---|---|---|\n| id | integer | NO |\n"""
        dictionary="""### public.a (table)\n### public.a (table)\n"""
        with tempfile.TemporaryDirectory() as d:
            s=Path(d)/'s.md'; x=Path(d)/'d.md'; s.write_text(schema); x.write_text(dictionary)
            got=compare(s,x)
            self.assertEqual(got['schema_only'],['public.b'])
            self.assertEqual(got['duplicate_dictionary_headings'],{'public.a':2})

if __name__=='__main__': unittest.main()
