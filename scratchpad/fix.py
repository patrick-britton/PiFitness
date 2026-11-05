import pathlib
p=pathlib.Path('docs/data/dictionary.md')
lines=p.read_text().splitlines(keepends=True)
lines[1279]='| `segment_id` | integer | YES | Excluded segment (FK to activities.segments). | integer | segment matching exclusion list | deploy/migrations/007__comment_backfill_activities.sql:138; backend/pifitness/activities/queries.py | confirmed, high |\n'
lines[1285]='| `activity_id` | bigint | YES | Excluded activity (FK to activities.activities). | integer | segment matching exclusion list | deploy/migrations/007__comment_backfill_activities.sql:139; backend/pifitness/activities/queries.py | confirmed, high |\n'
p.write_text(''.join(lines))
print('done')
