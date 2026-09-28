from backend_functions.backend_tasks import backup_database

print('starting backup')
backup_database()
print('backup complete')