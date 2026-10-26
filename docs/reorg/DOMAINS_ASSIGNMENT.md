# Phase 3: Python package by domain
# Slice order: health -> exercises -> volleyball -> admin -> food -> music -> integrations -> activities -> worker -> core -> api wiring

[health]
# Backend/api/health.py -> backend/pifitness/health/router.py
# backend_functions/database_functions.py (shared, keep in backend_functions)

[exercises]
# Backend/api/exercises.py -> backend/pifitness/exercises/router.py

[volleyball]
# Backend/api/volleyball.py -> backend/pifitness/volleyball/router.py

[admin]
# Backend/api/admin.py -> backend/pifitness/admin/router.py

[food]
# Backend/api/food.py, food_lookup.py -> backend/pifitness/food/{router,lookup}.py

[music]
# Backend/api/music.py -> backend/pifitness/music/router.py

[integrations]
# Backend/api/auth.py, config.py -> backend/pifitness/integrations/{auth,config}.py

[activities]
# Backend/api/activities.py -> backend/pifitness/activities/router.py

[worker]
# Backend_functions/backend_tasks.py -> backend/pifitness/worker/worker.py

[core]
# backend_functions/{config,domain_exceptions,logging_functions,...} -> backend/pifitness/core/{config,exceptions,logging}.py

[api]
# backend/main.py (FastAPI app) -> backend/api/main.py
