with open('backend/pifitness/config/__init__.py', 'w') as f:
    f.write('"""Domain package for the config slice."""\n')
    f.write('\n')
    f.write('from pifitness.config.router import router\n')
    f.write('from pifitness.config.queries import mapbox_token\n')
    f.write('\n')
    f.write('__all__ = ["router", "mapbox_token"]\n')
