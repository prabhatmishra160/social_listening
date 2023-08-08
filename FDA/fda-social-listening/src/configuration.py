import os
from brightfield_common.settings import *

BASE_DIR = settings.get_base_dir(__file__, True)
ENV_FILE = os.path.join(BASE_DIR, '.env')
settings['WHOOSH_BASE_DIR'] = BASE_DIR
settings['FIXTURE_DIR'] = os.path.join(BASE_DIR, 'tests/fixtures')
settings.load_variables_from_file(ENV_FILE)
settings['PROJECT'] = "brightfield-dev"
