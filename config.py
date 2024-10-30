# BASE SERVER CONFIGURATION
# General
# use 0.0.0.0:5000 for a docker deployment
SERVER_HOST = '127.0.0.1'
SERVER_PORT = 5001
DEBUG = True
# CORS Configuration
ENABLE_CORS = True  # Enable CORS compliancy only if the front app is served by another server (mostly in dev. conf)

# DATA SOURCE
CANDIDATURE_SOURCE = 'local/fr-esr-mon_master.csv'
INSERTION_SOURCE = 'local/fr-esr-insertion_professionnelle-master.csv'
