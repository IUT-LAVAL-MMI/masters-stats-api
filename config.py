# BASE SERVER CONFIGURATION
# General
DEBUG = True
# CORS Configuration
ENABLE_CORS = True  # Enable CORS compliancy only if the front app is served by another server (mostly in dev. conf)

# MONGODB CONNECTION CONFIGURATION
MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DATABASE = 'masters'
MONGO_USERNAME = 'root'
MONGO_PASSWORD = 'rootpassword'
MONGO_AUTH_SOURCE = 'admin'

# DATA SOURCE
CANDIDATURE_SOURCE = 'local/fr-esr-mon_master.csv'
INSERTION_SOURCE = 'local/fr-esr-insertion_professionnelle-master.csv'
DISC_MAPPING_SOURCE = 'local/mappingCandIns.csv'

# DEV CONFIG
# use 0.0.0.0:5000 for a docker deployment
SERVER_HOST_DEV = '127.0.0.1'
SERVER_PORT_DEV = 5001