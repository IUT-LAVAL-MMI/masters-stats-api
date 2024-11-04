# Masters-stats-api 

Project for SAE301-303 at MMI Laval.

REST API developed with Python Flask. Designed for a docker deployement.

## Deployment

### Architecture

Given a project folder $PF, $PF contains:

- masters-stats-api/ _a clone of the current project_
- local-config.py/ _a copy of the config.py file from the project, customized to the production env (see below)
- local-data/ _a folder containing the three .csv files (insertion professionnelle, mon master, mapping Candidatures - Insertion pro.)

### local-config.py for production

Exemple of properties of interest:

```
SERVER_HOST = '0.0.0.0'
SERVER_PORT = 5000
DEBUG = False

CANDIDATURE_SOURCE = '/var/api-data/fr-esr-mon_master.csv'
INSERTION_SOURCE = '/var/api-data/fr-esr-insertion_professionnelle-master.csv'
DISC_MAPPING_SOURCE = '/var/api-data/mappingCandIns.csv'
```

### Docker compose exemple extract

```
services:
  masters-stats-api:
    image: rvenant/masters-stats-api
    build:
      context: masters-stats-api/
    restart: always
    configs:
      - source: msapi-config
        target: /usr/src/app/config.py
        mode: 0440
    volumes:
      - ./local-data:/var/api-data:ro
    ports:
      - 0.0.0.0:80:5000

configs:
  msapi-config:
    file: local-config.py
```