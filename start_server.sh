#!/bin/sh
# -u is important to not buffer console I/O that could cause no showing output to console in some docker platform (macos especially)
gunicorn -c 'gunicorn.conf.py' 'MasterStatsAPI:main()'
