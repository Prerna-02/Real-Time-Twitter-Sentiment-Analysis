"""
WSGI config for sentimentapp project.
Used when deploying behind a real web server (gunicorn, uWSGI, etc.).
"""

import os
from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'sentimentapp.settings')

application = get_wsgi_application()
