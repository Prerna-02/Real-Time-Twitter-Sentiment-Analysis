"""
ASGI config for sentimentapp project.
Used for async deployments. Not required for runserver, but Django expects it.
"""

import os
from django.core.asgi import get_asgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'sentimentapp.settings')

application = get_asgi_application()
