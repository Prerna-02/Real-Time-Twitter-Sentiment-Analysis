"""
Project-level URL configuration.
Delegates everything to the dashboard app's urls.py.
"""

from django.urls import path, include

urlpatterns = [
    path('', include('dashboard.urls')),
]
