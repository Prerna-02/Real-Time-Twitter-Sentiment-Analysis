"""
Django settings for the Twitter Sentiment dashboard.
"""

from pathlib import Path

# Django-Dashboard/  (the directory containing manage.py)
BASE_DIR = Path(__file__).resolve().parent.parent

# Project root (Real-Time-Twitter-Sentiment-Analysis/)
PROJECT_ROOT = BASE_DIR.parent


# ── Security / debug ──────────────────────────────────────────────────────────
SECRET_KEY = 'django-insecure-dev-only-change-for-production'
DEBUG = True
ALLOWED_HOSTS = ['127.0.0.1', 'localhost']


# ── Apps ──────────────────────────────────────────────────────────────────────
INSTALLED_APPS = [
    'django.contrib.contenttypes',
    'django.contrib.auth',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'dashboard',
]


# ── Middleware ────────────────────────────────────────────────────────────────
MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]


# ── URL routing ───────────────────────────────────────────────────────────────
ROOT_URLCONF = 'sentimentapp.urls'


# ── Templates ─────────────────────────────────────────────────────────────────
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,  # picks up dashboard/templates/ automatically
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]


# ── WSGI ──────────────────────────────────────────────────────────────────────
WSGI_APPLICATION = 'sentimentapp.wsgi.application'


# ── Database ──────────────────────────────────────────────────────────────────
# We use MongoDB for tweet storage (see dashboard/models.py).
# Django still requires a default DB for sessions / CSRFkeep SQLite for that.
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}


# ── Static files (CSS / JS) ───────────────────────────────────────────────────
STATIC_URL = '/static/'
# APP_DIRS in TEMPLATES + STATIC_URL together pick up dashboard/static/


# ── i18n / tz ─────────────────────────────────────────────────────────────────
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True


# ── Default primary key ───────────────────────────────────────────────────────
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'


# ──────────────────────────────────────────────────────────────────────────────
# Custom settings used by the dashboard app
# ──────────────────────────────────────────────────────────────────────────────

# MongoDBmust match config.py used by consumer.py
MONGO_URI        = 'mongodb://localhost:27017'
MONGO_DB         = 'twitter_sentiment'
MONGO_COLLECTION = 'tweets'

# Spark model pathabsolute, forward-slashed (Windows-safe for PySpark)
SPARK_MODEL_PATH = str(
    (PROJECT_ROOT / 'ML-PySpark-Model' / 'saved_models' / 'spark_lr_pipeline').resolve()
).replace('\\', '/')
