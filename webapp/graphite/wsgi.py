import os
import sys
import gevent.monkey as monkey
monkey.patch_all()

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "graphite.settings")

from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()
