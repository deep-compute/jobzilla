from django.conf.urls import url, include

from . import views
from kwikapi.django import RequestHandler

from basescript import init_logger
log = init_logger(fmt='pretty', fpath=None, pre_hooks=[], post_hooks=[], metric_grouping_interval=1, level='warning')


urlpatterns = [
    url(r'api/', RequestHandler(views.api, log=log).handle_request),
]
