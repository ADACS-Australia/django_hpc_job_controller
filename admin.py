from django.contrib import admin

from .models import HpcCluster, HpcJob, WebsocketToken

admin.site.register(HpcCluster)
admin.site.register(HpcJob)
admin.site.register(WebsocketToken)
