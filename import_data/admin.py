# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

# Register your models here.

from .models import HiveTable, CommandRecord, ImportTask


admin.site.register(HiveTable)
admin.site.register(ImportTask)
admin.site.register(CommandRecord)