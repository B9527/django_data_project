# -*- coding: utf-8 -*-
'''
    models
'''
from __future__ import unicode_literals

from django.db import models
from django.contrib.postgres.fields import JSONField


class CleanConfig(models.Model):
    """
    reader:{
                "database": "raw",
                "table": "test",
                "fields": [
                    {
                        "name": "field_1",
                        "type": "int",
                        "order": 1
                    },
                    {
                        "name": "field_2",
                        "ype": "date",
                        "order": 2
                    }
                ]
            }

    writer: {
            "database": "raw",
            "table": "test",
            "fields": [
                {
                    "name": "field_1",
                    "type": "int",
                    "order": 1
                },
                {
                    "name": "field_2",
                    "type": "date",
                    "order": 2
             }
    ]
}

    transform:[
                    {
                        "path": "common.IntTransformer",
                        "params": {
                            "fields": ["int_field_1", "int_field_2"]
							},

                        "order": 1
                    },

                    {
                        "path": "common.DateTransformer",
                        "params": {
                            "fields": [
                                {
                                    "name": "field_1",
                                    "format": "%Y-%m-%d"
                                }
                            ]
                        },
                        "order": 2
                    }
    ]
    """
    STATUS_UPLOAD = 'upload'
    STATUS_SUBMIT = 'submit'
    STATUS_EXECUTING = 'executing'
    STATUS_SUCCESS = 'success'
    STATUS_FAIL = 'fail'

    reader = JSONField()
    success_writer = JSONField()
    error_writer = JSONField()
    transform = JSONField()
    settings = JSONField(null=True, blank=True)

    status = models.TextField()
    start_time = models.DateTimeField(null=True, blank=True)
    finish_time = models.DateTimeField(null=True, blank=True)
    log = models.TextField(null=True, blank=True)

    class Meta:
        db_table = 'clean_task'
        unique_together = ['reader']

    @property
    def config(self):
        return {'reader': self.reader, 
                'ok_writer': self.success_writer, 
                'error_writer': self.error_writer,
                'transform': self.transform
                }

    def __str__(self):
        return "%s-%s-%s" % (self.id, self.reader['database'], self.reader['table'])


class CleanMappingField(models.Model):

    src_name = models.TextField()
    dst_name = models.TextField()
    index = models.IntegerField()

    config = models.ForeignKey(CleanConfig, related_name='mapping_fields')

    class Meta:
        db_table = 'clean_mapping_field'
