# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from django.utils.encoding import python_2_unicode_compatible
from django.db import models
from django.contrib.postgres.fields import JSONField


@python_2_unicode_compatible
class HiveTable(models.Model):
    database = models.TextField(null=False, blank=False)
    table = models.TextField(null=False, blank=False)
    table_comment = models.TextField(blank=False, null=False)
    column = models.TextField()
    column_comment = models.TextField()
    column_index = models.IntegerField()
    time_format = models.TextField(blank=True, null=True)
    column_type = models.TextField()

    def __str__(self):
        return self.table + " | " + self.table_comment + " | " + self.column

    class Meta:
        ordering = ("-id", )
        db_table = 'hive_table'


class ImportTask(models.Model):

    reader = JSONField(null=False, blank=False)
    writer = JSONField(null=False, blank=False)
    options = JSONField(null=False, blank=False)

    STATUS_UPLOAD = 'upload'
    STATUS_SUBMIT = 'submit'
    STATUS_EXECUTING = 'executing'
    STATUS_SUCCESS = 'success'
    STATUS_FAIL = 'fail'
    
    STATUS_CHOICES = (
        (STATUS_UPLOAD, 'upload'),
        (STATUS_SUBMIT, 'submit'),
        (STATUS_EXECUTING, 'executing'),
        (STATUS_SUCCESS, 'success'),
        (STATUS_FAIL, 'fail'),
    )
    status = models.TextField(choices=STATUS_CHOICES, help_text="状态", default=STATUS_UPLOAD, null=True, blank=True)

    start_time = models.DateTimeField(null=True, blank=True)
    finish_time = models.DateTimeField(null=True, blank=True)

    @property
    def config(self):
        return {
            'reader': self.reader,
            'writer': self.writer,
            'options': self.options
        }

    def __str__(self):
        if self.reader['type'] == "sql":
            return "%s-%s-%s" % (self.id, self.reader['table'], self.writer['table'])
        else:
            return "%s-%s-%s" % (self.id, self.reader['textpath'], self.writer['table'])

    class Meta:
        ordering = ("-id", )
        db_table = 'import_task'


class CommandRecord(models.Model):
    
    STATUS_EXECUTING = 'executing'
    STATUS_SUCCESS = 'success'
    STATUS_FAIL = 'fail'

    step = models.TextField()  # sql2hdfs, hdfs2hive
    command = models.TextField(null=True, blank=True)  # shell
    status = models.TextField()  # executing, success, fail
    start_time = models.DateTimeField(null=True, blank=True)
    finish_time = models.DateTimeField(null=True, blank=True)
    log = models.TextField(null=True, blank=True)
    task = models.ForeignKey(ImportTask, related_name='records')

    class Meta:
        db_table = 'import_record'

    def __str__(self):
        return "%s-%s" % (self.task.id, self.step)
