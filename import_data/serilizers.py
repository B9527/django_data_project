from rest_framework import serializers
from hive_app.models import HiveTable, HiveColumn, ImportTask


class HiveTableSerializers(serializers.ModelSerializer):
    class Meta:
        model = HiveTable
        fields = ('name', 'comment', 'columns', 'id')


class ColumnSerializers(serializers.ModelSerializer):
    class Meta:
        model = HiveColumn
        fields = ('table', 'name', 'comment', 'id')


class TaskSerializers(serializers.ModelSerializer):
    class Meta:
        model = ImportTask
        fields = ('reader', 'writer', 'options', 'status', 'id', 'start_time', 'finish_time','id')
