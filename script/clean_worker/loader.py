# coding:utf-8

import pandas as pd

from clean_data.models import CleanConfig, CleanMappingField
from django.db.utils import IntegrityError


# 读取表格信息
def read_excel(file_obj):
    df = pd.read_excel(file_obj)
    grouped = df.groupby('SourceTable')
    config = {}

    for src_table, item in grouped:
        reader = {}
        reader['table'] = src_table
        reader['database'] = item['SourceDatabase'].iloc[0]
        reader['file_name'] = file_obj.name

        success_writer = {}
        success_writer['table'] = item['SuccessTable'].iloc[0]
        success_writer['database'] = item['SuccessDatabase'].iloc[0]
        success_writer['mode'] = "append"

        error_writer = {}
        error_writer['table'] = item['ErrorTable'].iloc[0]
        error_writer['database'] = item['ErrorDatabase'].iloc[0]
        error_writer['mode'] = "overwrite"

        reader_fields = []
        writer_fields = []

        int_fields = []
        double_fields = []
        date_fields = []
        timestamp_fields = []
        idnum_fields = []
        mapping_fields = {}
        map_list = []

        for name, data_type, order, time_format in zip(item['ColumnName'], item['ColumnType'], item['ColumnIndex'], item['ColumnFormat']):

            reader_item = {
                'name': name,
                'type': 'string',
                'order': int(order),
            }
            reader_fields.append(reader_item)

            type_mapping = {"int": "int", 
                            "double": "double", 
                            "date": "date", 
                            "timestamp": "timestamp",
                            "string": "string",
                            "id_num": "string",
                            }
            writer_item = {
                'name': name,
                'type': type_mapping[data_type],
                'order': int(order)
            }
            writer_fields.append(writer_item)

            map_item = {
                'order': int(order),
                'name': name,
                "val": name
            }
            map_list.append(map_item)
            if data_type == "int":
                int_fields.append(name)
            elif data_type == "double":
                double_fields.append(name)
            elif data_type == "date":
                date_fields.append((name, time_format))
            elif data_type == "timestamp":
                timestamp_fields.append((name, time_format))
            elif data_type == "id_num":
                idnum_fields.append(name)
            


        reader['fields'] = reader_fields
        success_writer['fields'] = writer_fields
        error_writer['fields'] = reader_fields

        transformers = [
            {
                'path': 'data_transform.common.rename.FieldRenameTransformer',
                'params': {"mapping_fields": mapping_fields},
                'order': 1
            },
            {
                'path': 'data_transform.common.field.FieldStripTansformer',
                'params': {},
                'order': 2
            },
            {
                'path': 'data_transform.common.field.FieldConvertNull',
                'params': {'null_value': 'null'},
                'order': 3
            }
        ]

        order = 4
        if len(int_fields):
            int_transform = {
                'path': 'data_transform.common.field.IntTransformer',
                'params': {'fields': int_fields},
                'order': order
            }
            transformers.append(int_transform)
            order += 1
        if len(double_fields):
            double_transform = {
                'path': 'data_transform.common.field.DoubleTransformer',
                'params': {'fields': double_fields},
                'order': order
            }
            transformers.append(double_transform)
            order += 1
        if len(date_fields):
            items = []
            for (name, time_format) in date_fields:
                items.append({'name': name, 'format': time_format})

            date_transform = {
                'path': 'data_transform.common.field.DateTransformer',
                'params': {'fields': items},
                'order': order
            }
            transformers.append(date_transform)
            order += 1
        if len(timestamp_fields):
            items = []
            for (name, time_format) in timestamp_fields:
                items.append({'name': name, 'format': time_format})
            timestamp_tansform = {
                'path': 'data_transform.common.field.TimestampTransformer',
                'params': {'fields': items},
                'order': order
            }
            transformers.append(timestamp_tansform)
            order += 1
        if len(idnum_fields):
            idnum_transform = {
                'path': 'data_transform.common.field.DoubleTransformer',
                'params': {'fields': idnum_fields},
                'order': order
            }
            transformers.append(idnum_fields)
            order += 1
        # if len(CleanConfig.objects.filter(reader=reader, success_writer=success_writer) > 0):
        if len(CleanConfig.objects.filter(reader=reader, success_writer=success_writer)) > 0:
            raise IntegrityError
        task = CleanConfig(reader=reader, success_writer=success_writer,
                           error_writer=error_writer, transform=transformers)
        task.status = CleanConfig.STATUS_UPLOAD

        task.save()
        for map_item in map_list:
            clean_map = CleanMappingField(src_name=map_item['name'], dst_name=map_item['val'], index=map_item['order'], config=task)
            clean_map.save()
        # config['reader'] = reader
        # config['success_writer'] = success_writer
        # config['error_writer'] = error_writer
        # config['transform'] = transformers
        # yield config


if __name__ == "__main__":
    path = u"飞贷第二批清洗数据.xlsx"
    read_excel(path)
    # for cfg in read_excel(path):
    #     print json.dumps(cfg, indent=2)
    #     break
