# coding:utf-8
import os
import json
import pandas as pd


def read_excel(file_obj, data_format):
    if data_format == 'sql':
        return read_excel_sql(file_obj)
    else:
        return read_excel_text(file_obj)


# 读取表格信息
def read_excel_sql(file_obj):
    df = pd.read_excel(file_obj)
   
    # header_info_list = df.head().keys()
    # need_keys = ['FromTable', 'sourcedatabase', 'FromDataFormat', 'FromIP', 'username', 'password', 'sourcecolumn'
    #              'ColumnInfo', 'ToTableinfo', 'ToTable', 'targetdatabase', 'TargetColumn', 'ColumnInfo']
    # for key in need_keys:
    #     if key not in header_info_list:
    #         raise(KeyError, "%s is nmissinf" % key)

    grouped = df.groupby('SourceTable')

    for src_table, item in grouped:
        reader = {}
        reader['type'] = "sql"
        reader['table'] = src_table
        reader['database'] = item['SourceDatabase'].iloc[0]
        reader['driver'] = item['Driver'].iloc[0]
        reader['host'] = item['Host'].iloc[0]
        reader['port'] = item['Port'].iloc[0]
        reader['username'] = item['Username'].iloc[0]
        reader['password'] = item['Password'].iloc[0]
        reader['columns'] = list(item['ColumnSourceName'])
        reader['comments'] = list(item['ColumnInfor'])
        reader['file_name'] = file_obj.name

        writer = {}
        writer['comment'] = item['TableInfor'].iloc[0]
        writer['table'] = item['HiveTable'].iloc[0]
        writer['database'] = item['HiveDatabase'].iloc[0]
        writer['columns'] = list(item[u'ColumnDestinationName'])
        writer['columns_comments'] = list(item[u'ColumnInfor'])

        options = {
            "hdfs_path": "/user/hive/import/%s" % reader['table'],
            "error_path": "/user/hive/error/%s" % reader['table'],
        }

        table_info = {}
        table_info['table_comment'] = item['TableInfor'].iloc[0]
        table_info['table'] = item['HiveTable'].iloc[0]
        table_info['database'] = item['HiveDatabase'].iloc[0]
        table_info['column'] = list(item[u'ColumnDestinationName'])
        table_info['columns_comment'] = list(item[u'ColumnInfor'])
        table_info['column_index'] = list(item[u'ColumnIndex'])
        table_info['column_type'] = list(item[u'ColumnDataType'])
        table_info['time_format'] = list(item[u'ColumnFormat'])

        config = {
            'reader': reader,
            'writer': writer,
            'options': options,
            'table_info': table_info
        }

        yield config


def read_excel_text(filepath):
    df = pd.read_excel(filepath)
    grouped = df.groupby('TextPath')
    for textpath, item in grouped:
        reader = {}
        reader['type'] = "text"
        reader['textpath'] = textpath
        reader['header'] = item['Header'].iloc[0]
        
        if isinstance(reader['header'], bool):
            reader['header'] = reader['header'].lower() == "true"
        reader['header'] = int(reader['header'])
        reader['columns'] = list(item['ColumnSourceName'])
        reader['comments'] = list(item['ColumnInfor'])

        writer = {}
        writer['comment'] = item['TableInfor'].iloc[0]
        writer['table'] = item['HiveTable'].iloc[0]
        writer['database'] = item['HiveDatabase'].iloc[0]
        writer['columns'] = list(item[u'ColumnDestinaionName'])
        writer['columns_comments'] = list(item[u'ColumnInfor'])

        basename = os.path.basename(reader['textpath'])
        path, _ = os.path.splitext(basename)
        options = {
            "hdfs_path": "/user/hive/import/%s" % path,
            "error_path": "/user/hive/error/%s" % path,
        }

        config = {
            'reader': reader,
            'writer': writer,
            'options': options
        }

        yield config


if __name__ == "__main__":
    filename = u"../../飞贷导入表-text.xlsx"
    for config in read_excel_text(filename):
        print json.dumps(config, indent=2)
