# -*- coding:utf-8 -*-
"""
spark python script
"""

import importlib
import argparse
import json
from functools import partial
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from data_transform import Record, TransformError


def import_object(func_path):
    mod_path, func_name = func_path.rsplit(".", 1)
    mod = importlib.import_module(mod_path)
    return getattr(mod, func_name)


def convert_fields(fields, record):
    items = []
    for column in fields:
        value = record[column['name']]
        items.append(value)
    return items


class Writer(object):
    
    def __init__(self, config):
        self.config = config
        self.fields = sorted(self.config['fields'], key=lambda x: x['order'])
    
    def create_table(self):
        column_sql = []
        for column in self.fields:
            column_sql.append("`%s` %s" % (column['name'], column['type']))
        column_sql = ",".join(column_sql)
        sql = """
        CREATE TABLE IF NOT EXISTS {database}.{table}(
            {column_sql})
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
            WITH SERDEPROPERTIES ('serialization.null.format'='') 
            STORED AS TEXTFILE""".format(database=self.config['database'], table=self.config['table'], column_sql=column_sql)

        print sql
        sqlContext.sql(sql)

    def truncate_table(self):
        sql = "truncate table %s.%s" % (self.config['database'], self.config['table'])
        sqlContext.sql(sql)

    
    def drop_table(self):
        sql = "DROP TABLE %s.%s PURGE" % (self.config['database'], self.config['table'])
        sqlContext.sql(sql)
    

    def write(self, records):
        if self.config['mode'] == "overwrite":
            self.drop_table()
        self.create_table()
        if self.config['mode'] == "overwrite":
            self.truncate_table()

        rdd = records.map(partial(convert_fields, self.fields))
        if rdd.isEmpty():
            return
        spark_types = {
                    "bigint": LongType,
                    "int": LongType,
                    "double": DoubleType,
                    "date": DateType,
                    "timestamp": TimestampType,
                    "string": StringType
                }
        fields = []
        for column in self.fields:
            name = column['name']
            spark_type = spark_types[column['type']]
            field = StructField(name, spark_type(), True)
            fields.append(field)
        schema = StructType(fields)
        dataframe = sqlContext.createDataFrame(rdd, schema)
        dst = "%s.%s" % (self.config['database'], self.config['table'])
        dataframe.saveAsTable(dst, mode="append")
    
    
        


class Reader(object):
    
    def __init__(self, config):
        self.config = config
    
    def read(self):
        dataframe = sqlContext.sql("SELECT * FROM %s.%s" % (self.config['database'], self.config['table']))
        return dataframe.rdd.map(lambda row: Record(row.asDict()))


class Transformers(object):
    def __init__(self, config):
        self.config = config
        self.transformers = self.load_transformer()

    def load_transformer(self):
        transformers = []
        for trans_cfg in sorted(self.config, key=lambda x: x['order']):
            trans_cls = import_object(trans_cfg['path'])
            t = trans_cls(trans_cfg['params'])
            transformers.append(t)
        return transformers

    def __call__(self, record):
        
        new_record = record.copy()
        for tran in self.transformers:
            try:
                new_record = tran.transform(new_record)
            except TransformError, e:
                record['_reason'] = e.msg
                return False, record
        return True, new_record


parser = argparse.ArgumentParser()
parser.add_argument("config", help='validate config json')
args = parser.parse_args()
config = json.loads(args.config)

sc = SparkContext()
sqlContext = HiveContext(sc)
log4jLogger = sc._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger('py4j')

reader = Reader(config['reader'])
transformers = Transformers(config['transform'])
ok_writer = Writer(config['ok_writer'])
error_writer = Writer(config['error_writer'])

records = reader.read()
middle = records.map(transformers).persist(StorageLevel.DISK_ONLY)
records_ok = middle.filter(lambda x: x[0]).map(lambda x: x[1])
ok_writer.write(records_ok)
records_error = middle.filter(lambda x: not x[0]).map(lambda x: x[1])
error_writer.write(records_error)

# records_error = middle.filter(lambda x: not x[0]).map(lambda x: x[1])


# config = {
#     "reader": {
#         "type": "hive",
#         "database": "default",
#         "table": "test_raw",
#         "column_order": ["id", "salary", "start_date", "start_datetime", "content"],
#         "column_type": {
#             "id": "string",
#             "salary": "string",
#             "start_date": "string",
#             "start_datetime": "string",
#             'content': 'string'
#         }
#     },

#     "success_writer": {
#         "type": "hive",
#         "mode": "append",
#         "database": "clean",
#         "table": "test_raw",
#         "column_order": ["id", "salary", "start_date", "start_datetime", "content"],
#         "column_type": {
#             "id": "int",
#             "salary": "double",
#             "start_date": "date",
#             "start_datetime": "timestamp",
#             'content': 'string'
#         }
#     },

#     # same as reader
#     "error_writer": {
#         "type": "hive",
#         "mode": "overwrite",
#         "database": "error",
#         "table": "test_raw",
#         "column_order": ["id", "salary", "start_date", "start_datetime", "content"],
#         "column_type": {
#             "id": "string",
#             "salary": "string",
#             "start_date": "string",
#             "start_datetime": "string",
#             'content': 'string'
#         }
#     }
# }