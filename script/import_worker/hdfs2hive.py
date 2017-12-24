#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" This is a spark read test on hive. """
import sys
import argparse
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *

parser = argparse.ArgumentParser()
parser.add_argument("database", help='hive database')
parser.add_argument("table", help='hive table')
parser.add_argument("columns", help='hive columns')
parser.add_argument("textfile", help='hdfs path')
parser.add_argument("error_path", help='error record path')

parser.add_argument("--delimiter", default=chr(1), help='field delimiter')
parser.add_argument("--encode", default='utf-8', help='file encode')
parser.add_argument("--header", action='store_true', help='contain header line')
parser.add_argument("--overwrite", action='store_true', help='overwrite hive table')


args = parser.parse_args(sys.argv[1:])
columns = args.columns.split(",")
fields_num = len(columns)
sc = SparkContext()
sqlContext = HiveContext(sc)
print args


def create_hive_table(table, columns):
    column_sql = ",".join(["`%s` string" % col for col in columns])
    sql = "CREATE TABLE IF NOT EXISTS {table} ({column_sql})".format(table=table, column_sql=column_sql)
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger('py4j')
    logger.info(sql)
    sqlContext.sql(sql)

def is_validate(record):
    items = record.split(args.delimiter)
    if len(items) != fields_num:
        return record, False
    return record, True


# data = sc.textFile(textfile, use_unicode=False)
data = sc.textFile(args.textfile)
if args.header:
    header_line = data.first()
    data = data.filter(lambda line: line != header_line)
all_data = data.map(is_validate).cache()

error_data = all_data.filter(lambda x: not x[1]).map(lambda x: x[0])
error_data.saveAsTextFile(args.error_path)

uniform_data = all_data.filter(lambda x: x[1]).map(lambda x: x[0].split(args.delimiter))
fields = [StructField(field, StringType(), nullable=True) for field in columns]
schema = StructType(fields)
df = sqlContext.createDataFrame(uniform_data, schema)
hive_table = "%s.%s" % (args.database, args.table)
create_hive_table(hive_table, columns)
mode = "append"
if args.overwrite:
    mode = "overwrite"
df.write.saveAsTable(hive_table, format="text", mode=mode)
