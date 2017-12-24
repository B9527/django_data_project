# coding:utf-8
import os
import time
import subprocess
from datetime import datetime
from import_data.models import ImportTask, CommandRecord
from script.import_worker.cmd_builder import ImportCommandBuilder, JdbcBuidler


def remove_hdfs_path(path):
    dir_path = os.path.dirname(os.path.abspath(__file__))
    bash_path = os.path.join(dir_path, "hdfs_remove.sh")
    cmd = 'bash %s %s' % (bash_path, path)
    execute_shell(cmd)

def local_to_hdfs(config, record):
    hdfs_path = config['options']['hdfs_path']
    remove_hdfs_path(hdfs_path)
    local_path = config['reader']['textpath']
    cmd = "sudo -u hive hadoop fs -put {local_path} {hdfs_path}".format(local_path=local_path, hdfs_path=hdfs_path)
    record.command = cmd
    record.save()
    return execute_shell(cmd)

def sqoop_from_sql_to_hdfs(config, record):
    reader = config['reader']
    remove_hdfs_path(config['options']['hdfs_path'])
    jdbc_buidler = JdbcBuidler()
    jdbc_buidler.set_driver(reader['driver'])
    jdbc_buidler.set_host(reader['host'])
    jdbc_buidler.set_database(reader['database'])

    builder = ImportCommandBuilder()
    builder.set_jdbc_url(jdbc_buidler.build())
    builder.set_username(reader['username'])
    builder.set_password(reader['password'])
    builder.set_table(reader['table'])
    # builder.set_split_by("id")
    # builder.set_mapper_count(8)
    builder.set_columns(reader['columns'])
    fetch_size = config['options'].get('fetch_size')
    if fetch_size:
        builder.set_fetch_size(fetch_size)
    builder.set_target_dir(config['options']['hdfs_path'])
    cmd = builder.build()
    record.command = cmd
    record.save()
    return cmd, execute_shell(cmd)


def hdfs_to_hive(config, record):
    remove_hdfs_path(config['options']['error_path'])
    dir_path = os.path.dirname(os.path.abspath(__file__))
    pythonpath = os.path.join(dir_path, "hdfs2hive.py")
    writer = config['writer']
    columns = ",".join(writer['columns'])
    if config['options']['hdfs_path'].endswith('/'):
        config['options']['hdfs_path'] = config['options']['hdfs_path'].rstrip('/')
    textpath = config['options']['hdfs_path']
    if config['reader']['type'] == "sql":
        textpath = "%s/part*" % config['options']['hdfs_path']
    
    cmd = ("sudo -u hive spark-submit {pythonpath} {database} {table} {columns} "
            "{text_path} {error_path}".format(pythonpath=pythonpath, database=writer['database'],
            table=writer['table'], columns=columns, text_path=textpath, \
            error_path=config['options']['error_path']))

    option = " "
    if config['reader'].get('header', False):
        option += "--header"
    cmd = cmd + option
    record.command = cmd
    record.save()
    return execute_shell(cmd)


def execute_shell(cmd):
    stdout = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    return stdout


def init_record(task, step):
    record = task.records.filter(step=step).first()
    if record is None:
        record = CommandRecord(step=step, task=task)
    return record


def execute_record(record):
    record.start_time = datetime.now()
    record.finish_time = None
    record.status = "executing"
    record.log = None
    record.save()

    is_success = True
    stdout = None
    cmd = None
    try:
        if record.step == "sql2hdfs":
            stdout = sqoop_from_sql_to_hdfs(record.task.config, record)
        elif record.step == "hdfs2hive":
            stdout = hdfs_to_hive(record.task.config, record)
        elif record.step == "local2hdfs":
            stdout = local_to_hdfs(record.task.config, record)
    except subprocess.CalledProcessError, e:
        is_success = False
        record.log = e.output
        record.status = CommandRecord.STATUS_FAIL
    else:
        record.status = CommandRecord.STATUS_SUCCESS
        record.log = stdout

    record.finish_time = datetime.now()
    record.save()
    return is_success

def execute_sql_to_hive_task(task):
    print "start import data from source table %s to destination table %s" % (task.reader['table'], task.writer['table'])
    for step in ["sql2hdfs", "hdfs2hive"]:
        record = init_record(task, step)
        if record.status == CommandRecord.STATUS_SUCCESS:
            continue
        is_success = execute_record(record)
        if not is_success:
            print "execute step %s fail" % step
            task.status = ImportTask.STATUS_FAIL
            task.finish_time = datetime.now()
            task.save()
            return
        print "execute step %s success" % step
    task.status = ImportTask.STATUS_SUCCESS
    task.finish_time = datetime.now()
    task.save()

def execute_local_to_hive_task(task):
    print "start import data from local file %s to destination table %s" % (task.reader['textpath'], task.writer['table'])
    for step in ["local2hdfs", "hdfs2hive"]:
        record = init_record(task, step)
        if record.status == CommandRecord.STATUS_SUCCESS:
            continue
        is_success = execute_record(record)
        if not is_success:
            print "execute step %s fail" % step
            task.status = ImportTask.STATUS_FAIL
            task.finish_time = datetime.now()
            task.save()
            return
        print "execute step %s success" % step
    task.status = ImportTask.STATUS_SUCCESS
    task.finish_time = datetime.now()
    task.save()


def main():
    while True:
        # 获取submit状态下的任务,执行导入任务
        # 每次取一个
        task = ImportTask.objects.filter(status=ImportTask.STATUS_SUBMIT).first()
        # 修改status 为1：executing，start_time 更新
        if task is None:
            print "no submit task found, will sleep 5 seconds"
            time.sleep(5)
            continue

        task.status = ImportTask.STATUS_EXECUTING
        task.start_time = datetime.now()
        task.finish_time = None
        task.save()

        # 执行数据导入
        if task.reader['type'] == "text":
            execute_local_to_hive_task(task)
        elif task.reader['type'] == "sql":
            execute_sql_to_hive_task(task)


if __name__=="__main__":
    main()
