# -*- coding:utf-8 -*-
import os
import time
import json
import subprocess
from datetime import datetime
from clean_data.models import CleanConfig


def execute_shell(cmd):
    stdout = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    return stdout


def execute_validate(config, task):
    config_str = json.dumps(config)
    dir_path = os.path.dirname(os.path.abspath(__file__))
    transform_path = os.path.join(dir_path, "data_transform-0.1-py2.7.egg")
    spark_file = os.path.join(dir_path, "validate.py")
    args = "'%s'" % json.dumps(config)
    cmd = "sudo -u hive spark-submit --executor-memory 6g --py-files %s %s %s" % (transform_path, spark_file, args)

    log = None
    try:
        log = execute_shell(cmd)
        task.status = CleanConfig.STATUS_SUCCESS
        print "execute validate success"
    except subprocess.CalledProcessError, e:
        log = e.output
        task.status = CleanConfig.STATUS_FAIL
        print "execute validate fail"
    log = log.replace(chr(0), '')
    task.log = log
    task.finish_time = datetime.now()
    task.save()


def main():
    while True:
        task = CleanConfig.objects.filter(status=CleanConfig.STATUS_SUBMIT).first()
        # 修改status 为1：executing，start_time 更新
        if task is None:
            print "no submit task found, will sleep 5 seconds"
            time.sleep(5)
            continue
        print "start validate table %s.%s" % (task.reader['database'], task.reader['table'])
        task.status = CleanConfig.STATUS_EXECUTING
        task.start_time = datetime.now()
        task.finish_time = None
        task.log = None
        task.save()

        config = task.config
        execute_validate(config, task)



if __name__ == "__main__":
    main()
