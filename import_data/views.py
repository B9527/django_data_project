# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import json
import datetime
from django.http import HttpResponseRedirect
from django.utils.datastructures import MultiValueDictKeyError
from django.views import View
from django.shortcuts import render, HttpResponse
from django.core.paginator import Paginator, InvalidPage, EmptyPage, PageNotAnInteger
from .models import ImportTask, CommandRecord, HiveTable
from .loader import read_excel
# restapi
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
# my error exexption
from utils.erroeException import MyException
# get_or_404
from django.shortcuts import get_object_or_404
from django.db.models import Q 


class UploadView(APIView):
    def post(self, request, format=None):

        return_data = {
            "msg": "上传出错，请重新上传！",
            "code": 400,
            "result": {

            }
        }
       
        # 读取excel表格信息
        post_request_data = self.request.data
        configs = []
        config_num = 0
        try:
            format_type = post_request_data['type']
            file_obj = post_request_data['file']
            if (file_obj.name).split('.')[1] not in ['txt', 'xlsx']:
                raise MyException(400, "上传文件类型不对！")
            configs = read_excel(file_obj, format_type)

            for config in configs:
                config_num = config_num + 1
                writer = config['writer']
                reader = config['reader']
                options = config['options']
                table_info = config['table_info']

                # save hive table and column information
                hive_table_args = {"database": table_info['database'], "table_comment": table_info['table_comment'], "table": table_info['table']}
                if HiveTable.objects.filter(**hive_table_args).exists():
                    hive_table_objlist = HiveTable.objects.filter(**hive_table_args)
                    for hive_table_obj in hive_table_objlist:
                        hive_table_obj.delete()

                for i in range(0, len(table_info['column_index'])):
                    hive_table = {}
                    hive_table['database'] = table_info['database']
                    hive_table['table_comment'] = table_info['table_comment']
                    hive_table['table'] = table_info['table']
                    hive_table['column'] = table_info['column'][i]
                    hive_table['column_comment'] = table_info['columns_comment'][i]
                    hive_table['column_index'] = table_info['column_index'][i]
                    hive_table['column_type'] = table_info['column_type'][i]
                    if hive_table['column_type'].lower() in ['date', 'timestamp', 'datetime']:
                        hive_table['time_format'] = table_info['coltime_formatumn_type'][i]
                    else:
                        hive_table['time_format'] = None

                    hive_table_obj = HiveTable(**hive_table)
                    hive_table_obj.save()

                # save import data task
                args = {'writer': writer, 'reader': reader, 'options': options}    
                if ImportTask.objects.filter(**args).exists():
                    raise MyException(400, "task %s already exists" % writer['table'])
                else:
                    args["start_time"] = datetime.datetime.now()
                    task = ImportTask(**args)
                    task.save()
            return_data['code'] = 200
            return_data['msg'] = "success"
        except MultiValueDictKeyError:
            return_data['code'] = 400
            return_data['msg'] = 'please upload you file'
        except KeyError as e:
            return_data['code'] = 400
            return_data['msg'] = str(e) + " is missing in " + file_obj.name
        except MyException as e:
            return_data['code'] = e.error_code
            return_data['msg'] = e.error_message
        except Exception as e:
            return_data['code'] = 400
            return_data['msg'] = str(e)
        finally:
            return Response(return_data, status=status.HTTP_200_OK)


class TaskListView(APIView):
    def get(self, request, format=None):
        print self.request.GET
        pageNum = 1
        pageSize = 20
        args = {}
        if 'id' in self.request.GET.keys():
            args['id'] = self.request.GET['id']
        if 'pageNum' in self.request.GET.keys():
            pageNum = int(self.request.GET['pageNum'])
        if 'pageSize' in self.request.GET.keys():
            pageSize = int(self.request.GET['pageSize'])
        
        if 'status' in self.request.GET.keys():
            status_ = self.request.GET['status']
            if status_ == "all":
                pass
            else:
                print status_
                args['status'] = status

        task_list = ImportTask.objects.filter(**args).order_by('-id')
        if 'search_data' in self.request.GET.keys():
            search_data = self.request.GET.dict()['search_data']
            task_list = task_list.filter(Q(reader__table__icontains=search_data) | Q(writer__table__icontains=search_data))

        total = task_list.count()
        task_list = task_list[(pageNum-1)*pageSize:pageNum*pageSize]
        pure_task_list = []
        for task in task_list:
            pure_task = {}
            pure_task['file_name'] = task.reader['file_name']
            pure_task['read_table'] = task.reader['table']
            pure_task['write_table'] = task.writer['table']
            pure_task['status'] = task.status
            pure_task['start_time'] = task.start_time
            pure_task['finish_time'] = task.finish_time
            pure_task['id'] = task.id
            pure_task_list.append(pure_task)
        # 返回数据格式
        return_data = {
            "msg": "success",
            "code": 200,
            "result": {"task_list": pure_task_list, "pageSize": pageSize, "pageNum": pageNum, "total": total, }
        }
        return Response(return_data, status=status.HTTP_200_OK)


class DelTaskView(APIView):
    def post(self, request):
        request_data = json.loads(self.request.data['request_delete_list'])
        print request_data
        for id in request_data:
            task_obj = get_object_or_404(ImportTask, pk=id)
            task_obj.delete()
        return_data = {
            "msg": "success",
            "code": 200,
            "result": {}
        }
        return Response(return_data, status=status.HTTP_200_OK)


class ChangeStatusSubmit(APIView):
    def post(self, request):
        request_data = json.loads(self.request.data['request_submit_list'])
        print request_data
        for id in request_data:
            task_obj = get_object_or_404(ImportTask, pk=id)
            task_obj.status = "submit"
            task_obj.save()
        return_data = {
            "msg": "success",
            "code": 200,
            "result": {}
        }
        return Response(return_data, status=status.HTTP_200_OK)


class TaskRecord(APIView):
    def post(self, request):
        import time
        print(time.time())
        args = {}
        if 'id' in request.data.keys():
            id = request.data['id']
            args['task_id'] = id
        print args
        task_record_list = CommandRecord.objects.filter(**args)
        pure_record_list = []
        for task_record in task_record_list:
            pure_record = {}
            pure_record['step'] = task_record.step
            pure_record['command'] = task_record.command
            pure_record['status'] = task_record.status
            pure_record['start_time'] = task_record.start_time
            pure_record['finish_time'] = task_record.finish_time
            pure_record['log'] = task_record.log
            pure_record_list.append(pure_record)
        return_data = {
            "msg": "success",
            "code": 200,
            "result": {"task_record": pure_record_list}
        }
        print return_data
        print time.time()
        return Response(return_data, status=status.HTTP_200_OK)