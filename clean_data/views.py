# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.http import HttpResponseRedirect
from django.shortcuts import render, HttpResponse
from django.views import View
from django.shortcuts import get_object_or_404
from .models import CleanConfig, CleanMappingField
import json
from django.core.paginator import Paginator, InvalidPage, EmptyPage, PageNotAnInteger
from script.clean_worker.loader import read_excel
from django.db.utils import IntegrityError
from django.contrib.auth import authenticate, login as user_login
# restapi
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.db.models import Q


class LoginView(APIView):
    def post(self, request):

        return_data = {
            "msg": "用户名或密码错误！",
            "code": 400,
            "user": {
                "name": "",
            }
        }
        try:
            name = request.data['username']
            password = request.data['password']
            user = authenticate(username=name, password=password)
            if user is None:
                pass
            else:
                user_login(request, user)
                return_data['msg'] = 'success'
                return_data['code'] = 200
                return_data['user']['name'] = user.username
        except Exception:
            pass
        return Response(return_data, status=status.HTTP_200_OK)


class UploadView(APIView):

    def post(self, request):

        # 返回数据结构
        return_data = {
            "msg": "上传出错，请重新上传！",
            "code": 400,
            "result": {

            }
        }    

        # 读取excel表格信息
        post_request_data = self.request.data
        try:
            file_obj = post_request_data['file']
            read_excel(file_obj)
            return_data['msg'] = 'success'
            return_data['code'] = 200
            return Response(return_data, status=status.HTTP_200_OK)
        except IntegrityError:
            return_data['code'] = 500
            return_data['msg'] = "上传失败，请勿重复上传！"
        finally:
            return Response(return_data, status=status.HTTP_200_OK)


class TaslList(APIView):
    def get(self, request, format=None):
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
                args['status'] = status_

        task_list = CleanConfig.objects.filter(**args).order_by('-id')

        if 'search_data' in self.request.GET.keys():
            search_data = self.request.GET.dict()['search_data']
            task_list = task_list.filter(Q(reader__table__icontains=search_data) | Q(reader__database__icontains=search_data))

        total = task_list.count()
        task_list = task_list[(pageNum-1)*pageSize:pageNum*pageSize]
        pure_task_list = []
        for task in task_list:
            pure_task = {}
            pure_task['file_name'] = task.reader['file_name']
            pure_task['database'] = task.reader['database']
            pure_task['table'] = task.reader['table']
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


class CleanDetailView(APIView):
    def get(self, request, format=None):
        task_id = request.GET['id']
        clean_config = get_object_or_404(CleanConfig, pk=task_id)
        reader = clean_config.reader
        reader['fields'] = sorted(reader['fields'],  key=lambda x: int(x['order']))
        map_list = CleanMappingField.objects.filter(config=clean_config).order_by('index')
        map_key_list = []
        map_val_list = []
        for map in map_list:
            temp_map_key = {}
            temp_map_val = {}
            temp_map_key['src_name'] = map.src_name
            temp_map_val['dst_name'] = map.dst_name
            temp_map_key['index'] = map.index
            temp_map_val['index'] = map.index
            map_key_list.append(temp_map_key)
            map_val_list.append(temp_map_val)

        transform_list = sorted(clean_config.transform, key=lambda x: int(x['order']))

        writer = clean_config.success_writer
        writer['fields'] = sorted(writer['fields'], key=lambda x: int(x['order']))
        # 返回数据格式
        return_data = {
            "msg": "success",
            "code": 200,
            "result": {
                "reader": reader,
                "map_list": {
                    "key_list": map_key_list,
                    "val_list": map_val_list
                },
                "transform": transform_list,
                "writer": writer,
            },
        }
        return Response(return_data, status=status.HTTP_200_OK)

    def post(self, request, format=None):
        try:
            code = 200
            msg = "success"
            request_data = request.data
            if 'id' in request_data.keys():
                task_id = request.data['id']
                clean_config = get_object_or_404(CleanConfig, pk=task_id)
                if 'reader' in request_data.keys():
                    reader = json.loads(request_data['reader'])
                    reader['fields'] = sorted(reader['fields'],  key=lambda x: int(x['order']))
                    for i in range(len(reader['fields'])):
                        reader['fields'][i]['order'] = i + 1
                    
                    # 对reader 进行排序
                    clean_config.reader = reader
                if 'writer' in request_data.keys():
                    # 对writer进行排序
                    writer = json.loads(request_data['writer'])
                    writer['fields'] = sorted(writer['fields'],  key=lambda x: int(x['order']))
                    for i in range(len(writer['fields'])):
                        writer['fields'][i]['order'] = i + 1
                    clean_config.success_writer = writer
                if 'transform' in request_data.keys():
                    # 对transform 进行排序
                    transform = json.loads(request_data['transform'])
                    transform = sorted(transform, key=lambda x: int(x['order']))
                    for i in range(len(transform)):
                        transform[i]['order'] = i + 1
                    clean_config.transform = transform
                clean_config.save()
            else:
                code = 400
                msg = "id 未传！"
        except Exception as e:
            code = 400
            msg = str(e)

        return_data = {
            "msg": msg,
            "code": code,
            "result": {
            },
        }
        return Response(return_data, status=status.HTTP_200_OK)


# 删除clean_task
class DelCleanTask(APIView):
    def post(self, request):
        clean_task_list = json.loads(request.POST['request_delete_list'])
        for id in clean_task_list:
            clean_config = get_object_or_404(CleanConfig, pk=id)
            clean_config.delete()
        # 返回数据结构
        return_data = {
            "msg": "success",
            "code": 200,
            "result": {

            }
        }
        return Response(return_data, status=status.HTTP_200_OK)


# 修改clean_task 状态为submit
class ChangeCleanTaskSubmit(APIView):
    def post(self, request):
        clean_task_list = json.loads(request.POST['request_submit_list'])
        for id in clean_task_list:
            print id
            clean_config = get_object_or_404(CleanConfig, pk=id)
            clean_config.status = "submit"
            clean_config.save()
        # 返回数据结构
        return_data = {
            "msg": "success",
            "code": 200,
            "result": {

            }
        }
        return Response(return_data, status=status.HTTP_200_OK)


# 保存map——list
class SaveMapList(APIView):
    def post(self, request, format=None):
        try:
            msg = "success"
            code = 200

            id = request.data['id']
            map_list = json.loads(request.data['map_list'])
            clean_config = get_object_or_404(CleanConfig, pk=id)
            old_map_list = CleanMappingField.objects.filter(config=clean_config)

            # 初始化 map——list
            for old_map in old_map_list:
                old_map.delete()
            
            # 保存新的map-list
            key_list = sorted(map_list['key_list'],  key=lambda x: int(x['index']))
            val_list = sorted(map_list['val_list'],  key=lambda x: int(x['index']))

            min_len = min(len(key_list), len(val_list))

            for i in range(min_len):
                cleanMap = CleanMappingField(src_name=key_list[i]['src_name'], dst_name=val_list[i]['dst_name'], index=i, config=clean_config)
                cleanMap.save()
        except Exception as e:
            msg = str(e)
            code = 400
        # 返回数据结构
        return_data = {
            "msg": msg,
            "code": code,
            "result": {

            }
        }
        return Response(return_data, status=status.HTTP_200_OK)



