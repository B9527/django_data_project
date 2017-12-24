"""import_data URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.11/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url, include
from django.contrib import admin
import import_data.views
import clean_data.views


import_data_urls = [
    url(r'^upload/$', import_data.views.UploadView.as_view()),
    url(r'^tasks/list/$', import_data.views.TaskListView.as_view()),
    url(r'^task/record/$', import_data.views.TaskRecord.as_view()),
    # url(r'^change_task_status/(?P<pk>[0-9]+)/$', import_data.views.change_task_status),
    url(r'^delete_task_view/$', import_data.views.DelTaskView.as_view()),
    url(r'^change_status_submit/$', import_data.views.ChangeStatusSubmit.as_view())
]


clean_data_urls = [
    # url(r'^$', clean_data.views.home_view),
    url(r'^upload/$', clean_data.views.UploadView.as_view()),
    url(r'^task/list/$', clean_data.views.TaslList.as_view()),
    url(r'^task/delete/$', clean_data.views.DelCleanTask.as_view()),
    url(r'^task/resubmit/$', clean_data.views.ChangeCleanTaskSubmit.as_view()),
    url(r'^task/detail/$', clean_data.views.CleanDetailView.as_view()),
    url(r'^task/map_list/$', clean_data.views.SaveMapList.as_view()),

]


urlpatterns = [
    url(r'^login/$', clean_data.views.LoginView.as_view()),
    url(r'^admin/', admin.site.urls),
    url(r'^import-data/', include(import_data_urls)),
    url(r'^clean-data/', include(clean_data_urls)),
]






