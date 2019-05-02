#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import re
import pandas as pd
from sqlalchemy import create_engine
import os
import time

pd.set_option('display.max_columns', None)
from IPython.display import display


"""
每次获取的数据最多50条，当有大量数据时，需要根据response.headers 中的Link
参数获取下一次数据访问的链接。参见文档链接
https://github.com/jinshuju/jinshuju-api-docs/blob/master/personal-api.md#%E5%88%86%E9%A1%B5
1. 获取所有表单列表，得到 name-token等映射
2. 获取表单结构数据与明细数据
3. 获取表结构 field - name 对应关系
4. 对特殊表单结构统一转化为 数值/文字类型
5. 存储表单（database/sql),每张调查表-> 数据表
"""
PATTERN = re.compile(r'<(.*)>; rel="next"')
PATH = 'jinshuju_data'

def get_token():
  """
  jinshuju API document:https://github.com/jinshuju/jinshuju-api-docs/blob/\
  master/enterprise-api-org-client-credentials-grant-flow.md

  """
  URL = 'https://account.jinshuju.com/oauth/token?grant_type=org_client_credentials&client_id=90ef16fd6818b1e6127af56a54f5af3a1160fde2a7367605a69b602176458ca5&client_secret=7607979c04b673a4a45072d238202612f37cc973ff199344011eb9c8e5dd870b'
  r = requests.post(URL)
  if r.status_code == 200:
    return r.json()['access_token']
  else:
    return 'error'

def get_all_forms(access_token):
  init_url = 'https://api.jinshuju.com/v4/'+ 'forms'+ '/?access_token=' +access_token+'&per_page=50'
  r = requests.get(init_url)
  form_list = []

  while (r.status_code == 200):
    form_list += r.json()
    if ( 'next' not in r.headers['Link']):
      break
    url = re.findall(PATTERN,r.headers['Link'])[0]
    r = requests.get(url)

  if r.status_code != 200:
    return 'error'
  else:
    return form_list


def get_forms(access_token,form_token,name):
  form_struct_url = 'https://api.jinshuju.com/v4/'+ 'forms/'+ form_token +   '?access_token=' +access_token+'&per_page=50'
  form_data_url = 'https://api.jinshuju.com/v4/'+ 'forms/'+ form_token +    '/entries?access_token=' +access_token+'&per_page=50'
  r = requests.get(form_struct_url)

  if r.status_code == 200:
    fields = r.json()['fields']
    table_name = r.json()['name'] + r.json()['token']
    table_name2 = r.json()['name'] +'_'+ r.json()['token']
  api_code_label_map = {}
  choice_list = []
  for x in fields:
    api_code_label_map[x['api_code']] = x['label']
    api_code_label_map['type'] = x['type']
    d = {}
    
    if x['type'] == 'single_choice':
      field_replace_dict_map={}
      d['api_code'] = x['api_code']
      for m in x['choices']:
        field_replace_dict_map[m['value']] = m['name']
      d['replace_dict'] = field_replace_dict_map
      choice_list.append(d)
        
  r = requests.get(form_data_url)   
  data_list = []
  while (r.status_code == 200):
    data_list += r.json()
    if 'Link' in r.headers:
      if ( 'next' not in r.headers['Link']):
        break
#             the last part of the data only have the prev_link
      url = re.findall(PATTERN,r.headers['Link'].split(',')[-1])
      if len(url)>0:
        r = requests.get(url[0])
      else:
        break
    else:
        break
   
  if r.status_code == 200:
    df = pd.DataFrame(data_list)
    if len(df) == 0:
      return 'error','error'
    for x in choice_list:
      df[x['api_code']].replace(x['replace_dict'],inplace=True)
    if 'x_field_weixin_province_city' in df.columns.tolist():#手工消除列表元素为字典（‘{}’）的项，将其替换为‘’（空字符）
      df['x_field_weixin_province_city']=df['x_field_weixin_province_city'].map(lambda x:'')
    df.rename(columns=api_code_label_map,inplace=True)
    df['table_name']=table_name2#增加一列table标签，便于在chartio内按table做filter
    #df['table_name']= df['table_name'].map(lambda x:'Udacity学习体验问卷-DAT_qkAepv')
    #print(df)
    if form['name'] == 'Udacity学习体验问卷-RF':
        df['table_name'] = df['table_name'].map(lambda x:'regular-cohort-failed')
    if form['name'] == 'Udacity学习体验问卷-R1':
        df['table_name'] = df['table_name'].map(lambda x:'regular-P1')
    if form['name'] == 'Udacity学习体验问卷-V1':
        df['table_name']= df['table_name'].map(lambda x:'VIP-P1')
    if form['name'] == 'Udacity学习体验问卷-VF':
        df['table_name'] = df['table_name'].map(lambda x:'VIP- cohort-failed')
    if form['name'] == 'Udacity学习体验问卷-VG':
        df['table_name'] = df['table_name'].map(lambda x:'VIP-graduate')
    if form['name'] == 'Udacity学习体验问卷-RGC':
        df['table_name'] = df['table_name'].map(lambda x:'regular-graduate-classroom')
    if form['name'] == 'Udacity学习体验问卷-BT':
        df['table_name'] = df['table_name'].map(lambda x:'BAND-trial')
    if form['name'] == 'Udacity学习体验问卷-DBT':
        df['table_name'] = df['table_name'].map(lambda x:'DAND-basic-trial')
    if form['name'] == 'Udacity学习体验问卷-DAT':
        df['table_name'] = df['table_name'].map(lambda x:'DAND-advanced-trial')
    if form['name'] == 'Udacity学习体验问卷-MVL':
        df['table_name'] = df['table_name'].map(lambda x:'MLND-VIP lite')
    if form['name'] == 'Udacity学习体验问卷-AVL':
        df['table_name'] = df['table_name'].map(lambda x:'AIPND-VIP lite')
    if form['name'] == 'Udacity学习体验问卷-RGW':
        df['table_name'] = df['table_name'].map(lambda x:'regular-graduate-WeChat')
    if form['name'] == 'Udacity学习体验问卷-R':
        df['table_name'] = df['table_name'].map(lambda x:'refund')
    return table_name,df
  else:
    return 'error'


def restore_data(df,table_name,data_type):
  if data_type == 'sql':
    table_name = table_name.replace(r'/','')#修改表名格式，使其不超过20字符。
#     table_name = table_name.replace('优达学城','')#命名规则为：删除‘优达学城’字样，将'学习服务体验问卷'字样替换成'NPS_'
#     table_name = table_name.replace('学习服务体验问卷','NPS_')
    engine = create_engine("postgresql+psycopg2://miscellaneous:,RBA2RMC9tBbGuNZ@miscellaneous.cwdfrpky5xdi.rds.cn-north-1.amazonaws.com.cn",use_batch_mode=True)
    df.to_sql(table_name,engine,if_exists='replace')#如果table名过长会出现replace不成功，因为table名被截断成20字符
  elif data_type == 'csv':
    table_name = table_name.replace(r'/','')
    path = './'+PATH+'/'+table_name+'.csv'
    df.to_csv(path)
  else:
    return 'error'


if __name__ == "__main__":
  if not os.path.exists(PATH):
    os.mkdir(PATH)
    
  token = get_token()
  form_list = get_all_forms(token)
  i=0
  for form in form_list:
    i+=1
    if re.match(r'^.[d][a][c][i][t][y][学][习][体][验][问][卷].*',form['name']+ '_&_'+form['token']):#匹配表单：‘^Udacity学习体验问卷.*’
        print(i,form['name']+ '_&_'+form['token'],time.localtime(time.time()))#,time.localtime(time.time()))
        name = form['name']
        table,df = get_forms(token,form['token'],name)
        table_name = form['name']+ '_&_'+form['token']

        if table!='error' and table :
            restore_data(df,table_name,'sql')
  for form in form_list:#为了将DAT/DBT的11.15的调查表数据接到总表后面
    i+=1
    #print(i,form['name']+ '_&_'+form['token'],time.localtime(time.time()))
    if re.match(r'^.[达][学][城][「][数][据][分][析][进][阶][试][学][班][」][学][习][服][务][体][验][问][卷].*',form['name']+ '_&_'+form['token']):
        print(i,form['name']+ '_&_'+form['token'],time.localtime(time.time()))
        table_name = 'DAND-advanced-trial'
        name = form['name']
        table,df = get_forms(token,form['token'],name)
        if '11/14' in  df.columns.tolist():#手工消除列表名为‘11.14’的列
            df.drop('11/14',axis=1, inplace=True)
        if '我们的课程内容帮助你掌握了通关实战项目需要的技能吗?' in df.columns.tolist():
            df.rename(columns={'我们的课程内容帮助你掌握了通关实战项目需要的技能吗?':'我们的课程内容有没有帮助到你？'},inplace = True)

        df['table_name']= df['table_name'].map(lambda x:'Udacity学习体验问卷-DAT_qkAepv')
        if table!='error' and table :
            table_name = table_name.replace(r'/','')#修改表名格式，使其不超过20字符。
            engine = create_engine("postgresql+psycopg2://miscellaneous:,RBA2RMC9tBbGuNZ@miscellaneous.cwdfrpky5xdi.rds.cn-north-1.amazonaws.com.cn",use_batch_mode=True)
            df.to_sql(table_name,engine,if_exists='append')
    if re.match(r'^.[达][学][城][「][数][据][分][析][入][门][试][学][班][」][学][习][服][务][体][验][问][卷].*',form['name']+ '_&_'+form['token']):
        print(i,form['name']+ '_&_'+form['token'],time.localtime(time.time()))
        name = form['name']
        table,df = get_forms(token,form['token'],name)
        table_name = 'DAND-basic-trial'
        
        if '11/14' in  df.columns.tolist():#手工消除列表名为‘11.14’的列
            df.drop('11/14',axis=1, inplace=True)
        if '我们的课程内容帮助你掌握了通关实战项目需要的技能吗?' in df.columns.tolist():
            df.rename(columns={'我们的课程内容帮助你掌握了通关实战项目需要的技能吗?':'我们的课程内容有没有帮助到你？'},inplace = True)
        df['table_name']= df['table_name'].map(lambda x:'Udacity学习体验问卷-DBT_Q9u6yp')
        if table!='error' and table :
            table_name = table_name.replace(r'/','')#修改表名格式，使其不超过20字符。
            engine = create_engine("postgresql+psycopg2://miscellaneous:,RBA2RMC9tBbGuNZ@miscellaneous.cwdfrpky5xdi.rds.cn-north-1.amazonaws.com.cn",use_batch_mode=True)
            df.to_sql(table_name,engine,if_exists='append')

  print("END MATCH")


# In[ ]:




