# -*- coding: utf-8 -*-
import os

import PIL.Image as Image
import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine
import pymysql
import requests
import base64


class MysqlDb(object):
    def __init__(self, user, password, host, database, port=3306):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.port = port
        connect_str = 'mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8' % (self.user,
                                                                       self.password,
                                                                       self.host,
                                                                       self.port,
                                                                       self.database
                                                                       )
        self.yconnect = create_engine(connect_str)

    # 从数据库中获取实时数据
    def get_data_from_mysql(self, sql):
        con = pymysql.Connect(host=self.host,
                              user=self.user,
                              password=self.password,
                              db=self.database,
                              charset='utf8',
                              port=int(self.port))
        data = pd.read_sql(sql, con)
        con.close()
        return data

    # 上传数据到数据库中
    def data_to_database(self, data, table_name, index_type):
        data.to_sql(table_name,
                    self.yconnect,
                    if_exists='append',
                    index=index_type)

    # commit_sql
    def commit_sql(self, sql):
        con = pymysql.Connect(host=self.host,
                              user=self.user,
                              password=self.password,
                              db=self.database,
                              charset='utf8',
                              port=int(self.port))
        cur = con.cursor()
        cur.execute(sql)
        con.commit()
        con.close()


hb_mysql = {'host': '13.213.126.178',
            'user': 'admin',
            'password': 'Z1P8q4moD9NjAZxC9CDc',
            'database': 'ido_server',
            'port': 3306}

mysql_con = MysqlDb(host=hb_mysql.get('host'),
                    user=hb_mysql.get('user'),
                    password=hb_mysql.get('password'),
                    database=hb_mysql.get('database'))

card_group_id = 10020
path = f'/Users/bixin/愚人老师/原图/'
path1 = f'/Users/bixin/愚人老师/新图/'
files = os.listdir(path)
prefix = '3EYED Cat'

result_list = []
kiko_cat_info_list = []
i = 0
for file in files :
    kiko_cat_info = {}
    nft_name = f'{prefix} # {i + 1}'
    final = Image.open(path + file ).convert('RGBA')
    path2 = f'{path1}{nft_name}.png'
    final.save(path2)
    image_base64 = ''
    picture_info = {
        'name': nft_name,
        'nft_id': 0,
        'group_id': card_group_id,
        'image_data': image_base64,
        'image_link': 111,
        'create_time': int(time.time() * 1000),
        'update_time': int(time.time() * 1000),
    }
    result_list.append(picture_info)
    kiko_cat_info['name'] = nft_name
    kiko_cat_info['group_id'] = card_group_id
    kiko_cat_info['create_time'] = int(time.time() * 1000)
    kiko_cat_info['update_time'] = int(time.time() * 1000)
    kiko_cat_info_list.append(kiko_cat_info)
    i += 1


info_df = pd.DataFrame(result_list)
kiko_cat_info_df = pd.DataFrame(kiko_cat_info_list)


headers = {'Authorization': 'Bearer 5_crM9D0ZQEjTmJm6P_J9CjAgxU06AKt0ZB-xeAb'}
# 存合成的图片相关数据
# 先去nft_composite_element 表中获取元素的id

done_name = []
card_info_list = []


for name in info_df['name']:
    if name not in done_name:
        files = {'file': open(f'{path1}/{name}.png', 'rb')}
        a = requests.post(
            "https://api.cloudflare.com/client/v4/accounts/06d48301c855502bf143d5a4c5d3a982/images/v1",
            files=files,
            headers=headers
        )
        score = 0
        r = {
            'nft_id': 0,
            'group_id': card_group_id,
            'name': name,
            'type':'composite_card',
            'owner': '',
            'image_link': a.json()['result']['variants'][0],
            'image_data': '',
            'rank': 0,
            'created': 0,
            'score':score,
            'create_time': int(time.time() * 1000),
            'update_time': int(time.time() * 1000)
        }
        print(r)
        card_info_list.append(r)
        done_name.append(name)

card_info_df = pd.DataFrame(card_info_list)
mysql_con.data_to_database(card_info_df, 'nft_info', index_type=False)


sql = f"""
select
id as info_id,
name
from nft_info
where group_id = {card_group_id}
"""
id_df = mysql_con.get_data_from_mysql(sql)


info = pd.merge(kiko_cat_info_df, id_df,on = 'name')

del info['name']
del info['score']
del info['group_id']


info =  info.rename(columns = {'facial expression':'facial_expression',
                              'facial expression_score':'facial_expression_score'}
                    )
mysql_con.data_to_database(info,'nft_kiko_cat',index_type=False)

