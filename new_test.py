
# -*- coding: utf-8 -*-
import random

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

data = pd.read_excel('./files/10012/kiko_cat_info_df-0214.xlsx')

col = 'Pants'
card_group_id = 10012
piece = data[[col, col + '_score']]
piece = piece.drop_duplicates()
done_name = []
headers = {'Authorization': 'Bearer 5_crM9D0ZQEjTmJm6P_J9CjAgxU06AKt0ZB-xeAb'}

sql = f"""
select
info_id
from nft_composite_element
where type = "{col}"
"""
data = mysql_con.get_data_from_mysql(sql)
info_ids = set(data['info_id'].tolist())
info_id_str = ','.join([str(x) for x in info_ids])



for x,y in piece.values:
    if type(x) != float:
        png_name = x.split(' ##')[0]
        if png_name not in done_name:
            files = {'file': open(f'./files/{card_group_id}/{col}/{png_name}.png', 'rb')}
            print(files)
            a = requests.post(
                "https://api.cloudflare.com/client/v4/accounts/06d48301c855502bf143d5a4c5d3a982/images/v1",
                files=files,
                headers=headers
            )
            img_link = a.json()['result']['variants'][0]
            sql = f'''
                    update nft_info
                    set `image_link` = "{img_link}" 
                    where `name` like "%{png_name}%"
                    and `id` in ({info_id_str})
                    '''
            mysql_con.commit_sql(sql)
            done_name.append(png_name)
