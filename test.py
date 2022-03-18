# -*- coding: utf-8 -*-


import pandas as pd

import time
import os
import pymysql

from sqlalchemy import create_engine
import requests

headers = {'Authorization': 'Bearer 5_crM9D0ZQEjTmJm6P_J9CjAgxU06AKt0ZB-xeAb'}


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

p = []
done_name = []
listdir3 = os.listdir('感恩节gif')
listdir3.remove('.DS_Store')
for name in listdir3:
    if name not in done_name:
        files = {'file': open(f'./感恩节gif/{name}', 'rb')}
        a = requests.post(
            "https://api.cloudflare.com/client/v4/accounts/06d48301c855502bf143d5a4c5d3a982/images/v1",
            files=files,
            headers=headers
        )

        r = {
            'nft_id': 0,
            'group_id': 10003,
            'name': f'Thanksgiving KIKO # {name.split(".")[0]}',
            'owner': '',
            'image_link': a.json()['result']['variants'][0],
            'image_data': '',
            'score': 0,
            'rank': 0,
            'created': 0,
            'create_time': int(time.time() * 1000),
            'update_time': int(time.time() * 1000)
        }
        print(r)
        p.append(r)
        done_name.append(name)

data = pd.DataFrame(p)
data = data.sort_values('name')

mysql_con.data_to_database(data, 'nft_info', index_type=False)

sql = """
select
id as info_id,
name
from nft_info
where group_id = 10003
"""
id_df = mysql_con.get_data_from_mysql(sql)

del id_df['name']

mysql_con.data_to_database(id_df, 'nft_kiko_cat', index_type=False)

