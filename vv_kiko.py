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


hb_mysql = {'host': '52.77.131.111',
            'user': 'admin',
            'password': 'OdeWzZNalcTPk2LAo0Lg',
            'database': 'ido_server',
            'port': 3306}

mysql_con = MysqlDb(host=hb_mysql.get('host'),
                    user=hb_mysql.get('user'),
                    password=hb_mysql.get('password'),
                    database=hb_mysql.get('database'))

card_group_id = 10019
path = f'./files/{card_group_id}/'
path1 = f'./img_result/{card_group_id}/'
num = 480

prefix = 'Vivid KIKO'
params_df = pd.read_excel(f'./files/{card_group_id}/稀有值设计-KIKO 0321-v2.xlsx')

classes = params_df['分类'].unique()
class_info = {}
for class1 in classes:
    info_dict = {}
    part = params_df[params_df['分类'] == class1]
    part_infos = part.to_dict('records')
    for part_info in part_infos:
        l1 = [part_info['Frequency']]
        l2 = [part_info['分类'], part_info['属性']]
        l3 = [part_info['Class'], part_info['Trait']]
        l4 = [part_info['对应素材编号']]
        info_dict[part_info['属性']] = {'l1': l1, 'l2': l2, 'l3': l3, 'l4': l4}

    class_info[class1] = info_dict

layers = ['背景',
          '身体',
          '头部',
          '表情',
          '衣服',
          '配饰',
          ]

path_dict = {
    '背景': 'Background',
    '尾巴': 'Tail',
    '身体': 'Body',
    '表情': 'Facial Expression',
    '左手': 'Left Hand',
    '裤子': 'Pants',
    '鞋子': 'Shoes',
    '衣服': 'Clothes',
    '头部': 'Head',
    '右手': 'Right Hand',
    '配饰': 'Accessories'
}


def select_element(class_info, element, num=500):
    num = np.random.randint(1, num + 1)
    for x in class_info[element]:
        th = class_info[element][x]['l1'][0]
        if num <= th:
            return x


i = 0
result_list = []
kiko_cat_info_list = []
chi_name_list = []
while i < num:
    chi_name = ''
    en_name = ''
    files_name = ''
    score = 0
    step = 0
    info_id = int(time.time() * 10 ** 3)
    layer_list = []
    kiko_cat_info = {}
    final_info = {}
    for element in layers:
        r = select_element(class_info, element, num)
        final_info[element] = r
        chi_name += '_' + f'{class_info[element][r]["l2"][0]}({class_info[element][r]["l2"][1]})'
        en_name += '_' + f'{class_info[element][r]["l3"][0]}({class_info[element][r]["l3"][1]})'
        kiko_cat_info.update(
            {
                class_info[element][r]["l3"][0].lower(): class_info[element][r]["l3"][1],
            }
        )
        file_name = f'{class_info[element][r]["l4"][0]}'

        files_name += '_' + f'{class_info[element][r]["l2"][0]}({file_name})'

        if file_name != 'nan':
            layer = Image.open(path + f'{path_dict[element]}/{file_name}.png').convert('RGBA')
            layer_list.append(layer)

    if chi_name in chi_name_list:
        continue

    chi_name_list.append(chi_name)

    for layer in layer_list:
        if step == 0:
            final = Image.new("RGBA", layer.size)
            final = Image.alpha_composite(final, layer)
        else:
            final = Image.alpha_composite(final, layer)
        step += 1

    nft_name = f'{prefix} # {i + 1}'
    # final=final.convert('RGB').resize((500,500))
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

    print(f"num:{i}")
    print(f'''
中文:{chi_name},
英文:{en_name},
文件:{files_name},
分数:{score}
          ''')
    i += 1



# 特殊图片
special_file = path + '夜光稀有/'
listdirs = os.listdir(special_file)
listdirs.remove('.DS_Store')

for file in listdirs :
    kiko_cat_info = {}
    nft_name = f'{prefix} # {i + 1}'
    final = Image.open(special_file + file ).convert('RGBA')
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

info_df.to_excel(f'{prefix}_info.xlsx',index = False)
kiko_cat_info_df.to_excel(f'{prefix}_kikocat.xlsx',index = False)


cols = [col for col in kiko_cat_info_df.columns if
        col not in ['info_id', 'create_time', 'update_time', 'type', 'name', 'group_id']]
score_cols = []
for element in cols:
    element_df = kiko_cat_info_df[[element]]
    element_df.loc[:,'name'] = [x.split(' ##')[0] for x in element_df[element]]
    element_df.index = element_df.name
    element_df.loc[:,'score'] = len(element_df) / element_df['name'].value_counts()
    element_score = element_df[[element,'score']].set_index(element).to_dict()['score']
    kiko_cat_info_df[f'{element}_score'] = kiko_cat_info_df[element].replace(element_score)
    score_cols.append(f'{element}_score')

kiko_cat_info_df['score'] = kiko_cat_info_df[score_cols].sum(axis=1)


headers = {'Authorization': 'Bearer 5_crM9D0ZQEjTmJm6P_J9CjAgxU06AKt0ZB-xeAb'}
# 存合成的图片相关数据
# 先去nft_composite_element 表中获取元素的id

done_name = []
card_info_list = []


for name in info_df['name']:
    if name not in done_name:
        files = {'file': open(f'./img_result/{card_group_id}/{name}.png', 'rb')}
        a = requests.post(
            "https://api.cloudflare.com/client/v4/accounts/06d48301c855502bf143d5a4c5d3a982/images/v1",
            files=files,
            headers=headers
        )
        score = kiko_cat_info_df[kiko_cat_info_df['name'] == name]['score'].iloc[0]
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

