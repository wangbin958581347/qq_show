from django.shortcuts import render

# Create your views here.

from django.http import HttpResponse
import requests
import PIL.Image as Image
import time

import pymysql





def insert_and_get_id(sql):
    """
    插入并获取其自增 ID：支持批量插入多条
    :return: {"1": 1, "2": 2, "3": 3}
    """
    db = pymysql.connect(
        host='101.201.46.114',
        port=3306,
        user="crypto_rw",
        password="crypto_rw_password",
        database="ido_server_bak"
    )
    cursor = db.cursor()
    cursor.execute(sql)
    insert_id = db.insert_id()
    db.commit()
    cursor.close()
    db.close()
    return insert_id


headers = {'Authorization':'Bearer 5_crM9D0ZQEjTmJm6P_J9CjAgxU06AKt0ZB-xeAb'}
def create_nft(request):
    if request.method == 'POST':
        layers = eval(request.POST.get('layers'))
        group_name = request.POST.get('group_name')
        group_id = request.POST.get('group_id')
        name = request.POST.get('name')
        occupation = request.POST.get('occupation')
        custom_name = request.POST.get('custom_name')
        original = request.POST.get('original')
        sex = request.POST.get('sex')

        if layers:
            lenth = len(layers)
            # 拼图
            layer = Image.open('./files/base/background/white.png').convert('RGBA')
            final = Image.new("RGBA", layer.size)
            score = 0
            composite_card = {
                'occupation':occupation,
                'custom_name':custom_name,
                'state':1,
                'original':original,
                'sex':sex,
                              }
            for x in range(lenth):
                x = str(x)
                element_name = layers[x]['property']
                png_name = layers[x]['name']
                score += layers[x]['score']
                composite_card[f'{element_name}_id'] = layers[x]['nft_id']
                layer = Image.open(f'./files/{group_name}/{element_name}/{png_name}.png').convert('RGBA')
                final = Image.alpha_composite(final, layer)

            # 图片存储
            save_path = f'./img_result/{group_name}/{name}.png'
            final.save(save_path)
            files = {'file': open(save_path, 'rb')}
            response = requests.post(
                "https://api.cloudflare.com/client/v4/accounts/06d48301c855502bf143d5a4c5d3a982/images/v1",
                files=files,
                headers=headers
                )
            image_link = response.json()['result']['variants'][0]
            info_value = {
                'nft_id': 0,
                'group_id': group_id,
                'type':'',
                'name': name,
                'owner': '',
                'image_link':image_link,
                'image_data': '',
                'score': score,
                'rank': 0,
                'created': 0,
                'state':0,
                'create_time': int(time.time() * 1000),
                'update_time': int(time.time() * 1000)
            }
            # 上传nft_info表
            sql = f"""
            insert into 
            nft_info(`nft_id`,`group_id`,`type`,`name`,`owner`,`image_link`,`image_data`,`score`,`rank`,`created`,`state`,`create_time`,`update_time`)
            values({info_value['nft_id']},{info_value['group_id']},"{info_value['type']}","{info_value['name']}","{info_value['owner']}","{info_value['image_link']}","{info_value['image_data']}",{info_value['score']},{info_value['rank']},{info_value['created']},{info_value['state']},{info_value['create_time']},{info_value['update_time']})
            """

            info_id = insert_and_get_id(sql)
            # 上传nft_composite_card表
            composite_card['info_id'] = info_id
            columns_str = ''
            value_str = ''
            for k,v in composite_card.items():
                columns_str += '`' + k + '`,'
                if k in ['occupation','custom_name']:
                    value_str += '"' + str(v) + '",'
                else:
                    value_str += str(v) + ','

            sql = f"""
            insert into 
            nft_composite_card({columns_str[:-1]})
            values({value_str[:-1]})
            """
            insert_and_get_id(sql)
            result = {'status': 'success', 'message': '上传成功'}
            return HttpResponse(str(result))
        else:
            result = {'status': 'faild', 'message': '图层数据为空'}
            return HttpResponse(str(result))

    else:
        result = {'status':'faild','message':'方法错误（POST）'}
        return HttpResponse(str(result))





