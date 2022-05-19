# init odps and o, D2 will prepare them automatically using pyodpswrapper.py
from io import StringIO
from odps import ODPS
from odps import DataFrame
from odps.models import Schema, Column, Partition
import pandas as pd
from collections import defaultdict
from datetime import datetime
import time
import requests
import json
import sys
import os
import hashlib

o = ODPS('LTAI5tKTTQGMqqDfaav4hPeZ', 'bKINtxYsZRiTvFtp5jgOJVOsktFWKJ', 'TKHGZ_DataCenter',
         endpoint='http://service.odps.aliyun.com/api')

def car_list():
    group_id = "TGDC_guangzhou_gztgh"
    apiKey = "fe92nseq"
    apiSecret = "3e058bd22c58260ecd3ef1d172a82eec"
    floor="B3F"

    url = "https://park-api.aibee.cn/car/v1/car/all-cars"
    body = {"group_id":group_id, "floor":floor}

    timestamp = int(datetime.timestamp(datetime.now()))
    print("timestamp =", timestamp)

    signstr = json.dumps(body) + str(timestamp) + apiSecret
    apiSign = hashlib.sha1(signstr.encode('utf-8')).hexdigest()
    print("apiSign: ", apiSign)

    headers = {}
    headers["Content-type"] = "application/json"
    headers["Aibee-Auth-ApiKey"] = apiKey
    headers["Aibee-Auth-Sign"] = apiSign
    headers["Aibee-Auth-Timestamp"] = str(timestamp)
    headers["group_id"] = group_id

    resp = requests.post(url, data=json.dumps(body), headers=headers)

    #print("resp.code:", resp.status_code, ", resp.text:", resp.text)
    response_data=json.loads(resp.text)
    for i in response_data['data']:
        try:
            requestid=response_data['request_id']
            car_no=i['car_no']
            car_status=i['car_status']
            pos_x=i['pos_x']
            pos_y=i['pos_y']
            lot_no=i['lot_no']
            updatetime=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            pt=time.strftime('%Y%m', time.localtime(time.time()))
            sql_str="insert into table ods_tkh_carparking_allcars partition(pt) VALUES ""('1','1','1','1','1','1','1','" + str(updatetime)  + "','" + pt  + "')"
            instance = o.run_sql(sql_str)  #异步的方式执行。
            #print(instance.get_logview_address())  # 获取logview地址。
            instance.wait_for_success()  # 阻塞直到完成。
        except Exception as e:
            print(str(e))

if __name__=="__main__":
    print('geting data... ...')
    #模块接口详细设计,每天新增
    car_list()


