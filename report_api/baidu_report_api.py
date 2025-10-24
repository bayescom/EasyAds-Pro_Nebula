import base64
import time
from datetime import datetime, timedelta

import requests
from Crypto.Hash import SHA1
from Crypto.PublicKey import DSA
from Crypto.Signature import DSS

import report_api_utils

# 百度api接入文档:
# https://baidu-ssp.gz.bcebos.com/mssp/api/%E7%99%BE%E5%BA%A6%E8%81%94%E7%9B%9F-%E6%95%B0%E6%8D%AE%E6%8A%A5%E5%91%8AAPI-%E6%8E%A5%E5%85%A5%E6%96%87%E6%A1%A3.pdf
# 需要在账户内配置access_key和DSA的private_key，见文档

host = 'https://ubapi.baidu.com'
uri = '/ssp/1/sspservice/app/app-report/get-app-report'

columns = ['adPositionId', 'request', 'view', 'click', 'income']
missing_column_indexes = [2]

def baidu_report(yesterday, access_key, private_key):
    yesterday = yesterday.replace('-', '')
    uri_date = uri + f'?beginDate={yesterday}&endDate={yesterday}'

    now = str(int(round(time.time())))
    # 1. 准备签名需要的数据
    itemsToBeSinged = []
    # 1.1 accessKey
    itemsToBeSinged.append(access_key)
    # 1.2 method
    itemsToBeSinged.append('GET')
    # 1.3 uri
    itemsToBeSinged.append(uri_date)
    # 1.4 时间
    itemsToBeSinged.append(now)
    itemsToBeSinged.append('')  # empty ContentType
    itemsToBeSinged.append('')  # empty content md5
    # 2.签名
    stringTobeSigned = '\n'.join(itemsToBeSinged)
    input_pri_key = DSA.import_key(extern_key=private_key)
    hash_obj = SHA1.new(stringTobeSigned.encode('utf8'))
    signer = DSS.new(key=input_pri_key, mode='fips-186-3', encoding='der')
    signatureBytes = signer.sign(hash_obj)
    signature = base64.b64encode(signatureBytes)
    # 发起请求
    headers = {'x-ub-date': now, 'x-ub-authorization': access_key + ':' + signature.decode('utf8')}
    result_json = requests.get(url=host + uri_date, headers=headers).json()
    if 0 == result_json["code"]:
        items = result_json["data"]
        table_data = report_api_utils.get_report_dataframe(items, columns, missing_column_indexes)
        return table_data, True
    else:
        return None, False
