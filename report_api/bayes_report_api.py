import json

import requests
import report_api_utils

# 倍业report api文档：
# https://www.bayescom.com/docsify/docs/#/report_api/

columns = ['adspotId', 'pv', 'bid', 'imp', 'click', 'income']
missing_column_indexes = []

TOKEN_URL = 'http://galileo.bayescom.com/Galileo/report_api/auth_token'
REPORT_URL = 'http://galileo.bayescom.com/Galileo/report_api/all_data'

def get_token(report_api_key):
    params = {
        "secretKey": report_api_key
    }
    try:
        response = requests.get(url=TOKEN_URL, params=params)
        response.raise_for_status()
        result = response.json()
        token = result.get("token")
        return token
    except Exception as e:
        raise Exception("倍业Report API token获取失败，请检查Report API密钥是否正确")

def bayes_report(yesterday, report_api_key):
    token = get_token(report_api_key)
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "token": token
    }
    body = {
        "startDate": yesterday,
        "endDate": yesterday
    }
    response = requests.post(
        url=REPORT_URL,
        headers=headers,
        data=json.dumps(body),
        timeout=30  # 设置超时时间
    )

    response.raise_for_status()
    result_json = response.json()

    if 0 == result_json["code"]:
        items = result_json["data"]
        table_data = report_api_utils.get_report_dataframe(items, columns, missing_column_indexes)
        return table_data, True
    else:
        return None, False
