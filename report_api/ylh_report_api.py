# coding=utf-8
import json
import hashlib
import base64
import time
from urllib.request import urlopen
from urllib.request import Request
import pandas as pd
import report_api_utils

# INDEX = [['app_id','媒体id'], ['medium_name', '媒体名称'],
#          ['placement_id', '广告位id'], ['placement_name', '广告位名称'], ['placement_type', '广告位类型'],
#          ['request_count', '广告位请求量'], ['return_count', '广告位返回量'],
#          ['ad_request_count', '广告请求量'], ['ad_return_count',	'广告返回量'],
#          ['pv', '曝光量'], ['click',	'点击量'], ['fill_rate', '广告位填充率'],
#          ['exposure_rate', '广告位曝光率'], ['ad_fill_rate', '广告填充率'],
#          ['ad_exposure_rate', '广告曝光率'], ['click_rate', '点击率'], ['revenue', '收入(元)'],
#          ['ecpm', '千次展示收入(元)'], ['cpc', '点击成本(元)']]

columns = ['placement_id', 'request_count', 'return_count', 'pv', 'click', 'revenue']
missing_column_indexes = []


class YlhMediaUtil:
    YLH_URL = "https://api.adnet.qq.com/open/v1.1/report/get?"

    @classmethod
    def get_token(self, params):
        memberid = params['member_id']
        secret = params['secret']
        timestamp = get_current_timestamp()
        sign = hashlib.sha1((memberid + secret + str(timestamp)).encode('utf-8')).hexdigest()
        list_v = [memberid, str(timestamp), sign]
        plain = ','.join(list_v)
        return base64.b64encode(plain.encode())

    @classmethod
    def get_url(self, params):
        param_list = []
        for k, v in params.items():
            if "secret" != k:
                param_list.append("%s=%s" % (k, v))
        return self.YLH_URL + "&".join(param_list)

    @classmethod
    def get_media_rt_income(self, params):
        result = self.get_url(params)
        token = self.get_token(params)

        return result, token


def get_current_timestamp():
    return int(time.mktime(time.localtime(time.time())))


# example
def ylh_report(yesterday, member_id, secret):
    yesterday = yesterday.replace('-', '')
    params = {
        "member_id": member_id,
        "secret": secret,
        "start_date": yesterday,
        "end_date": yesterday
    }

    report_url, token = YlhMediaUtil.get_media_rt_income(params)

    try:
        request = Request(url=report_url, headers={"token": token})
        yesterday_report = urlopen(request)

        report_json = json.loads(yesterday_report.read())

        if 0 == report_json['code']:
            report_result = []
            for item in report_json['data']['list']:
                if 0 == item['placement_id']:
                    continue
                one_result = []
                for col in columns:
                    one_result.append(item[col])
                report_result.append(one_result)

            table_data = report_api_utils.convert_result_to_dataframe(report_result, missing_column_indexes)
            return table_data, True
        else:
            return None, False

    except Exception as e:
        print(e)
