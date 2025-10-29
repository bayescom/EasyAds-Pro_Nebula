# coding=utf-8
import json
import hashlib
import time
from urllib.request import urlopen
import report_api_utils


# INDEX = [['app_id','媒体id'], ['app_name', '媒体名称'], ['position_id', '广告位id'], ['position_name', '广告位名称'],
#          ['ad_style', '广告位类型'],  ['impression', '广告展示'], ['click', '点击'], ['ctr', '点击率'],
#          ['share', '分成'], ['ecpm', '千次展现收益']]
# ADSTYLE_MAP = {1: "信息流", 2: "激励视频", 3:"全屏", 4: "开屏", 5: "Banner", 6: "Draw信息流", 13 :"插屏"}

columns = ['position_id', 'req_cnt', 'resp_cnt', 'impression', 'click', 'share']
missing_column_indexes = []


class KSMediaUtil:
    KEY_AK = "ak"
    KEY_SK = "sk"
    KEY_SIGN = "sign"
    KS_HOST = "https://ssp.e.kuaishou.com"
    KS_HOST_PATH = "/api/report/dailyShare?"

    @classmethod
    def sign_gen(self, params):
        result = {
            "sign": "",
            "url": "",
        }

        try:
            if not isinstance(params, dict):
                print("invalid params: ", params)
                return result

            param_orders = sorted(params.items(), key=lambda x: x[0], reverse=False)
            sign_param_str = ""
            req_param_str = ""
            for k, v in param_orders:
                each_param = str(k) + "=" + str(v) + "&"
                sign_param_str += each_param
                if k != "sk":
                    req_param_str += each_param

            if len(sign_param_str) == 0:
                return ""
            sign_str = self.KS_HOST_PATH + sign_param_str[0:-1]

            sign = hashlib.md5(sign_str.encode()).hexdigest().lower()
            result[self.KEY_SIGN] = sign
            result["url"] = req_param_str + "sign=" + sign
            return result
        except Exception as err:
            print("invalid Exception", err)
        return result

    @classmethod
    def get_signed_url(self, params):
        return self.sign_gen(params).get("url", "")

    @classmethod
    def get_media_rt_income(self, params):
        result = self.get_signed_url(params)
        if result == "":
            return ""
        return self.KS_HOST + self.KS_HOST_PATH + result


def get_current_timestamp():
    return int(time.mktime(time.localtime(time.time())))


# example
def ks_report(yesterday, access_key, security_key):
    params = {
        "date": yesterday,
        "timestamp": get_current_timestamp(),
        "ak": access_key,
        "sk": security_key,
        "page": 1
    }
    report_url = KSMediaUtil.get_media_rt_income(params)
    try:
        yesterday_report = urlopen(report_url)
        report_json = json.loads(yesterday_report.read())

        if 1 == report_json['result']:
            items = report_json['data']
            table_data = report_api_utils.get_report_dataframe(items, columns, missing_column_indexes)

            return table_data, True
        else:
            return None, False
    except Exception as e:
        print(e)