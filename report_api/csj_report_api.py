# coding=utf-8
import json
import hashlib
from urllib.request import urlopen
import report_api_utils

# INDEX = [['app_id','应用id'], ['app_name', '应用名称'], ['ad_slot_id', '代码位id'], ['ad_slot_type', '代码位类型'],
#          ['request', '广告请求量'],  ['response', '广告返回量'], ['fill_rate', '填充率'], ['show', '展示量'], ['click', '点击量'],
#          ['click_rate', '点击率'], ['revenue', '收益'], ['ecpm', '预估ecpm'], ['ad_request', '物料请求量'], ['return', '物料返回量'],
#          ['ad_fill_rate', '物料填充率'], ['ad_impression_rate','物料展示率']]
#
# ADSLOT_TYPE_MAP = {1: "信息流", 2: "Banner", 3:"开屏", 4: "插屏", 5: "激励视频", 6: "全屏视频", 7 :"Draw信息流", 8 : "贴片", 9 : "新插屏广告"}

columns = ['ad_slot_id', 'request', 'response', 'show', 'click', 'revenue']
missing_column_indexes = []


class CSJMediaUtil:
    version = "2.0"
    sign_type_md5 = "MD5"
    KEY_USER_ID = "user_id"
    KEY_ROLE_ID = "role_id"
    KEY_VERSION = "version"
    KEY_SIGN = "sign"
    KEY_SIGN_TYPE = "sign_type"
    CSJ_HOST = "https://www.csjplatform.com"

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

            if self.user_id != "":
                params[self.KEY_USER_ID] = self.user_id

            if self.role_id != "":
                params[self.KEY_ROLE_ID] = self.role_id

            params[self.KEY_VERSION] = self.version
            params[self.KEY_SIGN_TYPE] = self.sign_type_md5

            param_orders = sorted(params.items(), key=lambda x: x[0], reverse=False)
            raw_str = ""
            for k, v in param_orders:
                raw_str += (str(k) + "=" + str(v) + "&")

            if len(raw_str) == 0:
                return ""
            sign_str = raw_str[0:-1] + self.secure_key
            # print "raw sign_str: ", sign_str

            sign = hashlib.md5(sign_str.encode()).hexdigest()
            result[self.KEY_SIGN] = sign
            result["url"] = raw_str + "sign=" + sign
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
        return self.CSJ_HOST + "/union_media/open_api/rt/income?" + result


# example
def csj_report(yesterday, user_id, role_id, secure_key):
    params = {
        "currency": "cny",
        "region": "cn",
        "date": yesterday,
    }
    CSJMediaUtil.user_id = user_id
    CSJMediaUtil.role_id = role_id
    CSJMediaUtil.secure_key = secure_key
    report_url = CSJMediaUtil.get_media_rt_income(params)

    try:
        yesterday_report = urlopen(report_url)

        report_json = json.loads(yesterday_report.read())
        if "100" == report_json['Code']:
            items = report_json['Data'][yesterday]
            table_data = report_api_utils.get_report_dataframe(items, columns, missing_column_indexes)
            return table_data, True
        else:
            return None, False

    except Exception as e:
        print(e)
