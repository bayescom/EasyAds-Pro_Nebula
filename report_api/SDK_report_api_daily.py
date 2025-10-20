# 调用所有SDK的 Report API 获取每日数据任务

import os
import sys
import time
from datetime import timedelta, datetime

import bayes_report_api
import csj_report_api
import ks_report_api
import ylh_report_api
import baidu_report_api

utils_path = os.path.abspath(os.path.dirname(__file__)) + '/../utils'
sys.path.append(utils_path)
from db_utils import DbUtils

if len(sys.argv) > 1:
    yesterday = sys.argv[1]
else:
    yesterday = (datetime.today() + timedelta(-1)).strftime("%Y-%m-%d")
timestamp = int(datetime.strptime(yesterday, "%Y-%m-%d").timestamp())

MAX_RETRY_TIMES = 5
RETRY_INTERVAL = 60 * 20

sdk_name_map = {
    1: '倍业',
    2: '优量汇',
    3: '穿山甲',
    4: '百度',
    5: '快手'
}

# sdk_id和report方法映射
def get_one_report(sdk_setting):
    sdk_id = sdk_setting['adn_id']
    params = sdk_setting['params']
    # 这里必须声明为lambda函数，这样才不会在生成字典的时候直接调用
    report_dict = {
        # 1 倍业
        1: lambda: bayes_report_api.bayes_report(yesterday, params['secret_key']),
        # 2 优量汇
        2: lambda: ylh_report_api.ylh_report(yesterday, params['member_id'], params['secret']),
        # 3 穿山甲
        3: lambda: csj_report_api.csj_report(yesterday, params['user_id'], params['role_id'], params['security_key']),
        # 4 百度
        4: lambda: baidu_report_api.baidu_report(yesterday, params['access_key'], params['private_key']),
        # 5 快手
        5: lambda: ks_report_api.ks_report(yesterday, params['access_key'], params['security_key'])
    }
    return report_dict.get(sdk_id, None)()


def get_meta_report_map(sdk_setting_list):
    meta_report_map = {}
    retry_sdk_setting_list = []

    for sdk_setting in sdk_setting_list:
        try:
            report, is_success = get_one_report(sdk_setting)
            # 不成功和返回空数据都重试
            if not is_success:
                retry_sdk_setting_list.append(sdk_setting)
                continue
            if report is None:
                retry_sdk_setting_list.append(sdk_setting)
                # 如果最后一次重试拿到的仍然是成功状态码+数据为空，记录下来
                if retry_times == (MAX_RETRY_TIMES - 1):
                    empty_report_list.append(sdk_setting)
                continue

            for row in report.itertuples(index=False):
                # 去重key: adn_id(channel_id) + sdk_adspot_id
                key = (str(sdk_setting['adn_id']), str(row[0]))
                meta_report_map[key] = row[1:]
        except Exception as e:
            # 异常处理
            print(f'获取report时出现异常: {e}')
            continue
    return meta_report_map, retry_sdk_setting_list


def get_update_list(meta_report_map, record_id_map):
    update_list = []
    for key in record_id_map.keys():
        # (请求数, 返回数, 展现数, 点击数, 唤起数, 收入)
        report_data = meta_report_map.get(key)
        if report_data is None:
            continue
        # [(记录id1, 请求数1, 返回数1, 展现数1, 点击数1, 唤起数1, 收入1), (记录id2, 请求数2, 返回数2, 展现数2, 点击数2, 唤起数2, 收入2)...]
        record_id_list = record_id_map[key]
        # 如果渠道广告位对应多个倍业广告位，各个指标要分别按比例划分
        if len(record_id_list) == 1:
            update_list.append(report_data + (record_id_list[0][0],))
        elif len(record_id_list) > 1:
            total_req = 0
            total_bid = 0
            total_show = 0
            total_click = 0
            total_income = 0
            for record_id in record_id_list:
                total_req += record_id[1]
                total_bid += record_id[2]
                total_show += record_id[3]
                total_click += record_id[4]
                total_income += record_id[5]
            total_req = 1 if total_req == 0 else total_req
            total_bid = 1 if total_bid == 0 else total_bid
            total_show = 1 if total_show == 0 else total_show
            total_click = 1 if total_click == 0 else total_click
            total_income = 1 if total_income == 0 else total_income
            for record_id in record_id_list:
                id = record_id[0]
                req_percent = record_id[1] * 1.0 / total_req
                bid_percent = record_id[2] * 1.0 / total_bid
                show_percent = record_id[3] * 1.0 / total_show
                click_percent = record_id[4] * 1.0 / total_click
                income_percent = record_id[5] * 1.0 / total_income

                values = [round(report_data[0] * req_percent),
                          round(report_data[1] * bid_percent),
                          round(report_data[2] * show_percent),
                          round(report_data[3] * click_percent),
                          round(report_data[4] * income_percent, 2)]
                update_list.append(tuple(values) + (id,))

    return update_list


# 用来生成查询失败/信息为空时账号的信息，用来发送给钉钉
def get_account_msg_list(sdk_setting_list):
    return [f'{sdk_name_map.get(sdk_setting.get("adn_id", "-"), "-")}-{sdk_setting.get("account_name", "-")}' for sdk_setting in sdk_setting_list]


def do_update_report_api(sdk_setting_list):
    # (渠道id, 渠道广告位id) -> (请求数, 返回数, 展现数, 点击数, 收入)
    meta_report_map, retry_sdk_setting_list = get_meta_report_map(sdk_setting_list)

    # 如果没查出结果，不做广告源映射，直接返回失败列表重试
    if not meta_report_map:
        return retry_sdk_setting_list

    # 有可能同一个广告源配在了多个自己的广告位上，对应多条记录
    # (渠道id, 渠道广告位id) -> 记录id
    sdk_adspot_id_list = [x[1] for x in meta_report_map.keys()]
    if len(sdk_adspot_id_list) == 0:
        record_id_map = {}
    else:
        record_id_map = DbUtils().get_record_id_map(timestamp, sdk_adspot_id_list)

    # 记录id -> (请求数, 返回数, 展现数, 点击数, 收入)
    update_list = get_update_list(meta_report_map, record_id_map)

    # 更新到数据库
    # (请求数, 返回数, 展现数, 点击数, 收入, 记录id)
    DbUtils().update_report_api(update_list)

    # 返回没有获取到report的列表，用于重试
    return retry_sdk_setting_list


# ------从这里开始执行------
# 第一次尝试，从数据库查询用户配置的账号列表，用来从各个平台查询report数据，此时retry_sdk_setting_list是所有要更新的账号数据
sdk_setting_list = DbUtils().get_sdk_report_api_params()
empty_report_list = []
retry_times = 0

sdk_setting_list = do_update_report_api(sdk_setting_list)
# 如果重试列表还不为空 & 小于最大重试次数，执行更新report api流程
while sdk_setting_list and retry_times < MAX_RETRY_TIMES:
    time.sleep(RETRY_INTERVAL)
    sdk_setting_list = do_update_report_api(sdk_setting_list)
    retry_times += 1

if sdk_setting_list:
    failed_account_msg_list = get_account_msg_list(sdk_setting_list)
    msg = f'【Nebula】【Report API】【Warn】 {yesterday}的SDK渠道report api任务部分执行失败，失败账号: \n' \
          f'{", ".join(failed_account_msg_list)}'
else:
    msg = f'【Nebula】【Report API】【Success】 ' \
          f'{yesterday}的SDK渠道report api任务执行完毕，重试次数: {retry_times}次'

# 同时报告请求成功但数据为空的结果
if empty_report_list:
    empty_account_msg_list = get_account_msg_list(empty_report_list)
    msg += f'\n响应码为成功但返回空数据的账号: \n{", ".join(empty_account_msg_list)}'

print(msg)
