import glob
import os
import sys
import time
import re
import pandas as pd

utils_path = os.path.abspath(os.path.dirname(__file__)) + '/../utils'
sys.path.append(utils_path)
from db_utils import DbUtils
from log_utils import LogUtils

SEPARATOR = ':'
# 和数据库的列名一致
adspot_key = ['timestamp', 'media_id', 'adspot_id']
sdk_adspot_key = ['timestamp', 'media_id', 'adspot_id', 'channel_id', 'sdk_adspot_id']
exp_key = ['timestamp', 'media_id', 'adspot_id', 'channel_id', 'sdk_adspot_id', 'exp_type', 'exp_id', 'group_id']

pv_value = ['pvs']
deal_value = ['reqs', 'bids', 'wins', 'shows', 'clicks', 'income']

# 输出到两个表：广告源维度的报表和ab测试分组维度的报表
all_cols = exp_key + pv_value + deal_value
report_cols = sdk_adspot_key + pv_value + deal_value
exp_report_cols = exp_key + deal_value
report_hourly_table_name = 'report_hourly'
exp_report_hourly_table_name = 'exp_report_hourly'


# 1. 如果是小时报表任务，需要把三个reduce结果文件按照广告源维度left join起来得到结果
def do_join(pv_file_in, deal_file_in, file_out, report_timestamp):
    # NOTE 注意: 所有的列读进来都是string格式, 后续要做比较判断和计算要注意类型转换
    # 如果缺少了请求日志的结果文件或者上报日志的结果文件，用空的DataFrame替代，正常插入另一部分文件
    try:
        pv_df = pd.read_csv(pv_file_in, sep=SEPARATOR, header=None, dtype=str, names=(adspot_key + pv_value))
    except:
        pv_df = pd.DataFrame(columns=adspot_key + pv_value)
    try:
        deal_df = pd.read_csv(deal_file_in, sep=SEPARATOR, header=None, dtype=str, names=(exp_key + deal_value))
    except:
        deal_df = pd.DataFrame(columns=exp_key + deal_value)

    join_df = pd.merge(pv_df, deal_df, on=adspot_key, how='outer')

    # 把时间戳不是当前小时的记录筛掉
    join_df = join_df[join_df['timestamp'] == report_timestamp]

    # 填充没有join上的空值, 除了特殊列其他列填0
    fillna_dict = {
        'channel_id': '-1',
        'sdk_adspot_id': '-',
        'exp_type': '-1',
        'exp_id': '-1',
        'group_id': '-1'
    }
    join_df = join_df.fillna(value=fillna_dict).fillna('0')

    # 把income转换成浮点型，保留3位小数四舍五入
    join_df['income'] = join_df['income'].astype(float).round(3)
    int_columns = ['pvs', 'reqs', 'wins', 'bids', 'shows', 'clicks']
    join_df[int_columns] = join_df[int_columns].astype(int)

    # 写出到结果文件夹，用作备份
    join_df = join_df[all_cols]
    join_df.to_csv(file_out, sep=SEPARATOR, index=False, header=False, encoding='utf-8-sig')

    # 拆成广告源和ab测试报表，广告源报表要按照广告源维度聚合，ab测试报表只要type不等于-1的记录，也不要pv
    report_df = join_df[report_cols].groupby(sdk_adspot_key).sum().reset_index()
    exp_report_df = join_df[exp_report_cols]
    exp_report_df = exp_report_df[exp_report_df['exp_type'] != '-1']

    return report_df, exp_report_df


# 2. 报表插入数据库，并检查插入是否成功
def insert_hourly_report(table_name, df, logger):
    update_count = 0
    try:
        if table_name == report_hourly_table_name:
            update_count = DbUtils().insert_hourly_report(df)
        elif table_name == exp_report_hourly_table_name:
            update_count = DbUtils().insert_hourly_exp_report(df)

        if update_count == 0:
            logger.warn(f'{table_name}小时报表数据库更新异常, 更新条数为0')
        elif update_count < len(df):
            logger.warn(
                f'{table_name}小时报表数据库更新异常, 没有全部写入, 更新数据{len(df)}条, 写入数据库{update_count}条')
    except Exception as e:
        logger.error(f'{table_name}小时报表数据库操作异常: {e}')
    logger.info(f'{table_name}小时报表数据库更新已完成, 共{update_count}条记录')


def init():
    if len(sys.argv) != 7 and len(sys.argv) != 4:
        print('请检查传入join_and_update_db.py的参数个数')
        sys.exit(1)

    time_type = sys.argv[1]
    report_datetime = sys.argv[2]

    if time_type == 'hour':
        time_format = '%Y%m%d_%H'

        pv_file_in = sys.argv[3]
        deal_file_in = sys.argv[4]
        file_out = sys.argv[5]
        log_file = sys.argv[6]
        logger = LogUtils(log_file)
        logger.info('小时报表结果连接和数据库更新正在启动...')

        report_timestamp = str(int(time.mktime(time.strptime(report_datetime, time_format))))
        # 1. 小时报表任务，把reduce结果文件按照广告源维度left join起来得到结果
        report_df, exp_report_df = do_join(pv_file_in, deal_file_in, file_out, report_timestamp)
        # 2. 插入数据库并检查是否成功
        insert_hourly_report(report_hourly_table_name, report_df, logger)
        insert_hourly_report(exp_report_hourly_table_name, exp_report_df, logger)

    elif time_type == 'day':
        time_format = '%Y%m%d'
        log_file = sys.argv[3]
        logger = LogUtils(log_file)
        logger.info('天报表数据库更新正在启动...')

        report_timestamp = int(time.mktime(time.strptime(report_datetime, time_format)))
        # 从数据库中查小时报表插入到天报表
        n = DbUtils().insert_daily_report_from_hourly(report_timestamp)
        if n == 0:
            logger.warn(f'天报表数据库更新异常, 更新条数为0')
        n = DbUtils().insert_daily_exp_report_from_hourly(report_timestamp)
        if n == 0:
            logger.warn(f'AB测试天报表数据库更新异常, 更新条数为0')
    else:
        sys.exit(1)


if __name__ == '__main__':
    init()
