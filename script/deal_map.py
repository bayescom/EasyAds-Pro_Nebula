import sys
import hashlib
import json
import os
import time
import uuid
from concurrent.futures import ProcessPoolExecutor

utils_path = os.path.abspath(os.path.dirname(__file__)) + '/../utils'
sys.path.append(utils_path)
from log_utils import LogUtils

MAX_WORKERS = 24
SEPARATOR = ':'


def do_map(hdfs_hosts, file_in, deal_file_out, logger):
    data = open(file_in, 'rb')
    deal_writer = open(deal_file_out, 'w')

    for line in data:
        try:
            # TODO 增加从hdfs读取
            # 1. 日志字段预处理
            json_obj = json.loads(line.decode('utf-8'))
            action = json_obj['action']
            reqid = json_obj['reqid']
            priority = json_obj['priority']
            date_str = json_obj['ftime']
            timestamp = str(int(time.mktime(time.strptime(date_str[0: 13], "%Y-%m-%d %H"))))
            media_id = json_obj['appid']
            adspot_id = json_obj['adspotid']
            channel_id = json_obj['supplierid']
            sdk_adspot_id = json_obj['sdk_adspotid']

            # 竞价位置是bidResult，固价是sdk_price
            if 'bidResult' in json_obj:
                income = json_obj['bidResult']
            elif 'sdk_price' in json_obj:
                income = json_obj['sdk_price']
            else:
                income = '0'

            # 用来去重上报的key，聚合sdk单次请求只有一个reqid/auction_id，但是有多个优先级+渠道+广告源+action
            m = hashlib.md5()
            m.update((reqid + channel_id + sdk_adspot_id + priority + action).encode('latin1'))
            unique_action_key = m.hexdigest()

            sdk_adspot_key = SEPARATOR.join((
                timestamp, media_id, adspot_id, channel_id, sdk_adspot_id
            ))
            deal_value = SEPARATOR.join((
                action, unique_action_key, income
            ))
            deal_writer.write(f'{sdk_adspot_key}\t{deal_value}\n')

        except Exception as e:
            logger.info(f'deal_map.py引发异常: {e}, 行数据: {line}')
            continue
    data.close()
    deal_writer.close()


def init():
    if len(sys.argv) != 5:
        sys.exit(1)
    hdfs_hosts = sys.argv[1]
    path_in = sys.argv[2]
    deal_path_out = sys.argv[3]
    log_file = sys.argv[4]
    logger = LogUtils(log_file)
    logger.info('deal_map.py正在启动...')

    pool = ProcessPoolExecutor(max_workers=MAX_WORKERS)
    results = []

    files = os.listdir(path_in)

    for file in files:
        file_in = os.path.join(path_in, file)

        uid = str(uuid.uuid3(uuid.NAMESPACE_DNS, file))
        deal_file_out = os.path.join(deal_path_out, "deal_map_" + uid)

        result = pool.submit(do_map, hdfs_hosts, file_in, deal_file_out, logger)
        results.append(result)
    pool.shutdown()


if __name__ == '__main__':
    init()
