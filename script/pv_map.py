import sys
import os
import time
import json
import uuid
from concurrent.futures import ProcessPoolExecutor

utils_path = os.path.abspath(os.path.dirname(__file__)) + '/../utils'
sys.path.append(utils_path)
from log_utils import LogUtils

MAX_WORKERS = 24
SEPARATOR = ':'

# action = req日志的map
def do_map(hdfs_hosts, file_in, pv_file_out, logger):
    data = open(file_in, 'rb')
    pv_writer = open(pv_file_out, 'w')
    pv_dict = {}

    for line in data:
        try:
            # 1. pv日志字段预处理
            json_obj = json.loads(line.decode('utf-8'))
            if json_obj['filter_info']['is_filtered']:
                continue
            date_str = json_obj['ftime']
            timestamp = str(int(time.mktime(time.strptime(date_str[0: 13], "%Y-%m-%d %H"))))

            # pv_req
            pv_req = json_obj['pv_req']
            media_id = pv_req['appid']
            adspot_id = pv_req['adspotid']

            # 2. pv map输出
            adspot_key = SEPARATOR.join((
                timestamp, media_id, adspot_id
            ))
            if adspot_key not in pv_dict:
                pv_dict[adspot_key] = 0
            pv_dict[adspot_key] += 1

        except Exception as e:
            logger.info(f'pv_map.py引发异常: {e}, 行数据: {line}')
            continue

    for adspot_key, pv in pv_dict.items():
        pv_writer.write(f'{adspot_key}\t{pv}\n')

    data.close()
    pv_writer.close()


def init():
    if len(sys.argv) != 5:
        sys.exit(1)
    hdfs_hosts = sys.argv[1]
    path_in = sys.argv[2]
    pv_path_out = sys.argv[3]
    log_file = sys.argv[4]
    logger = LogUtils(log_file)
    logger.info('pv_map.py正在启动...')

    pool = ProcessPoolExecutor(max_workers=MAX_WORKERS)
    results = []

    files = os.listdir(path_in)

    for file in files:
        file_in = os.path.join(path_in, file)

        uid = str(uuid.uuid3(uuid.NAMESPACE_DNS, file))
        pv_file_out = os.path.join(pv_path_out, 'pv_map_' + uid)

        result = pool.submit(do_map, hdfs_hosts, file_in, pv_file_out, logger)
        results.append(result)
    pool.shutdown()


if __name__ == '__main__':
    init()
