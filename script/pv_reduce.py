import sys
from itertools import groupby
from operator import itemgetter

SEPARATOR = ':'


def read_input(file):
    for line in file:
        yield line.rstrip().split('\t')


def do_reduce():
    data = read_input(sys.stdin)

    # [adspot_key]: timestamp, media_id, adspot_id
    # [pv_value]: pv

    # 示例: 键A, [(键A, 值1), (键A, 值2), (键A, 值3)...]
    for sdk_adspot_key, key_value_pair_list in groupby(data, itemgetter(0)):
        total_pv = 0
        for key_value_pair in key_value_pair_list:
            pv = int(key_value_pair[1])
            total_pv += pv
        print(sdk_adspot_key + SEPARATOR + str(total_pv))
    data.close()


if __name__ == '__main__':
    do_reduce()
