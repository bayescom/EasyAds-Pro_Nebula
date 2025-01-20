import sys
from itertools import groupby
from operator import itemgetter

SEPARATOR = ':'


def read_input(file):
    for line in file:
        yield line.rstrip().split('\t')


def do_reduce():
    data = read_input(sys.stdin)

    # [sdk_adspot_key]: timestamp, media_id, adspot_id, channel_id, sdk_adspot_id
    # [deal_value]: action, unique_action_key, income

    # 示例: 键A, [(键A, 值1), (键A, 值2), (键A, 值3)...]
    for sdk_adspot_key, key_value_pair_list in groupby(data, itemgetter(0)):
        try:
            # 取出同一个键的所有值，放进列表里reduce
            # deal_value_list = [key_value_pair[1].split(SEPARATOR) for key_value_pair in key_value_pair_list]

            total_req = 0
            total_bid = 0
            total_show = 0
            total_click = 0
            total_income = 0
            unique_action_key_dict = {}
            for key_value_pair in key_value_pair_list:
                deal_value = key_value_pair[1].split(SEPARATOR)

                # 如果上报的唯一id重复了，跳过
                unique_action_key = deal_value[1]  # unique_action_key，唯一上报id
                if unique_action_key not in unique_action_key_dict:
                    unique_action_key_dict[unique_action_key] = 1

                    action = deal_value[0]
                    if action == 'loaded':
                        total_req += 1
                    elif action == 'succeed':
                        total_bid += 1
                    elif action == 'win':
                        total_show += 1
                        total_income += float(deal_value[2])  # 只在曝光的时候统计收入
                    elif action == 'click':
                        total_click += 1
            value_str = SEPARATOR.join((
                str(total_req),
                str(total_bid),
                str(total_show),
                str(total_click),
                str(total_income / 100000)  # 分每千次展示转成元
            ))
            # 每一行输出全用分隔符连接
            print(sdk_adspot_key + SEPARATOR + value_str)
        except:
            continue
    data.close()


if __name__ == '__main__':
    do_reduce()
