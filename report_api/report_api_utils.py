import pandas as pd

# report api的统一dataframe列名
# rename_columns = ['sdk_adspot_id', 'report_api_req', 'report_api_bid', 'report_api_imp', 'report_api_click',
#                   'report_api_income']
rename_columns_dict = {'sdk_adspot_id': 'object',
                       'report_api_req': 'int',
                       'report_api_bid': 'int',
                       'report_api_imp': 'int',
                       'report_api_click': 'int',
                       'report_api_income': 'float'}


# items: 结果的json数组
# columns: 不同平台api返回字段的名称，对应位置缺失的把索引写在missing_column_indexes
# len(columns) + len(missing_column_indexes) == 6

# report_result: 查询结果的二维数组
# missing_column_indexes: 缺失列的索引，索引从0-6分别是:
# 0 渠道广告位id
# 1 请求数
# 2 返回数
# 3 展现数
# 4 点击数
# 5 收入
# 以快手的api为例，请求返回的数据缺少请求数、返回数字段，所以快手的report api的missing_column_indexes = [1, 2]

def get_report_dataframe(items, columns, missing_column_indexes):
    report_result = []
    for item in items:
        one_result = []
        for col in columns:
            one_result.append(item[col])
        report_result.append(one_result)
    return convert_result_to_dataframe(report_result, missing_column_indexes)


def convert_result_to_dataframe(report_result, missing_column_indexes):
    if not report_result:
        return None
    table_data = pd.DataFrame(data=report_result)
    for idx in missing_column_indexes:
        table_data.insert(idx, f'x{idx}', 0)
    table_data.columns = rename_columns_dict.keys()
    table_data = table_data.astype(rename_columns_dict)
    return table_data
