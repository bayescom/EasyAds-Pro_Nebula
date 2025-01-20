#!/bin/bash

# 日期格式是YYYY-mm-dd，大部分的report api要求的格式
report_date=$(date -d "-1 day" +%Y-%m-%d)
if [[ $# == 1 ]]; then
    report_date=$1
fi

#/nebula
root_path=$(cd ./..; pwd)

# 调用所有SDK的report api更新任务
python "${root_path}/report_api/SDK_report_api_daily.py" "${report_date}" &