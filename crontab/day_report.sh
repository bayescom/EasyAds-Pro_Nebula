#!/bin/bash

main() {
  # 准备运行参数
  init "$@"

  # 查询数据库小时表，更新到数据库天表
  # [1]脚本 [2]时间类型参数，天 [3]当前任务的日期字符串，YYYYmmdd [4]日志文件路径
  python "${script_path}/join_and_update_db.py" "day" "${report_date}" "${log_file}"
  notify "join_and_update_db.py"

  finish_msg="【Nebula】【Finish】【${report_date}报表】执行完毕,完成时间=[$(date '+%Y-%m-%d %H:%M')]"
  echo "${finish_msg}" >> "${log_file}"
}

notify(){
  # 如果上一个命令没有执行成功，告警
  if [ $? -ne 0 ]; then
    local fail_msg="【Nebula】【Error】【${report_date}报表】【$1】执行失败！请检查代码和日志"
    echo "${fail_msg}" >> "${log_file}"
    exit 1
  else
    local success_msg="$(date '+%Y-%m-%d %H:%M:%S') - Info - 【${report_date}报表】 - 【$1】执行完成"
    echo "${success_msg}" >> "${log_file}"
  fi
}

init() {
  report_date=$(date -d "-1 day" +%Y%m%d)

  while getopts 't:h' OPTION; do
      case "$OPTION" in
          t)
            DATE_REGEX="^[0-9]{8}$"  # 日期格式的正则表达式
            if [[ $OPTARG =~ $DATE_REGEX ]]; then
                report_date=$OPTARG
            else
                echo "无效的-t 选项, 请输入正确格式的日期，如20240101"
                exit 1
            fi
            ;;
          h)
            echo "默认数据日期为前一天。-t 指定特定数据日期，如20240101"
            exit 1
            ;;
          ?)
            echo "Usage: $(basename $0) [-t] [-h]" >&2
            exit 1
            ;;
      esac
  done

  echo "正在处理 ${report_date} 的数据......"

  #/nebula
  root_path=$(cd ./..; pwd)

  #/nebula/log/day
  log_path=${root_path}/log/day
  log_file=${log_path}/${report_date}.log

  #/nebula/script
  script_path=${root_path}/script

  # 第一次运行需要创建的文件夹
  mkdir -p "${log_path}"
}

main "$@"; exit