#!/bin/bash

main() {
    # 准备运行参数
    init "$@"

    if [ -d "${temp_nebula_pv_log_path}" ] && [ "$(ls -A "${temp_nebula_pv_log_path}")" ]; then
        # 1. req数据处理
        (
          # 1.1 pv做map
          # [1]pv的map脚本 [2]hdfs服务器地址/字符串"file" [3]聚合SDK日志hdfs路径/本地聚合SDK req日志路径 [4]pv_map输出文件夹路径 [5]日志文件路径
          python "${script_path}/pv_map.py" "${hdfs_hosts}" "${temp_nebula_pv_log_path}" "${temp_pv_map_path}" "${log_file}"
          notify "pv_map.py"

          # 1.2.1 pv做map合并 + sort
          # [1]pv_map输入文件夹路径 [2]用于sort的临时路径 [3]每个pv_map文件sort后的文件夹路径 [4]pv_sort聚合结果文件
          do_sort "${temp_pv_map_path}" "${pv_sort_temp_path}" "${temp_pv_sort_path}" "${temp_pv_sort_file}"
          notify "pv sort"

          # 1.2.2 pv做reduce
          # [1]pv_sort聚合结果文件 [2]pv_reduce脚本 [3]pv_reduce结果文件
          < "${temp_pv_sort_file}" python "${script_path}/pv_reduce.py" > "${pv_result_file}" # "${log_file}" 逻辑很简单，不需要log
          notify "pv_reduce.py"
        ) &
    else
       warn "请求日志文件夹不存在或无请求日志文件，已跳过请求日志处理"
    fi

    # 2. 其他上报处理
    if [ -d "${temp_nebula_deal_log_path}" ] && [ "$(ls -A "${temp_nebula_deal_log_path}")" ]; then
        (
          # 2.1 deal做map
          # [1]deal_map脚本 [2]hdfs服务器地址/字符串"file" [3]聚合SDK日志hdfs路径/本地聚合SDK其他上报日志路径 [4]deal_map输出文件夹路径 [5]日志文件路径
          python "${script_path}/deal_map.py" "${hdfs_hosts}" "${temp_nebula_deal_log_path}" "${temp_deal_map_path}" "${log_file}"
          notify "deal_map.py"

          # 2.2.1 deal做map合并 + sort
          # [1]deal_map输入文件夹路径 [2]用于sort的临时路径 [3]每个deal_map文件sort后的文件夹路径 [4]deal_sort聚合结果文件
          do_sort "${temp_deal_map_path}" "${deal_sort_temp_path}" "${temp_deal_sort_path}" "${temp_deal_sort_file}"
          notify "deal sort"

          # 2.2.2 deal做reduce
          # [1]deal_sort聚合结果文件 [2]deal_reduce脚本 [3]deal_reduce结果文件
          < "${temp_deal_sort_file}" python "${script_path}/deal_reduce.py" > "${deal_result_file}"
          notify "deal_reduce.py"
        ) &
    else
       warn "上报日志文件夹不存在或无上报日志文件，已跳过上报日志处理"
    fi

    wait

    # 3. join and update，各部分结果合并成完整小时报表，更新到数据库
    # [1]把pv、deal的结果合并并更新到数据库的脚本 [2]时间类型参数，小时 [3]当前任务的日期字符串，YYYYmmdd_HH [4]pv_reduce结果文件 [5]deal_reduce结果文件 [6]合并后的小时结果文件 [7]日志文件路径
    python "${script_path}/join_and_update_db.py" "hour" "${report_datetime}" "${pv_result_file}" "${deal_result_file}" "${hour_result_file}" "${log_file}"
    notify "join_and_update_db.py"

    finish_msg="【Nebula】【Finish】【${report_datetime}报表】执行完毕,完成时间=[$(date '+%Y-%m-%d %H:%M')]"
    echo "${finish_msg}" >> "${log_file}"

    # 运行结束后删除临时中间文件
    rm -r "${temp_path}"
}

do_sort(){
  local parallel_limit=12 # 设置sort的时候最大并行数量为12
  local temp_map_path=$1
  local sort_temp_path=$2
  local temp_sort_path=$3
  local temp_sort_file=$4
  for one_map_file in "${temp_map_path}"/*
  do
    LC_ALL=C sort "${one_map_file}" -S 70% --parallel=16 -T "${sort_temp_path}" -o "${temp_sort_path}"/"$(basename "${one_map_file}")"_sort &
    if [[ $(jobs -p | wc -l) -ge ${parallel_limit} ]]; then
        wait -n
    fi
  done
  wait
  LC_ALL=C sort -m "${temp_sort_path}"/*_sort -S 70% --parallel=16 -T "${sort_temp_path}" -o "${temp_sort_file}"
}

notify(){
  # 如果上一个命令没有执行成功，告警
  if [ $? -ne 0 ]; then
    local fail_msg="【Nebula】【Error】【${report_datetime}报表】【$1】执行失败！请检查代码和日志"
    echo "${fail_msg}" >> "${log_file}"
    exit 1
  else
    local success_msg="$(date '+%Y-%m-%d %H:%M:%S,%3N') - INFO - 【${report_datetime}报表】 - 【$1】执行完成"
    echo "${success_msg}" >> "${log_file}"
  fi
}

warn(){
  local warn_msg="【Nebula】【Warn】【${report_datetime}报表】$1"
  echo "${warn_msg}" >> "${log_file}"
}

split_logs() {
    # 1. 参数赋值
    local stella_pv_log_file="$1"
    local stella_deal_log_file="$2"
    local temp_nebula_pv_log_path="$3"
    local temp_nebula_deal_log_path="$4"

    # 2. 参数校验
    if [ ! -f "$stella_pv_log_file" ] || [ ! -f "$stella_deal_log_file" ]; then
        echo "错误: 请检查给定的Stella日志文件是否存在"
        echo "当前给定请求日志路径: ${stella_pv_log_file}"
        echo "当前给定上报日志路径: ${stella_deal_log_file}"
        return 1
    fi

    process_single_file() {
        local input_file="$1"
        local output_dir="$2"
        local type_name="$3"

        echo "开始处理 $type_name 文件: $input_file"

        # 执行分割命令
        prefix=$(basename "${input_file%.*}")
        <"${input_file}" parallel --pipe -N 100000 'cat > "'"${output_dir}/${prefix}"'.data.$(uuidgen).log"'

        echo "$type_name 分割任务已提交后台。"
    }

    # 4. 执行处理
    process_single_file "$stella_pv_log_file" "$temp_nebula_pv_log_path" "请求日志"
    process_single_file "$stella_deal_log_file" "$temp_nebula_deal_log_path" "上报日志"

    # 5. 等待所有后台任务完成
    echo "等待所有日志分割任务完成..."
    wait
    echo "所有日志分割任务处理完成"
}

init(){
  report_datetime=$(date -d "-1 hour" +%Y%m%d_%H)

  while getopts 't:fh' OPTION; do
      case "$OPTION" in
          t)
            DATE_REGEX="^[0-9]{8}_[0-9]{2}$"  # 日期格式的正则表达式
            if [[ $OPTARG =~ $DATE_REGEX ]]; then
                report_datetime=$OPTARG
            else
                echo "无效的-t 选项, 请输入正确格式的时间，如20240101_00"
                exit 1
            fi
            ;;
          h)
            echo "默认以本地文件模式运行，-t 指定特定数据时间，如20240101_00"
            exit 1
            ;;
          ?)
            echo "Usage: $(basename $0) [-t] [-f] [-h]" >&2
            exit 1
            ;;
      esac
  done

  echo "正在以本地文件模式处理 ${report_datetime} 的数据......"
  report_date=$(echo "$report_datetime" | awk -F "_" '{print $1}')

  #/nebula
  root_path=$(cd ./..; pwd)

  #/nebula/log/hour
  log_path=${root_path}/log/hour
  log_file=${log_path}/${report_datetime}.log

  #/nebula/script
  script_path=${root_path}/script

  #/nebula/temp/YYYYmmdd_HH
  temp_path=${root_path}/temp/${report_datetime}
  temp_pv_map_path=${temp_path}/pv_map
  temp_pv_sort_path=${temp_path}/pv_sort
  temp_deal_map_path=${temp_path}/deal_map
  temp_deal_sort_path=${temp_path}/deal_sort
  temp_nebula_pv_log_path=${temp_path}/log/pv
  temp_nebula_deal_log_path=${temp_path}/log/deal

  # sort排序用的临时文件夹
  sort_temp_path=${temp_path}/sort
  pv_sort_temp_path=${sort_temp_path}/pv
  deal_sort_temp_path=${sort_temp_path}/deal

  # sort结果文件
  temp_pv_sort_file=${temp_pv_sort_path}/${report_datetime}.pv_sort
  temp_deal_sort_file=${temp_deal_sort_path}/${report_datetime}.deal_sort

  #/nebula/result/hour/YYYYmmdd_HH
  result_path=${root_path}/result/hour/${report_datetime}
  pv_result_file=${result_path}/${report_datetime}.pv
  deal_result_file=${result_path}/${report_datetime}.deal
  hour_result_file=${result_path}/${report_datetime}.result

  # 检查和创建临时文件夹、输出文件夹，递归创建
  # 第一次运行后有的临时文件夹
  mkdir -p "${log_path}"

  # 每次运行都要创建的临时文件夹
  mkdir -p "${temp_path}" \
           "${temp_pv_map_path}" \
           "${temp_pv_sort_path}" \
           "${temp_deal_map_path}" \
           "${temp_deal_sort_path}" \
           "${pv_sort_temp_path}" \
           "${deal_sort_temp_path}" \
           "${result_path}" \
           "${temp_nebula_pv_log_path}" \
           "${temp_nebula_deal_log_path}" \

  # TODO 这里需要配置你的Stella项目输出的日志路径
  stella_log_path="/home/work/stella/slog"

  stella_report_datetime=$(echo "$report_datetime" | sed 's/\(....\)\(..\)\(..\)_\(.*\)/\1-\2-\3_\4/')
  stella_pv_log_file="${stella_log_path}/req.${stella_report_datetime}.log"
  stella_deal_log_file="${stella_log_path}/track.${stella_report_datetime}.log"

  split_logs \
  "${stella_pv_log_file}" \
  "${stella_deal_log_file}" \
  "${temp_nebula_pv_log_path}" \
  "${temp_nebula_deal_log_path}"

  hdfs_hosts="file"
}

main "$@"; exit