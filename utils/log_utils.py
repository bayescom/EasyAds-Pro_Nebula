import logging
import os


class LogUtils:
    def __init__(self, log_file):
        self.report_timestamp = os.path.splitext(os.path.basename(log_file))[0]

        logging.basicConfig(
            filename=log_file,
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
        # % (asctime)s：日志记录的时间
        # % (levelname)s：日志级别（如DEBUG、INFO、WARNING、ERROR、CRITICAL）
        # % (message)s：日志消息内容
        # % (name)s：记录器的名称
        # % (filename)s：调用日志记录器的源文件名
        # % (lineno)d：调用日志记录器的行号
        # % (funcName)s：调用日志记录器的函数名称
        # % (thread)d：线程ID
        # % (process)d：进程ID
        # % (module)s：模块名称（不带路径）
        self.logger = logging.getLogger()

    def info(self, message):
        self.logger.info(message, stacklevel=2)  # stacklevel=2 用于获取调用者的信息

    def warn(self, message):
        self.logger.warning(message, stacklevel=2)

    def error(self, message):
        self.logger.error(message, stacklevel=2)


# 示例用法
if __name__ == "__main__":
    log_util = LogUtils("20241127_00.log")
    log_util.info("This is an info message.")
    log_util.warn("This is a warning message.")
    log_util.error("这是一条测试告警信息")
