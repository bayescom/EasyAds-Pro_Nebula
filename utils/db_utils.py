import configparser
import json
import pathlib

import pymysql


class DbUtils:
    root_dir = pathlib.Path(__file__).parent.parent
    _cf = configparser.RawConfigParser()
    _cf.read(root_dir / 'db.config')

    def get_mysql_conn(self):
        selection = 'mysql'
        return self.__get_mysql_conn(selection)

    def get_mysql_dict_conn(self):
        selection = 'mysql'
        return self.__get_mysql_dict_conn(selection)

    def __get_mysql_conn(self, selection):
        db_host = self._cf.get(selection, 'host')
        db_port = self._cf.getint(selection, 'port')
        db_user = self._cf.get(selection, 'user')
        db_password = self._cf.get(selection, 'passwd')
        db_name = self._cf.get(selection, 'db')
        return pymysql.connect(host=db_host, port=db_port, user=db_user, passwd=db_password, db=db_name, charset='utf8')

    def __get_mysql_dict_conn(self, selection):
        db_host = self._cf.get(selection, 'host')
        db_port = self._cf.getint(selection, 'port')
        db_user = self._cf.get(selection, 'user')
        db_password = self._cf.get(selection, 'passwd')
        db_name = self._cf.get(selection, 'db')
        db_cursorclass = pymysql.cursors.DictCursor
        return pymysql.connect(host=db_host, port=db_port, user=db_user, passwd=db_password, db=db_name, charset='utf8',
                               cursorclass=db_cursorclass)

    # 用来查询的通用方法，返回查询结果给各个方法自己处理，省去管理资源的步骤
    @staticmethod
    def __select_template(db, sql):
        cursor = db.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        db.close()
        return result

    # 小时报表更新
    def insert_hourly_report(self, df):
        update_list = df.values.tolist()

        db = self.get_mysql_conn()
        cursor = db.cursor()
        sql = f"""
        INSERT INTO report_hourly
        (timestamp, media_id, adspot_id, channel_id, sdk_adspot_id, pvs, reqs, bids, wins, shows, clicks, income)   
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE pvs=values(pvs), reqs=values(reqs), bids=values(bids), wins=values(wins), shows=values(shows), 
        clicks=values(clicks), income=values(income)
        """
        n = cursor.executemany(sql, update_list)
        db.commit()
        cursor.close()
        db.close()
        return n

    # 小时AB测试报表更新
    def insert_hourly_exp_report(self, df):
        update_list = df.values.tolist()

        db = self.get_mysql_conn()
        cursor = db.cursor()
        sql = f"""
        INSERT INTO exp_report_hourly
        (timestamp, media_id, adspot_id, channel_id, sdk_adspot_id, exp_type, exp_id, group_id,
         reqs, bids, wins, shows, clicks, income)   
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE reqs=values(reqs), bids=values(bids), wins=values(wins), shows=values(shows), 
        clicks=values(clicks), income=values(income)
        """
        n = cursor.executemany(sql, update_list)
        db.commit()
        cursor.close()
        db.close()
        return n

    # 根据小时报表数据更新天报表数据
    def insert_daily_report_from_hourly(self, begin_time):
        # 天结束时间戳
        end_time = begin_time + 86400 - 1

        db = self.get_mysql_conn()
        cursor = db.cursor()
        sql = f"""
        INSERT INTO report_daily 
        (timestamp, media_id, adspot_id, channel_id, sdk_adspot_id, pvs, reqs, bids, wins, shows, clicks, income)
        SELECT
            {begin_time} AS timestamp,
            B.media_id, 
            B.adspot_id, 
            B.channel_id, 
            B.sdk_adspot_id,
            A.pvs,
            B.reqs, 
            B.bids, 
            B.wins, 
            B.shows, 
            B.clicks,
            B.income
        FROM (
            SELECT
                SUM(pvs) AS pvs,
                media_id, 
                adspot_id
            FROM 
                report_hourly
            WHERE 
                timestamp BETWEEN {begin_time} AND {end_time}
            GROUP BY 
                media_id, 
                adspot_id
        ) A
        JOIN (
            SELECT
                SUM(reqs) AS reqs,
                SUM(bids) AS bids,
                SUM(wins) AS wins,
                SUM(shows) AS shows,
                SUM(clicks) AS clicks,
                SUM(income) AS income,
                media_id, 
                adspot_id, 
                channel_id, 
                sdk_adspot_id
            FROM 
                report_hourly
            WHERE 
                timestamp BETWEEN {begin_time} AND {end_time}
            GROUP BY 
                media_id, 
                adspot_id, 
                channel_id, 
                sdk_adspot_id
        ) B
        ON A.media_id = B.media_id AND A.adspot_id = B.adspot_id
        ON DUPLICATE KEY UPDATE 
        pvs=VALUES(pvs), 
        reqs=VALUES(reqs),
        bids=VALUES(bids),
        wins=VALUES(wins),
        shows=VALUES(shows),
        clicks=VALUES(clicks),
        income=VALUES(income)
        """
        n = cursor.execute(sql)
        db.commit()
        cursor.close()
        db.close()
        return n

    def insert_daily_exp_report_from_hourly(self, begin_time):
        # 天结束时间戳
        end_time = begin_time + 86400 - 1

        db = self.get_mysql_conn()
        cursor = db.cursor()
        sql = f"""
        INSERT INTO exp_report_daily 
        (timestamp, media_id, adspot_id, channel_id, sdk_adspot_id, exp_type, exp_id, group_id,
         reqs, bids, wins, shows, clicks, income)
        SELECT
            {begin_time} AS timestamp,
            media_id, 
            adspot_id, 
            channel_id, 
            sdk_adspot_id,
            exp_type, 
            exp_id,
            group_id, 
            SUM(reqs) AS reqs,
            SUM(bids) AS bids,
            SUM(wins) AS wins,
            SUM(shows) AS shows,
            SUM(clicks) AS clicks,
            SUM(income) AS income
        FROM
            exp_report_hourly
        WHERE 
            timestamp BETWEEN {begin_time} AND {end_time}
        GROUP BY 
            media_id, 
            adspot_id, 
            channel_id, 
            sdk_adspot_id, 
            exp_type, 
            exp_id, 
            group_id
        ON DUPLICATE KEY UPDATE 
            reqs=VALUES(reqs),
            bids=VALUES(bids),
            wins=VALUES(wins),
            shows=VALUES(shows),
            clicks=VALUES(clicks),
            income=VALUES(income)
        """
        n = cursor.execute(sql)
        db.commit()
        cursor.close()
        db.close()
        return n

    # Report API
    # 获取sdk渠道report api账号
    def get_sdk_report_api_params(self):
        db = self.get_mysql_dict_conn()
        sql = """SELECT adn_id, params, account_name 
                 FROM sdk_report_api_params 
                 WHERE status = 1 and mark_deleted = 0"""
        result = self.__select_template(db, sql)
        for result_dict in result:
            # 在数据库里是json字符串，解析成json对象
            result_dict['params'] = json.loads(result_dict.get('params', '{}'))
        return result

    # 查询天报表sdk_adspot_id到id的映射
    def get_record_id_map(self, timestamp, sdk_adspot_id_list):
        db = self.get_mysql_conn()
        sdk_adspot_id_str = ','.join([f"'{x}'" for x in sdk_adspot_id_list])
        sql = f"""SELECT channel_id, sdk_adspot_id, id,
                         reqs, bids, shows, clicks, income 
                  FROM report_daily
                  WHERE timestamp = {timestamp} AND sdk_adspot_id IN ({sdk_adspot_id_str})"""
        record_id_map = {}
        result = self.__select_template(db, sql)
        for x in result:
            channel_id = str(x[0])
            sdk_adspot_id = str(x[1])
            id = str(x[2])
            req = int(x[3])
            bid = int(x[4])
            show = int(x[5])
            click = int(x[6])
            income = float(x[7])
            if (channel_id, sdk_adspot_id) not in record_id_map:
                record_id_map[(channel_id, sdk_adspot_id)] = []
            record_id_map[(channel_id, sdk_adspot_id)].append((id, req, bid, show, click, income))
        return record_id_map

    # 根据报表记录id更新report api拉取的三方数据
    def update_report_api(self, update_list):
        db = self.get_mysql_conn()
        cursor = db.cursor()
        sql = """UPDATE report_daily
                 SET report_api_req=%s, report_api_bid=%s, report_api_imp=%s, report_api_click=%s,report_api_income=%s
                 WHERE id=%s"""
        cursor.executemany(sql, update_list)
        db.commit()
        cursor.close()
        db.close()
