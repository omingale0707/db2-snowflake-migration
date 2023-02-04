import pyodbc
import snowflake.connector
import pandas as pd
import math
import yaml
import os
from datetime import datetime
import glob


class Etl:
    def __init__(self, config_path, data_path):
        self.config_path = config_path
        self.data_path = data_path + '{tableName}\\'

    def extractData(self, table_name):
        with open(self.config_path, encoding='utf-8') as file:
            con_list = yaml.load(file, Loader=yaml.FullLoader)  # dictionary with credentials

        tableName = table_name

        start_time = datetime.today()

        ctx = snowflake.connector.connect(
            user=con_list['snowflake_conn']['user'],
            password=con_list['snowflake_conn']['password'],
            account=con_list['snowflake_conn']['account'])

        cs = ctx.cursor()

        cs.execute(
            "select max(EXTRACT_START_TIME) from " + con_list['snowflake_conn']['database'] + ".LANDING.JOB_STATUS_TABLE where JOB_NAME='{tableName}' and EXTRACT_STATUS='SUCCESS' and LANDING_STATUS='SUCCESS' and STAGE_STATUS='SUCCESS'".format(
                tableName=tableName))

        last_time = '1900-01-01'
        for row in cs.fetchall():
            last_time = row[0]

        conn = pyodbc.connect(
            'DSN=' + con_list['extractdata']['DSN'] + ';UID=' + con_list['extractdata']['UID'] + ';PWD=' +
            con_list['extractdata']['PWD'])
        cursor = conn.cursor()

        if con_list['extractdata'][tableName]['query'] is None:
            sqlTbl = "select * from " + con_list['extractdata']['schema'] + ".{tableName} limit {limit} offset {offset}"
        else:
            sqlTbl = con_list['extractdata'][tableName]['query'] + " limit {limit} offset {offset}"

        try:
            if con_list['extractdata'][tableName]['query'] is None:
                cursor.execute("select count(*) from " + con_list['extractdata']['schema'] + ".{tableName}".format(
                    tableName=tableName))
                for row in cursor.fetchall():
                    noOfRcrds = row[0]
            else:
                cursor.execute("select count(*) from ( " + con_list['extractdata'][tableName]['query'].format(
                    last_runtime=last_time) + ")")
                for row in cursor.fetchall():
                    noOfRcrds = row[0]

            print("no of records ", noOfRcrds)
            noOfPart = con_list['extractdata'][tableName]['partition']

            if os.path.isdir(self.data_path[:-1].format(tableName=tableName)):
                number_files = len(os.listdir(self.data_path.format(tableName=tableName)))  # folder path
                if noOfPart != number_files:
                    files = glob.glob((self.data_path + '*').format(tableName=tableName))
                    for f in files:
                        os.remove(f)

            if noOfRcrds <= 1000:
                noOfPart = 1
            elif noOfRcrds <= 10000:
                noOfPart = 10

            print("No of partitions ", noOfPart)

            limit = math.floor(noOfRcrds / noOfPart)
            mod = noOfRcrds % noOfPart  # the remainder
            last_limit = mod + limit  # the remainder + the limit value to be taken as limit for last partition records

            for i in range(noOfPart):
                if i == noOfPart - 1:
                    offset = i * limit
                    if con_list['extractdata'][tableName]['query'] is None:
                        sql = sqlTbl.format(tableName=tableName, limit=last_limit, offset=offset)
                    else:
                        sql = sqlTbl.format(last_runtime=last_time, limit=last_limit, offset=offset)
                    print(sql)
                    df = pd.read_sql(sql, conn)

                    df.to_csv((self.data_path + '{tableName}-{partition}.csv').format(tableName=tableName, partition=i),
                              index=False, header=False)
                else:
                    offset = i * limit
                    if con_list['extractdata'][tableName]['query'] is None:
                        sql = sqlTbl.format(tableName=tableName, limit=limit, offset=offset)
                    else:
                        sql = (con_list['extractdata'][tableName]['query'] + " limit {limit} offset {offset}").format(
                            last_runtime=last_time,
                            limit=limit, offset=offset)
                    print(sql)
                    df = pd.read_sql(sql, conn)
                    df.to_csv((self.data_path + '{tableName}-{partition}.csv').format(tableName=tableName, partition=i),
                              index=False, header=False)

            end_time = datetime.today()

            cs.execute(
                "insert into " + con_list['snowflake_conn']['database'] + ".LANDING.JOB_STATUS_TABLE(JOB_NAME, EXTRACT_START_TIME, EXTRACT_END_TIME, EXTRACT_STATUS) values('{tableName}','{start_time}', '{end_time}', 'SUCCESS')".format(
                    tableName=tableName, start_time=start_time, end_time=end_time))

        except Exception as e:
            print(e)
            cs.execute(
                "insert into " + con_list['snowflake_conn']['database'] + ".JOB_STATUS_TABLE(JOB_NAME, EXTRACT_START_TIME, EXTRACT_END_TIME, EXTRACT_STATUS) values('{tableName}','{start_time}', NULL, 'FAIL')".format(
                    tableName=tableName, start_time=start_time))

        ctx.close()
        conn.close()

    def loadSnowflake(self, table_name):
        with open(self.config_path, encoding='utf-8') as file:
            # The FullLoader parameter handles the conversion from YAML
            # scalar values to Python the dictionary format
            con_list = yaml.load(file, Loader=yaml.FullLoader)

        tableName = table_name

        ctx = snowflake.connector.connect(
            user=con_list['snowflake_conn']['user'],
            password=con_list['snowflake_conn']['password'],
            account=con_list['snowflake_conn']['account'])

        cs = ctx.cursor()

        try:
            start_time = datetime.today()

            ctx.cursor().execute("USE DATABASE " + con_list['snowflake_conn']['database'])
            ctx.cursor().execute("USE SCHEMA LANDING")

            # ctx.cursor().execute("remove @%{tableName}".format(tableName=tableName))
            ctx.cursor().execute(
                ("PUT file://" + self.data_path + "* @%{tableName} auto_compress=true overwrite=true").format(
                    tableName=tableName))
            ctx.cursor().execute("truncate table {tableName}".format(tableName=tableName))
            ctx.cursor().execute(
                "COPY INTO {tableName} FILE_FORMAT = (format_name = 'csv_landing') ON_ERROR = CONTINUE".format(
                    tableName=tableName))

            end_time = datetime.today()

            ctx.cursor().execute(
                "update JOB_STATUS_TABLE set LANDING_START_TIME='{start_time}',LANDING_END_TIME='{end_time}',LANDING_STATUS='SUCCESS' where JOB_NAME='{tableName}' and EXTRACT_END_TIME=(select max(EXTRACT_END_TIME) from JOB_STATUS_TABLE where JOB_NAME='{tableName}') and EXTRACT_STATUS='SUCCESS'".format(
                    tableName=tableName, start_time=start_time, end_time=end_time))

        except Exception as e:
            print(e)
            ctx.cursor().execute(
                "update JOB_STATUS_TABLE set LANDING_START_TIME='{start_time}',LANDING_STATUS='FAIL' where JOB_NAME='{tableName}' and EXTRACT_END_TIME=(select max(EXTRACT_END_TIME) from JOB_STATUS_TABLE where JOB_NAME='{tableName}') and EXTRACT_STATUS='SUCCESS'".format(
                    tableName=tableName, start_time=start_time))

        ctx.close()

    def loadStage(self, table_name):
        with open(self.config_path, encoding='utf-8') as file:
            # The FullLoader parameter handles the conversion from YAML
            # scalar values to Python the dictionary format
            con_list = yaml.load(file, Loader=yaml.FullLoader)

        tableName = table_name

        ctx = snowflake.connector.connect(
            user=con_list['snowflake_conn']['user'],
            password=con_list['snowflake_conn']['password'],
            account=con_list['snowflake_conn']['account']
            )

        cs = ctx.cursor()

        ctx.cursor().execute("USE DATABASE " + con_list['snowflake_conn']['database'])
        ctx.cursor().execute("USE SCHEMA LANDING")
        ctx.cursor().execute("call sp_load_stage('{tableName}')".format(tableName=tableName))
        ctx.close()
