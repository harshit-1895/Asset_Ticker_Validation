import os
import sys
import time
import pyodbc
import boto3
import pytz
import requests
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from emailutility import sendmailutility
from config import *


# Loading env file
load_dotenv('configuration.env')

BUCKET = os.environ["BUCKET"]
DAR_AWS_KEY = os.environ["DAR_AWS_KEY"]
DAR_AWS_SECRET_KEY = os.environ["DAR_AWS_SECRET_KEY"]
SERVER = os.environ["SERVER"]
USER_ID = os.environ["USER_ID"]
PASSWORD = os.environ["PASSWORD"]
PORT = os.environ["PORT"]
DATABASE = os.environ["DATABASE"]
START_HOUR = int(os.environ['START_HOUR'])
START_MINUTE = int(os.environ['START_MINUTE'])
SLACK_ALERT_WEBHOOK = os.environ['SLACK_ALERT_WEBHOOK']

class rowCount:

    def __init__(self):
        self.message = "Exchange Row Count Report\n"
        # Setting up S3 Connection
        self.s3 = self.s3_connection()
        self.cursor, self.cnxn = self.connection_rds()
        # Today Timestamp
        d_today = datetime.today()
        # date_today = d_today.astimezone(pytz.timezone('US/Eastern'))
        self.folder_name = f'{d_today.year}{d_today.month:02d}{d_today.day:02d}'
        self.df = self.data_insert()
        self.insert_into_db()
        self.check_threshold2()
        self.cursor.close()

    def s3_connection(self):
        """
        Establish connection with aws and return S3 object.
        """
        try:
            aws_session = boto3.Session(
                aws_access_key_id=DAR_AWS_KEY,
                aws_secret_access_key=DAR_AWS_SECRET_KEY)
            aws_s3 = aws_session.resource('s3')
        except Exception as e:
            self.message = self.message + '\n' + 'S3 Connection Failed' + str(e)
            self.slack_alert()

        return aws_s3

    def connection_rds(self):
        """
        Establish connection with RDS and return cursor object
        """
        try:
            conn_string = 'DRIVER={};PORT=1433;SERVER={};UID={};PWD={};'.format("{ODBC Driver 17 for SQL Server}",
                                                                                SERVER,
                                                                                USER_ID, PASSWORD)
            conn = pyodbc.connect(conn_string, autocommit=True)
            cursor = conn.cursor()
            print("Connection successful")
        except Exception as e:
            self.message = self.message + '\n' + 'Connection to rds Failed' + e
            print(self.message)
            self.slack_alert()

        return cursor, conn

    def data_insert(self):
        try:
            file_list = []
            exchange_list = []
            df_list = {}
            pd.set_option('display.max_columns', 40)
            my_bucket = self.s3.Bucket('scrapingfilelistrdsintegarion')
            for object_summary in my_bucket.objects.filter(Prefix=self.folder_name):
                if object_summary.key.endswith('.csv'):
                    file_list.append(object_summary.key)
            print(file_list)
            for file in file_list:
                file_name = file.split('/')[1]
                exchange_name = file_name.split()[0]
                exchange_list.append(exchange_name)
                obj = self.s3.Object(BUCKET, file).get()
                obj_df = pd.read_csv(obj["Body"])
                df_list[exchange_name] = len(obj_df)
            # print(exchange_list)
            # print(df_list)
            df = pd.DataFrame(df_list.items(), columns=['Exchange', 'RowCount'])

            self.message = self.message + '\n' + 'Exchange and row count dataframe created.'
        except:
            self.message = self.message + '\n' + 'Exchange and row count dataframe creation failed.'
        finally:
            self.slack_alert()
            print("Exchange and row count dataframe")
        return df

    def insert_into_db(self):
        try:
            date = pd.to_datetime(self.folder_name)
            print(date)
            self.cursor.execute("DELETE FROM [ReferenceCore].[dbo].[ExchangeRowCount] "
                                "where convert(varchar(12),CreateDate,112)=CONVERT(varchar(12),GETDATE(), 112)")
            for index, row in self.df.iterrows():
                self.cursor.execute(
                    "INSERT INTO [ReferenceCore].[dbo].[ExchangeRowCount] (ExchangeName, ExchangeRowCount, CreateDate) "
                    "values (?,?,?)", row['Exchange'], row['RowCount'], date)
        except:
            self.message = self.message + '\n' + 'Insertion in Exchange Row Count failed.'
            self.slack_alert()


    def slack_alert(self):
        payload = {
            "text": self.message,
            "mrkdwn": 'true'
        }
        payload = str(payload)
        # requests.post(SLACK_ALERT_WEBHOOK, payload)

    def check_threshold2(self):
            try:
                query = "SELECT * FROM [ReferenceCore].[dbo].[ExchangeRowCount] " \
                        "where convert(varchar(12),CreateDate,112)<=CONVERT(varchar(12),DATEADD(DAY," + str(
                    CURRENTDATEMINUS) + ", GETDATE()), 112) ORDER BY CreateDate DESC"
                data = pd.read_sql(query, self.cnxn)
                columns = ('Exchange', 'CurrentCount', 'PreviousCount', 'rowCountDifference')
                new_df = pd.DataFrame(columns=columns)
                exchange_list = []
                current_count_list = []
                previous_count_list = []
                for index, row in self.df.iterrows():
                    for i, real in data.iterrows():
                        if row['Exchange'] == real['ExchangeName']:
                            exchange_list.append(row['Exchange'])
                            current_count_list.append(row['RowCount'])
                            previous_count_list.append(real['ExchangeRowCount'])
                            break

                query1 = "SELECT ExchangeName, COUNT(ExchangeName) as ExchangeCount " \
                         "FROM [ReferenceCore].[dbo].[ExchangeRowCount] group by " \
                         "ExchangeName Having COUNT(ExchangeName) = 1 order by ExchangeCount"
                data1 = pd.read_sql(query1, self.cnxn)
                length_of_df = len(data1)
                if length_of_df > 0:
                    for i, row in data1.iterrows():
                        for j, real in self.df.iterrows():
                            if row['ExchangeName'] == real['Exchange']:
                                exchange_list.append(row['ExchangeName'])
                                current_count_list.append(real['RowCount'])
                                previous_count_list.append(0)

                new_df = pd.DataFrame(
                    {'Exchange': exchange_list, 'CurrentCount': current_count_list,
                     'PreviousCount': previous_count_list})

                new_df['rowCountDifference'] = new_df['CurrentCount'] - new_df['PreviousCount']

                new_df = new_df[(new_df['rowCountDifference'] >= THRESHOLD_VALUE) | (
                        new_df['rowCountDifference'] <= (THRESHOLD_VALUE * -1))]
                new_df.set_index('Exchange', inplace=True)
                new_df.to_csv('merge_data.csv')
                subject = "Asset Ticker Validation Report - " + self.folder_name
                body = "Hi All,\n\nPlease find the attached file for Asset Ticker Validation.\n\nRegards,\nDigital Asset Reasearch"
                all_files = os.listdir()
                filename = list(filter(lambda f: f.endswith('.csv'), all_files))
                obj = sendmailutility(subject, body, TO, FROM, filename[0])
                obj.send_email_with_attachment()
                self.message = self.message + '\n' + 'Asset Ticker Validation calculated successfully.'
            except:
                self.message = self.message + '\n' + 'Asset Ticker Validation calculation failed.'
            finally:
                self.slack_alert()
                print("Row Count Difference")


if __name__ == '__main__':
    rowCount()
    date_today = datetime.today().astimezone(pytz.timezone('US/Eastern'))
    start_today = date_today.replace(hour=START_HOUR, minute=START_MINUTE, second=15, microsecond=0)
    start_stamp = start_today.timestamp()
    print("Checking for timestamp", time.time(), "against", start_stamp)
    while True:
        try:
            if (time.time() >= start_stamp):
                print(f'Running for {start_stamp}')
                rowCount()
                start_today = start_today + timedelta(days=1)
                start_stamp = start_today.timestamp()
                print(f'Waiting for {start_stamp - time.time()}')
        except Exception as e:
            msg = f'Error occurred in Asset Ticker Validation Calculation. Trace: {e}'
            payload = {
                "text": "Error Occured in Asset Ticker Validation  " + msg,
                "mrkdwn": 'true'
            }
            payload = str(payload)
            # requests.post(SLACK_ALERT_WEBHOOK, payload)
            start_today = start_today + timedelta(days=1)
            start_stamp = start_today.timestamp()