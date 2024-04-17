from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
from urllib.parse import urlencode
import requests
import clickhouse_connect

default_args = {
    'owner':'vladperesad',
    'depends_on_past':False,
    'start_date':datetime(2024,3,11),
    'retries':0
}

@dag("report_dag_wed_1905",
     default_args=default_args,
     catchup=False,
     schedule='0 9 * * 1')

def taskflow():
    @task
    def connect_to_clickhouse_and_pull_data():
        with open('/home/vladperesad/keys/clickhouse_connect_key.json') as src:
            data = json.load(src)

        client = clickhouse_connect.get_client(host=data['host'],
                                       port=8443,
                                       username=data['user'],
                                       password=data['password'])
        
        print('credentials passed')
    
        #connect to the database and pull necessary data
    
        #df_14 contains attendance and grades records for the last two weeks
        df_14 = client.query_df('''
            SELECT
                 at.student_id AS student_id,
                 st.student_name AS student_name,
                 gr.group_name as group_name,
                 date,
                 homework,
                 behaviour,
                 comprehension,
                 vocabulary,
                 speaking,
                 reading,
                 writing
            FROM
                luxinedu_2.attendance AS at 
            JOIN luxinedu_2.students AS st ON at.student_id=st.student_id
            JOIN luxinedu_2.groups AS gr ON st.group_id=gr.group_id
            WHERE date > now() - INTERVAL 14 DAY AND gr.group_id = 1
            ORDER BY 
            at.student_id,
            date
            ''')
        
        print('df_14 has peen pulled up')
        
        #df_7 contains attendance and grades records for last week
        df_7 = client.query_df('''
            SELECT
                 at.student_id AS student_id,
                 st.student_name AS student_name,
                 gr.group_name as group_name,
                 date,
                 homework,
                 behaviour,
                 comprehension,
                 vocabulary,
                 speaking,
                 reading,
                 writing
            FROM
                luxinedu_2.attendance AS at 
            JOIN luxinedu_2.students AS st ON at.student_id=st.student_id
            JOIN luxinedu_2.groups AS gr ON st.group_id=gr.group_id
            WHERE date > now() - INTERVAL 7 DAY AND gr.group_id = 1
            ORDER BY 
            at.student_id,
            date
            ''')

        print('df_7 has peen pulled up')
        
        #turn datetime datetype into just date
        df_7['date'] = df_7['date'].dt.date

        #create a subset that only contains student_ids
        df_st = df_14['student_id']
        
        #calculate percent difference in students performance beween most recent class and the class before that
        df_pct = df_14 \
            .drop(columns=['student_name',
                            'group_name',
                            'date']) \
            .groupby('student_id') \
            .pct_change(fill_method=None)*100
        
        #join subset with student_ids and round percent difference values
        df_pct = df_pct \
            .round(1) \
            .dropna(axis=0,
                    how='all') \
            .join(df_st,
                  how='inner')
        
        #merge table with records for the past week with the table that contains percentage dfference
        final_table = df_7 \
            .merge(df_pct,
               how='left',
               on='student_id',
               suffixes=('_act','_pct'))

        print('final_table has been composed')

        return final_table
    
    @task
    def build_report_and_send_id(final_table):
        #write credentials for telegram api
        with open('/home/vladperesad/keys/token_wed1905.json') as src:
            data = json.load(src)
            
        token = data['token']
        chat_id = data['chat_id']

        #compose and send message
        for index, row in final_table.iterrows():
            date = row['date']
            student_name = row['student_name']
            homework_a = row['homework_act']
            homework_p = row['homework_pct']
            behaviour_a = row['behaviour_act']
            behaviour_p = row['behaviour_pct']
            comprehension_a = row['comprehension_act']
            comprehension_p = row['comprehension_pct']
            vocabulary_a = row['vocabulary_act']
            vocabulary_p = row['vocabulary_pct']
            speaking_a = row['speaking_act']
            speaking_p = row['speaking_pct']
            reading_a = row['reading_act']
            reading_p = row['reading_pct']
            writing_a = row['writing_act']
            writing_p = row['writing_pct']
                
            message = f''' Report on the class on {date}
            Student : {student_name}
            Homework: {homework_a} ({homework_p}%)
            Behaviour: {behaviour_a} ({behaviour_p}%)
            Comprehension: {comprehension_a} ({comprehension_p}%)
            Vocabulary: {vocabulary_a} ({vocabulary_p}%)
            Speaking: {speaking_a} ({speaking_p}%)
            Reading: {reading_a} ({reading_p}%)
            Writing: {writing_a} ({writing_p}%)'''
        
            message = message
            params = {'chat_id': chat_id,
              'text': message}
            base_url = f'https://api.telegram.org/bot{token}/'
            url = base_url + 'sendMessage?' + urlencode(params)
            resp = requests.get(url)

    build_report_and_send_id(connect_to_clickhouse_and_pull_data())
dag = taskflow()