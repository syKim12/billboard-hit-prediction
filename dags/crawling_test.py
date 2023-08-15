import os, json, csv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, timedelta
from urllib.request import urlopen
import requests, csv, datetime
from bs4 import BeautifulSoup
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator


def crawling(crawl_date, date, csv_filename):
    if crawl_date.weekday() == 5: # crawl only on saturday
        url = "https://www.billboard.com/charts/hot-100/" + date + "/"
        req = requests.get(url)  
        html = req.text
        bsObject = BeautifulSoup(html, "html.parser") 

        # get data from billboard website
        ranks = bsObject.select('div.pmc-paywall > div > div > div > div.chart-results-list.\/\/.lrv-u-padding-t-150.lrv-u-padding-t-050\@mobile-max > div > ul > li.o-chart-results-list__item.\/\/.lrv-u-background-color-black.lrv-u-color-white.u-width-100.u-width-55\@mobile-max.u-width-55\@tablet-only.lrv-u-height-100p.lrv-u-flex.lrv-u-flex-direction-column\@mobile-max.lrv-u-flex-shrink-0.lrv-u-align-items-center.lrv-u-justify-content-center.lrv-u-border-b-1.u-border-b-0\@mobile-max.lrv-u-border-color-grey > span.c-label.a-font-primary-bold-l.u-font-size-32\@tablet.u-letter-spacing-0080\@tablet')
        songs = bsObject.select('div.pmc-paywall > div > div > div > div.chart-results-list.\/\/.lrv-u-padding-t-150.lrv-u-padding-t-050\@mobile-max > div > ul > li.lrv-u-width-100p > ul > li.o-chart-results-list__item.\/\/.lrv-u-flex-grow-1.lrv-u-flex.lrv-u-flex-direction-column.lrv-u-justify-content-center.lrv-u-border-b-1.u-border-b-0\@mobile-max.lrv-u-border-color-grey-light.lrv-u-padding-l-1\@mobile-max')

        # save into a csv file
        csv_open = open(csv_filename, 'w', encoding='UTF-8', newline='')
        csv_writer = csv.writer(csv_open)
        csv_writer.writerow(('Date','Rank','Title','Artist'))

        for i in zip(ranks, songs):
            day = date
            rank = i[0].text.strip()
            title = i[1].find('h3').text.strip()
            artist = i[1].find('span').text.strip()
            csv_writer.writerow([day, rank, title, artist])

        print("SUCCESS", date)
        csv_open.close()
        
    else:
        print("IT IS NOT SATURDAY", crawl_date)


def load_data():
    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()
    db = client['music']
    collection = db['chart']
    
    with open(csv_filename, 'r') as file:
        csv_data = csv.DictReader(file)
        for row in csv_data:
            collection.insert_one(row)

    client.close()


crawling_dag = DAG(
    dag_id='crawling_billboard_test',
    catchup=False,
    start_date=datetime.datetime(2023, 4, 10),
    schedule='47 6 * * *',
    tags=['crawling']
)

# initial setting
crawl_date = datetime.date(2023, 8, 12)
date = crawl_date.strftime('%Y-%m-%d')
csv_filename = date + "_chart.csv" 

# start crawling
crawling_operator = PythonOperator(
        task_id='crawling',
        python_callable=crawling,
        op_kwargs={"crawl_date": crawl_date, "date": date, "csv_filename": csv_filename},
        dag=crawling_dag)

# start uploading csv to S3 bucket
create_object = S3CreateObjectOperator(
    task_id="create_object",
    s3_bucket="billboard-chart",
    s3_key="csv_files/" + csv_filename,
    data=csv_filename,
    replace=True,
    aws_conn_id='aws_access',
)

load_data = PythonOperator(
    task_id='load-csv-to-mongo',
    python_callable=load_data,
    dag=crawling_dag
)


crawling_operator >> create_object >> load_data