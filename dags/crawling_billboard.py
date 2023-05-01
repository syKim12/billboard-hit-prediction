import datetime
from urllib.request import urlopen
import requests, csv, datetime
from bs4 import BeautifulSoup
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

def crawling():
    crawl_date = datetime.datetime.now() - datetime.timedelta(days=1)
    if crawl_date.weekday() == 5: # crawl only on saturday
        date = crawl_date.strftime('%Y-%m-%d')
        url = "https://www.billboard.com/charts/hot-100/" + date + "/"
        req = requests.get(url)  
        html = req.text
        bsObject = BeautifulSoup(html, "html.parser") 

        # get data from billboard website
        ranks = bsObject.select('div.pmc-paywall > div > div > div > div.chart-results-list.\/\/.lrv-u-padding-t-150.lrv-u-padding-t-050\@mobile-max > div > ul > li.o-chart-results-list__item.\/\/.lrv-u-background-color-black.lrv-u-color-white.u-width-100.u-width-55\@mobile-max.u-width-55\@tablet-only.lrv-u-height-100p.lrv-u-flex.lrv-u-flex-direction-column\@mobile-max.lrv-u-flex-shrink-0.lrv-u-align-items-center.lrv-u-justify-content-center.lrv-u-border-b-1.u-border-b-0\@mobile-max.lrv-u-border-color-grey > span.c-label.a-font-primary-bold-l.u-font-size-32\@tablet.u-letter-spacing-0080\@tablet')
        songs = bsObject.select('div.pmc-paywall > div > div > div > div.chart-results-list.\/\/.lrv-u-padding-t-150.lrv-u-padding-t-050\@mobile-max > div > ul > li.lrv-u-width-100p > ul > li.o-chart-results-list__item.\/\/.lrv-u-flex-grow-1.lrv-u-flex.lrv-u-flex-direction-column.lrv-u-justify-content-center.lrv-u-border-b-1.u-border-b-0\@mobile-max.lrv-u-border-color-grey-light.lrv-u-padding-l-1\@mobile-max')

        # save into a csv file
        csv_filename = date + "_chart.csv"
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
        

crawling_dag = DAG(
    dag_id='crawling_billboard',
    catchup=False,
    start_date=datetime.datetime(2023, 4, 10),
    schedule='@weekly',
    tags=['crawling']
)
    
crawling_operator = PythonOperator(
        task_id='crawling',
        python_callable=crawling,
        dag=crawling_dag)

crawling_operator


