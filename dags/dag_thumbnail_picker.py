from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.models.baseoperator import BaseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from bs4 import BeautifulSoup
import requests

with DAG(
    dag_id="dag_thumbnail_picker",
    schedule="0 10 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    description="특정 유튜브 채널의 비디오들의 썸네일(thumbnail_url), 업로드 날짜(uploaded_at), 뷰(views)를 크롤링해서 저장한다. "
) as dag:
    # 필요한 함수들
    # - 무한 스크롤
    # - 크롤링(썸네일, 제목, 업로드 날짜, 뷰)
    # - 데이터베이스에 저장
    # - 뷰 / 업로드 시간
    # - TOP 5 찾기
    def crawl(url):
        import time
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys

        browser = webdriver.Chrome()
        browser.get(url)

        elements = browser.find_element(By.CSS_SELECTOR, "body")
        
        thumbnail_urls, uploaded_ats, views = [], [], []
        scroll_down = True
        scrollY_before = 1
        while scroll_down:
            scrollY_now = browser.execute_script("return document.scrollY")
        return
    
    def save_in_mongo():
        return
    
    def get_view_per_hour():
        return

    def get_top_five():
        return

    task_test1 = BashOperator(
        task_id="task_test1",
        bash_command="echo ping"
    )

    task_test2 = BashOperator(
        task_id="task_test2",
        bash_command="echo pong"
    )



    task_test1 >> task_test2