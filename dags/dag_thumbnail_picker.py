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

    # Objective : Crawl thumbnail, title, views, and hours_uploaded from youtube and push them to xcom
    # Input : url
    # Output : -
    def crawl(ti, url, name, **kwargs):
        import time
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from datetime import datetime
        import pytz

        uploaded_at_stamps = {
            "second": 1 / (60 * 60),
            "seconds": 1 / (60 * 60),
            "minute": 1 / 60,
            "minutes": 1 / 60,
            "hour": 1,
            "hours": 1,
            "day": 24,
            "days": 24,
            "week": 24 * 7,
            "weeks": 24 * 7,
            "month": 24 * 30,
            "months": 24 * 30,
            "year": 24 * 30 * 365,
            "years": 24 * 30 * 365
        }

        options = webdriver.ChromeOptions()
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-setuid-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--start-maximized")
        options.add_argument("--window-size=1920,1080")
        driver = webdriver.Remote(command_executor="http://remote_chromedriver:4444/wd/hub", options=options)
        # with webdriver.Remote(command_executor="http://remote_chromedriver:4444/wd/hub", options=options) as driver:
        print(url)
    
        driver.get(url)
        action = driver.find_element(By.CSS_SELECTOR, "body")
        reached_bottom = False
        scrollY_old = 0
        while not reached_bottom:
            action.send_keys(Keys.END)
            time.sleep(2)
            scrollY_new = driver.execute_script("scrollY")
            if scrollY_new == scrollY_old:
                reached_bottom = True
            else:
                scrollY_old = scrollY_new
        
        thumbnail_elements = driver.find_elements(By.CSS_SELECTOR, '.yt-core-image')
        aria_label_elements = driver.find_elements(By.CSS_SELECTOR, "#video-title")
        thumbnail_list, raw_label_list, title_list, views_list, hours_uploaded_list = [], [], [], [], []
        for i, thumbnail_element in enumerate(thumbnail_elements):
            thumbnail_list.append(thumbnail_element.get_attribute("src"))
            raw_label = aria_label_elements[i].get_attribute("aria-label")
            raw_label_list.append(raw_label)
            label_splited = raw_label.split("by")
            title = label_splited[0].strip()
            title_list.append(title)

            # 2번째 레이블 문자열에서 views, uploaded at을 구해야함
            supplementary_label_splited = label_splited[1].split(" ")
            # 1. views 는 views 앞의 숫자. 쉼표는 제거한 다음, 숫자로 변환해야 함.
            views_idx = supplementary_label_splited.index("views") - 1
            views = int(supplementary_label_splited[views_idx].replace(",", ""))
            views_list.append(views)
            # 2. uploaded 는 second/seconds/minute/minutes/hour/hours/day/days/month/months/year/years ago 앞의 숫자를 구한 다음 시간으로 변환해야 함.
            time_unit_idx = supplementary_label_splited.index("ago") - 1
            time_idx = time_unit_idx - 1
            hours_uploaded = float(supplementary_label_splited[time_idx]) * uploaded_at_stamps[supplementary_label_splited[time_unit_idx]]
            hours_uploaded_list.append(hours_uploaded)
            time_today = datetime.now(pytz.timezone("Asia/Seoul"))
            date_today = time_today.strftime("%Y%m%d")
            
            ti.xcom_push(key="channel_owner_name", value=name)
            for i, title in enumerate(title_list):
                thumbnail = thumbnail_list[i]
                views = views_list[i]
                hours_uploaded = hours_uploaded_list[i]
                ti.xcom_push(
                    key=f"video_{i + 1}", 
                    value = {
                        "title": title,
                        "thumbnail": thumbnail,
                        "views": views,
                        "hours_uploaded": hours_uploaded,
                        "document_created_at": date_today
                    }
                )
            ti.xcom_push(key="total_number_of_videos", value=len(title_list))
        print("xcom migration completed.")
    
    # Objective : create new document if a document with the thumbnail and title does not exist. If there is a document with the thumbnail and title, edit views and hours uploaded
    # Input : -
    # Output : log
    def save_in_mongo(ti):
        hook = MongoHook(conn_id="edu_mongo")
        client = hook.get_conn()
        youtube_data_table = hook.get_collection(mongo_collection="youtube_data")
        print(f"Connected to MongoDB - {client.server_info()}")

        total_number_videos = ti.xcom_pull(key="total_number_of_videos", task_ids="task_crawl")
        creator_name = ti.xcom_pull(key="channel_owner_name", task_ids="task_crawl")
        for i in range(total_number_videos):
            video_info = ti.xcom_pull(key=f"video_{i + 1}", task_ids="task_crawl")
            video_info["creator"] = creator_name
            youtube_data_table.insert_one(video_info)
        
    def get_view_per_hour():
        return

    def get_top_five():
        return

    # task_test1 = BashOperator(
    #     task_id="task_test1",
    #     bash_command="echo ping"
    # )

    # task_test2 = BashOperator(
    #     task_id="task_test2",
    #     bash_command="echo pong"
    # )

    task_crawl = PythonOperator(
        task_id="task_crawl",
        python_callable=crawl,
        op_kwargs={
            "url": "https://www.youtube.com/@user-yy5dx4lm9o/videos",
            "name": "나니의 연대기"
        }
    )

    task_mongo_create = PythonOperator(
        task_id="task_mongo_create",
        python_callable=save_in_mongo,
    )

    task_crawl >> task_mongo_create