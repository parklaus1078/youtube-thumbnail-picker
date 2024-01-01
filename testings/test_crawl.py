import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

url_bighead = "https://www.youtube.com/@bighead033/videos"
url_nani = "https://www.youtube.com/@user-yy5dx4lm9o/videos"
uploaded_at_stamps = {
    "second": 1 / (60 * 60),
    "seconds": 1 / (60 * 60),
    "minute": 1 / 60,
    "minutes": 1 / 60,
    "hour": 1,
    "hours": 1,
    "day": 24,
    "days": 24,
    "month": 24 * 30,
    "months": 24 * 30,
    "year": 24 * 30 * 365,
    "years": 24 * 30 * 365,
}

browser = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))
browser.get(url_nani)
action = browser.find_element(By.CSS_SELECTOR, "body")
reached_bottom = False
scrollY_old, scrollY_new = 0, 0

# 스크롤 끝까지
while not reached_bottom:
    action.send_keys(Keys.END)
    time.sleep(2)
    scrollY_new = browser.execute_script("return scrollY")

    if scrollY_new == scrollY_old:
        reached_bottom = True
    else:
        scrollY_old = scrollY_new

thumbnail_elements = browser.find_elements(By.CSS_SELECTOR, '.yt-core-image')
aria_label_elements = browser.find_elements(By.CSS_SELECTOR, "#video-title")

thumbnail_list, raw_label_list, title_list, views_list, hours_uploaded_list = [], [], [], [], []
for i, thumbnail_element in enumerate(thumbnail_elements):
    thumbnail_list.append(thumbnail_element.get_attribute("src"))
    raw_label = aria_label_elements[i].get_attribute("aria-label")
    raw_label_list.append(raw_label)

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


print(thumbnail_list)
print(raw_label_list)
print(title_list)
print(views_list)
print(hours_uploaded_list)

