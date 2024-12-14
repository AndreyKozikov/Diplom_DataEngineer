from selenium import webdriver
from selenium.common import TimeoutException, ElementClickInterceptedException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from utils import json_serializing_data, cookie_window_close


def navigate(xpats, driver, action) -> bool:
    try:
        link = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, xpats)))
        action.move_to_element(link).click()
        action.perform()
        return True
    except:
        print(f"Ошибка при навигации по {xpats}")
        return False


def links_parser(driver):
    links = []
    try:
        page_source = (WebDriverWait(driver, 30)
                       .until(EC.visibility_of_element_located((By.XPATH,
                                                                "//table/tbody[@class='bonds__table-list bonds__table-wrapper wrapper-p']"))))
        rows = page_source.find_elements(By.XPATH, ".//tr//a")
        for link in rows:
            href = link.get_attribute("href")
            links.append(href)
        return links
    except TimeoutException:
        print(f"Ошибка: Не удалось найти элементы на странице. ")
        return []


def links_parser_task(**kwargs):
    ti = kwargs['ti']  # Извлечение TaskInstance
    conf_options = ti.xcom_pull(task_ids='config_file_load', key='config')
    print(conf_options)
    chrome_options = Options()
    for value in conf_options['chrome_options']:
        chrome_options.add_argument(value)
    start_url = conf_options['start_url']
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(start_url)
    cookie_window_close(driver)
    action = ActionChains(driver)
    links = []
    next_page = True
    i = 0
    while next_page and (i < conf_options['page_count'] or conf_options['environment'] == "production"):
        links.extend(links_parser(driver))
        time.sleep(1)
        next_page = navigate("//div[contains(@class,'dataTables_paginate')]/a[@class='paginate_button next']",
                             driver=driver, action=action)
        i += 1
    print(f"Всего найдено {len(links)} ссылок.")
    driver.quit()
    if len(links) > 0:
        ti.xcom_push(key='links', value=links)
    else:
        raise AirflowException("Задача завершена неудачно: результат не соответствует ожиданиям.")

