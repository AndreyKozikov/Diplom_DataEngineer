from selenium import webdriver
from selenium.common import TimeoutException, ElementClickInterceptedException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from utils import json_serializing_data, cookie_window_close

def desc_parser(link, driver):
    # Ожидаем появления элемента <bond-analysis> на странице
    bond_analysis_element = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.TAG_NAME, "bond-analysis"))
    )
    isin = link.split("/")[-1]
    main_data = []
    # Используем JavaScript для доступа к Shadow DOM
    shadow_root = driver.execute_script("return arguments[0].shadowRoot", bond_analysis_element)
    tabs = shadow_root.find_elements(By.CSS_SELECTOR, "div.dohod-tabs ul li")
    for tab in tabs:
        # Проверим, содержит ли элемент текст "Подробно", "Комментарии", и т.д.
        if tab.text.strip() == "Подробно":
            try:
                tab.click()
                main_data = shadow_root.find_elements(By.CSS_SELECTOR, "div.dohod-tabs-details > div > div")
            except ElementClickInterceptedException as e:
                print(f"Закладка {tab.text} не найдена")
            # Получаем все элементы в div.dohod-tabs.details
            finally:
                return (isin,  {'main': main_data[0].text,
                               'portfolio': main_data[7].text,
                               'tax': main_data[8].text}) if main_data else (isin, {'main': None})


def description_parser_task(**kwargs):
    ti = kwargs['ti']
    conf_options = ti.xcom_pull(task_ids='config_file_load', key='config')
    chrome_options = Options()
    for value in conf_options['chrome_options']:
        chrome_options.add_argument(value)
    driver = webdriver.Chrome(options=chrome_options)
    links = ti.xcom_pull(task_ids='link_parser', key='links')
    bonds_desc = dict()
    driver.get(links[0])  # Осуществляем переход, чтобы закрыть окно
    cookie_window_close(driver)
    for link in links:
        driver.get(link)
        time.sleep(0.5)
        isin, data = desc_parser(link, driver)
        bonds_desc[isin] = data
    ti.xcom_push(key='desc_bonds', value=bonds_desc)
    path = conf_options['output_dir'] + conf_options['description_output_file']
    json_serializing_data(data=bonds_desc, filename=path, mode="w")
