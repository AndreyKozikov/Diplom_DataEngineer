from selenium import webdriver
from selenium.common import TimeoutException, ElementClickInterceptedException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from utils import json_serializing_data, cookie_window_close


def parser(link, driver):
    # Ожидаем появления элемента <bond-analysis> на странице
    print(f"Переход по ссылке {link}")
    main_data = []
    isin = link.split("/")[-1]
    try:
        bond_analysis_element = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "bond-analysis"))
        )
    except:
        return (isin, [])
    time.sleep(2)
    # Используем JavaScript для доступа к Shadow DOM
    shadow_root = driver.execute_script("return arguments[0].shadowRoot", bond_analysis_element)
    tabs = shadow_root.find_elements(By.CSS_SELECTOR, "div.dohod-tabs ul li")
    print(f"Всего найдено закладок {len(tabs)}")
    for tab in tabs:
        if tab.text.strip() == "Календарь выплат":
            try:
                tab.click()
                checkbox_label = shadow_root.find_element(By.CSS_SELECTOR, "div.dohod-switch label.dohod-switch_switch")
                if checkbox_label.is_displayed():
                    checkbox_label.click()  # Кликаем по <label>, если он видим
                else:
                    print("Элемент скрыт или не доступен для взаимодействия")
                # Получаем все элементы в div.dohod-tabs.details
                main_data = shadow_root.find_elements(By.CSS_SELECTOR,
                                                      "div.dohod-description-info-table_wrapper > table")

            except ElementClickInterceptedException:
                print(f"Закладка {tab.text} не найдена")
            finally:
                return (isin, main_data[0].text) if len(main_data[0].text) > 0 else (isin, [])


def coupon_parser_task(**kwargs):
    ti = kwargs['ti']
    conf_options = ti.xcom_pull(task_ids='config_file_load', key='config')
    chrome_options = Options()
    for value in conf_options['chrome_options']:
        chrome_options.add_argument(value)
    driver = webdriver.Chrome(options=chrome_options)
    links = ti.xcom_pull(task_ids='link_parser', key='links')
    coupons = dict()
    driver.get(links[0]) # Осуществляем переход, чтобы закрыть окно
    cookie_window_close(driver)
    count = len(links)
    for link in links:
        driver.get(link)
        time.sleep(0.5)
        isin, coupon = parser(link, driver)
        coupons[isin] = coupon
        count -=1
        print(f"Осталось ссылок {count}")
    ti.xcom_push(key='coupons', value=coupons)
    path = conf_options['output_dir'] + conf_options['coupon_output_file']
    json_serializing_data(data=coupons, filename=path,  mode="w")
