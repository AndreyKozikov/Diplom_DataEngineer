import json
from selenium import webdriver
from selenium.common import TimeoutException, ElementClickInterceptedException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy_utils import database_exists, create_database


def json_serializing_data(data="", filename="output.txt", mode="none"):
    if mode == "w":
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    elif mode == "r":
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)


def cookie_window_close(driver):
    try:
        cookie_close_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".cookiemsg__bottom_close"))
        )
        if cookie_close_button.is_displayed():
            cookie_close_button.click()
            print("Закрыли окно сообщений о куках")
    except TimeoutException:
        print("Cookie message button not found or already closed.")


def check_db_connect(engine, database):
    # Проверяем доступность базы данных
    try:
        with engine.connect() as connection:
            print(f"База данных '{database}' существует и доступна.")
    except Exception as e:
        # Проверяем, существует ли база данных
        if not database_exists(engine.url):
            try:
                create_database(engine.url)
                print(f"База данных '{database}' успешно создана.")
            except Exception as create_error:
                raise AirflowException(f"Не удалось создать базу данных: {create_error}")
        else:
            raise AirflowException("База данных существует, но недоступна. Проверьте параметры подключения.")
