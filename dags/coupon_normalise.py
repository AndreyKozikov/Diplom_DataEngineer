from utils import check_db_connect
import pandas as pd
from sqlalchemy import create_engine

def coupon_normalise_task(**kwargs):
    ti = kwargs['ti']
    conf_options = ti.xcom_pull(task_ids='config_file_load', key='config')
    username = conf_options['db_user']
    password = conf_options['db_password']
    host = conf_options['db_host']
    port = conf_options['db_port']
    database = conf_options['db_database']
    input_file = conf_options['output_dir'] + conf_options['coupon_csv_file']
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'
    engine = create_engine(connection_string)
    check_db_connect(engine, database)
    df = pd.read_csv(input_file)
    replace_dict = {'-': 0, 'MAT': 1, 'PUT': 2}
    df['redemption_option'] = df['redemption_option'].replace(replace_dict)
    df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y')
    df['coupon_stake'] = df['coupon_stake'].str.replace('%', '', regex=False)
    df[['coupon_stake', 'nominal', 'redemption_price', 'money_coupon']] = df[
        ['coupon_stake', 'nominal', 'redemption_price', 'money_coupon']].replace('-', 0, regex=False)
    df[['coupon_stake', 'money_coupon', 'nominal']] = df[['coupon_stake', 'money_coupon', 'nominal']].astype(float)
    df['redemption_price'] = df['redemption_price'].astype(int)
    df[['currency', 'isin']] = df[['currency', 'isin']].astype('string')

    df.to_sql('coupons', engine, if_exists='replace', index=False)
