from utils import check_db_connect
import pandas as pd
from sqlalchemy import create_engine


def description_normalise_task(**kwargs):
    ti = kwargs['ti']
    conf_options = ti.xcom_pull(task_ids='config_file_load', key='config')
    username = conf_options['db_user']
    password = conf_options['db_password']
    host = conf_options['db_host']
    port = conf_options['db_port']
    database = conf_options['db_database']
    input_file = conf_options['output_dir'] + conf_options['description_csv_file']
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'
    engine = create_engine(connection_string)
    check_db_connect(engine, database)
    df = pd.read_csv(input_file)
    df['сoupon'] = df['сoupon'].replace('nan', 0)
    replacement_dict = {'фиксированный': 2, 'плавающий': 1, 'нулевой (дисконтная)': 0, 'изменяемый': 3}
    df['сoupon_type'] = df['сoupon_type'].replace(replacement_dict)
    df[['volume_circulation', 'volume_circulation_currency']] = df['volume_circulation'].str.split(" ", expand=True)
    df[['сoupon', 'coupon_currency']] = df['сoupon'].str.split(" ", expand=True)
    df[['amort', 'subord', 'guarant', 'high_yield', 'available_iis', 'hold_benefit']] = df[
        ['amort', 'subord', 'guarant', 'high_yield', 'available_iis', 'hold_benefit']].replace(
        {"NAN": 0, "нет": 0, "да": 1})
    df[['issuer', 'sector', 'short_name', 'status', 'isin', 'currency', 'instrument_type',
        'volume_circulation_currency', 'coupon_currency']] = df[
        ['issuer', 'sector', 'short_name', 'status', 'isin', 'currency', 'instrument_type',
         'volume_circulation_currency', 'coupon_currency']].astype('string')
    df['issue_date'] = pd.to_datetime(df['issue_date'], format='%d.%m.%Y')
    df['maturity_date'] = pd.to_datetime(df['maturity_date'], format='%d.%m.%Y')
    df[['volume_circulation', 'сoupon']] = df[['volume_circulation', 'сoupon']].astype('float')

    df.to_sql('bonds', engine, if_exists='replace', index=False)