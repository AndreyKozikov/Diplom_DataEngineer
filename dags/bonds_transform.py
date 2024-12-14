import csv
from utils import json_serializing_data

colums = ['Эмитент', 'Сектор рынка', 'Краткое название', 'ISIN', 'Валюта', 'Объем в обращении, млрд.',
          'Минимальный лот', 'Статус', 'Дата выпуска', 'Дата погашения', 'Цена (last/bid/ask)', 'НКД',
          'Текущий номинал', 'Купон', 'Купон (раз/год)', 'Тип купона', 'Тип инструмента', 'Амортизируемые',
          'Cубординированные', 'С гарантией', 'High Yield (ВДО)', 'Доступно для ИИС', 'Льгота по времени владения']

head = ['issuer', 'sector', 'short_name', 'isin', 'currency', 'volume_circulation',
        'min_lot', 'status', 'issue_date', 'maturity_date', 'price', 'accumulated_coupon',
        'denomination', 'сoupon', 'сoupon_once_year', 'сoupon_type', 'instrument_type', 'amort',
        'subord', 'guarant', 'high_yield', 'available_iis', 'hold_benefit']

def bonds_transform_task(**kwargs):
    ti = kwargs['ti']
    conf_options = ti.xcom_pull(task_ids='config_file_load', key='config')
    path = conf_options['output_dir'] + conf_options['description_output_file']
    description = json_serializing_data(filename=path, mode="r")
    output = conf_options['output_dir'] + conf_options['description_csv_file']
    with open(output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(head)
        for isin, data in description.items():
            parse = list()
            bonds_data = list()
            parse.extend((data['main']).split('\n'))
            parse.extend((data['portfolio']).split('\n'))
            parse.extend((data['tax']).split('\n'))
            for column in colums:
                try:
                    index = parse.index(column)
                    bonds_data.append(parse[index + 1])
                except ValueError:
                    bonds_data.append('NAN')
            writer.writerow(bonds_data)