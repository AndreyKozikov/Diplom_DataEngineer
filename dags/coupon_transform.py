from utils import json_serializing_data
import csv

head = ['redemption_option', 'date', 'coupon_stake', 'money_coupon', 'nominal', 'redemption_price', 'currency', 'isin']

def coupon_transform_task(**kwargs):
    ti = kwargs['ti']
    conf_options = ti.xcom_pull(task_ids='config_file_load', key='config')
    path = conf_options['output_dir'] + conf_options['coupon_output_file']
    coupons = json_serializing_data(filename=path, mode="r")
    output = conf_options['output_dir'] + conf_options['coupon_csv_file']
    with open(output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(head)
        for isin, data in coupons.items():
            columns_values = list()
            literal = ""
            data_split = data.split("\n")[4:-2]
            for value in data_split:
                if len(value.split(" ")) > 1:
                    if literal == "":
                        columns_values.append("-")
                        columns_values.extend(value.split(" "))
                        columns_values.append(isin)
                    else:
                        columns_values.append(literal)
                        columns_values.extend(value.split(" "))
                        columns_values.append(isin)
                        literal = ""
                    writer.writerow(columns_values)
                    columns_values = list()
                else:
                    literal = value