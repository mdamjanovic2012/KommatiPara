from pyspark.sql import SparkSession
import os

from pyspark.sql.functions import col

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
client_file_path = os.path.join(BASE_PATH, 'dataset_one.csv')
financial_details_file_path = os.path.join(BASE_PATH, 'dataset_two.csv')
output_file = os.path.join(BASE_PATH, 'client_data', 'output_client_data.csv')

spark = SparkSession \
    .builder \
    .appName('KommatiPara') \
    .getOrCreate()


def __rename_data_columns(data, data_to_rename=None):
    if data_to_rename is None:
        data_to_rename = [('id', 'client_identifier'), ('btc_a', 'bitcoin_address'), ('cc_t', 'credit_card_type')]

    for dtr in data_to_rename:
        data = data.withColumnRenamed(dtr[0], dtr[1])

    return data


def __remove_personal_info(data, personal_info=None):
    if personal_info is None:
        personal_info = ['first_name', 'last_name', 'cc_n']

    return data.drop(*personal_info)


def __filter_data_per_country(data, countries=None):
    if countries is None:
        countries = ['United Kingdom', 'Netherlands']
    return data.filter(col('country').isin(countries))


def __save_output_file(data):
    if os.path.exists(output_file):  # to_csv doesnt have an overwrite mode
        os.remove(output_file)

    data.toPandas().to_csv(output_file, encoding='utf-8', index=False)
    spark.stop()


def execute():
    clients = spark.read.csv(client_file_path, header=True)
    clients = __filter_data_per_country(data=clients)

    financial_details = spark.read.csv(financial_details_file_path, header=True)
    full_clients_data = clients.join(financial_details, on=['id'], how='inner')

    full_clients_data = __remove_personal_info(data=full_clients_data)
    # Rename columns
    full_clients_data = __rename_data_columns(data=full_clients_data)
    print(output_file)
    full_clients_data.show()
    __save_output_file(full_clients_data)


if __name__ == '__main__':
    execute()
