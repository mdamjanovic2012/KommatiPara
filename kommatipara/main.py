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
    """
   Renames columns from dataset.

   :param dataset data:
   :param list data_to_rename: list of columns to be renamed in data
   :return: dataset with renamed columns
   """
    if data_to_rename is None:
        data_to_rename = [('id', 'client_identifier'), ('btc_a', 'bitcoin_address'), ('cc_t', 'credit_card_type')]

    for dtr in data_to_rename:
        data = data.withColumnRenamed(dtr[0], dtr[1])

    return data


def __remove_personal_info(data, personal_info=None):
    """
   Removes columns from dataset.

   :param dataset data:
   :param list personal_info: list of columns to be removed from data
   :return: dataset with removed columns
   """
    if personal_info is None:
        personal_info = ['first_name', 'last_name', 'cc_n']

    return data.drop(*personal_info)


def __filter_data_per_country(data, countries=None):
    """
    Filters dataset by countries.

    :param dataset data: dataset to be filtered
    :param list countries: list of countries that dataset will be filtered by
    :return: Filtered dataset
    """
    if countries is None:
        countries = ['United Kingdom', 'Netherlands']
    return data.filter(col('country').isin(countries))


def __save_output_file(data):
    """
    Saving dataset to output folder. Overwrites file if exists.

    :param dataset data: dataset to be saved
    :return: void
    """
    if os.path.exists(output_file):
        os.remove(output_file)

    data.toPandas().to_csv(output_file, encoding='utf-8', index=False)
    spark.stop()


def execute():
    """
    Main function that executes whole logic.
    """
    clients = spark.read.csv(client_file_path, header=True)
    clients = __filter_data_per_country(data=clients)

    financial_details = spark.read.csv(financial_details_file_path, header=True)
    full_clients_data = clients.join(financial_details, on=['id'], how='inner')

    full_clients_data = __remove_personal_info(data=full_clients_data)
    full_clients_data = __rename_data_columns(data=full_clients_data)

    __save_output_file(full_clients_data)


if __name__ == '__main__':
    execute()
