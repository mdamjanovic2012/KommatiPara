import os
import sys
import logging
from logging.handlers import RotatingFileHandler

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Add rotating log
logger = logging.getLogger("KommatiPara Log")
logger.setLevel(logging.DEBUG)
handler = RotatingFileHandler("logs/kommatipara.log", maxBytes=20, backupCount=5)
logger.addHandler(handler)

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
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

    logger.debug(f"Data to be renamed: {data_to_rename}")
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

    logger.debug(f"Personal data to be removed: {personal_info}")
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

    logger.debug(f"Filtering countries: {countries}")
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

    Gets arguments from command line:
      clients_file_path clients_financials_file countries_to_filter

    Example
    C:/.../kommatipara/dataset_one.csv C:/.../kommatipara/dataset_two.csv "[United Kingdom, Netherlands]"
    """
    try:
        client_file_path = sys.argv[1]
        financial_details_file_path = sys.argv[2]

        countries_to_filter = sys.argv[3][1:-1].split(',')
        countries_to_filter = [_.strip() for _ in countries_to_filter]  # Removing whitespaces from data
    except Exception:  # Rollback to default values
        logger.debug(f"Invalid or no parameters passed")
        client_file_path = os.path.join(BASE_PATH, 'dataset_one.csv')
        financial_details_file_path = os.path.join(BASE_PATH, 'dataset_two.csv')
        countries_to_filter = ['United Kingdom', 'Netherlands']

    clients = spark.read.csv(client_file_path, header=True)
    clients = __filter_data_per_country(data=clients, countries=countries_to_filter)

    financial_details = spark.read.csv(financial_details_file_path, header=True)
    full_clients_data = clients.join(financial_details, on=['id'], how='inner')

    full_clients_data = __remove_personal_info(data=full_clients_data)
    full_clients_data = __rename_data_columns(data=full_clients_data)

    __save_output_file(full_clients_data)


if __name__ == '__main__':
    execute()
