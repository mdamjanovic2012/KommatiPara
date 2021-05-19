# PySpark KommatiPara

## About the project:
Simple data processing project that merges two datasets (clients and their financial dataset) into one
dataset filtered by clients needs.

## Installation:
`pip install -r requirements.txt` or `pip install .`

## Execution:
Project expects arguments to be passed (if not passed or passed incorectly it will go to default values) 


`python main.py clients_file_path clients_financials_file countries_to_filter`

    Example
    python main.py C:/.../kommatipara/dataset_one.csv C:/.../kommatipara/dataset_two.csv "[United Kingdom, Netherlands]"
## Create source distribution file:
`python setup.py sdist`

## Logs:

  Logs are stored in `logs` folder. 

## Authors

* **Milos Damjanovic** - (https://github.com/mdamjanovic2012)
