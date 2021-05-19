from setuptools import setup, find_packages

setup(
    name="KommatiPara",
    description='Project for processing financial data from (UK and NL) clients.',
    version='1.0.0',
    author='Milos Damjanovic',
    author_email='mdamjanovic2012@gmail.com',

    python_requires='==3.7.*',
    install_requires=[
        'setuptools',
        'wheel',
        'flake8',
        'pytest-spark',
        'pyspark',
        'pytest-cov',
        'Pandas >= 0.23.2'
    ],

    packages=find_packages(),
)
