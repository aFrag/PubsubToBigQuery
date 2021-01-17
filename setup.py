import setuptools

REQUIRED_PACKAGES = [
    'google-cloud-storage==1.28.*',
    'dask[dataframe]==2.18.*',
    'gcsfs==0.6.*',
    'pandas==1.1.*',
    'bigquery-schema-generator==1.4'
]

PACKAGE_NAME = 'pubsub_to_bigquery'
PACKAGE_VERSION = '0.0.1'

setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description='Salesforce Pubsub to JSON',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)
