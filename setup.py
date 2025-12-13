import setuptools

setuptools.setup(
    name='procesamiento-dataflow',
    version='0.1.0',
    install_requires=[
        'apache-beam[gcp]',
        'confluent-kafka',
        'pymongo',
        'kafka-python' # Legacy support if needed
    ],
    packages=setuptools.find_packages(),
)
