from setuptools import setup, find_packages

setup(
    name="cql_orm",
    version="0.1",
    packages=find_packages(),
    install_requires=["scylla-driver"],
)
