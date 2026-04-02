#!/usr/bin/env python
from setuptools import find_packages, setup

setup(
    name="tap-sap-sales-service-cloud",
    version="0.0.1",
    description="Singer.io tap for extracting data from SAP Sales and Service Cloud OData v2 APIs",
    author="Singer Community",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_sap_sales_service_cloud"],
    install_requires=[
        "singer-python==6.3.0",
        "requests==2.32.5",
        "backoff==2.2.1",
        "python-dateutil==2.9.0.post0",
    ],
    extras_require={
        "dev": [
            "pylint",
            "parameterized",
        ]
    },
    entry_points="""
        [console_scripts]
        tap-sap-sales-service-cloud=tap_sap_sales_service_cloud:main
    """,
    packages=find_packages(),
    include_package_data=True,
)
