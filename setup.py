#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-google-ads",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_google_ads"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python==5.12.2",
        "google-ads==15.0.0"
    ],
    entry_points="""
    [console_scripts]
    tap-google-ads=tap_google_ads:main
    """,
    packages=["tap_google_ads"],
    package_data = {
        "schemas": ["tap_google_ads/schemas/*.json"]
    },
    include_package_data=True,
)
