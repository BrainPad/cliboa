#!/usr/bin/env python
#
# Copyright BrainPad Inc. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# Always prefer setuptools over distutils
from io import open
from os import path

from setuptools import setup


def read(filename):
    with open(path.join(path.dirname(__file__), filename), encoding="utf-8") as f:
        return f.read()


setup(
    name="cliboa",
    version="1.3.7b0",
    description="application framework for ETL(ELT) processing",
    long_description=read("README.md"),
    long_description_content_type='text/markdown',
    url="https://github.com/BrainPad/cliboa",  # Optional
    author="BrainPad",
    # author_email='brainpad.co.jp',
    classifiers=[
        # How mature is this project? Common values are
        #   4 - Beta
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    packages=[
        "cliboa",
        "cliboa.adapter",
        "cliboa.cli",
        "cliboa.common",
        "cliboa.conf",
        "cliboa.core",
        "cliboa.scenario",
        "cliboa.util",
        "cliboa.scenario.extract",
        "cliboa.scenario.transform",
        "cliboa.scenario.load",
        "cliboa.template",
        "cliboa.template.bin",
    ],
    package_data={
        "cliboa.conf": ["logging.conf", "cliboa.ini"],
        "cliboa.template": [
            "Pipfile.above35",
            "Pipfile.above36",
            "Pipfile.above37",
            "requirements.above35.txt",
            "requirements.above36.txt",
            "requirements.above37.txt",
        ],
    },
    python_requires=">=3.5",
    entry_points={"console_scripts": ["cliboadmin = cliboa.cli.cliboadmin:main"]},
)
