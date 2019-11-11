#!/usr/bin/env python
#
# Copyright 2019 BrainPad Inc. All Rights Reserved.
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
import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
from os import path
from io import open

def read(filename):
    with open(path.join(path.dirname(__file__), filename), encoding="utf-8") as f:
        return f.read()

setup(
    name="cliboa",
    version="1.2.0beta",
    description="application framework for etl(extract, transform, load) processing",
    #long_description=read("README.md"),
    #long_description_content_type='text/markdown',
    url="https://github.com/BrainPad/cliboa",  # Optional
    author="BrainPad",
    # author_email='brainpad.co.jp',
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    # unit test
    setup_requires=["pytest-runner"],
    tests_require=["pytest", "pytest-cov"],
    packages=[
        "cliboa",
        "cliboa.cli",
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
    package_data={"cliboa.template": ["requirements.txt"]},
    python_requires=">=3.0",
    install_requires=["PyYaml==3.13"],
    entry_points={"console_scripts": ["cliboadmin = cliboa.cli.cliboadmin:main"]},
)
