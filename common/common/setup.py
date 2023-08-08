#!/usr/bin/python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


def read_requirements():
    with open('requirements.txt', 'r') as f:
        return f.readlines()


setup(
    name="brightfield_common",
    version="0.7",
    description="Common utility functions for Brightfield",
    license='MIT',
    author="Aviram",
    author_email="aviram@toptal.com",
    url="https://github.com/brightfieldgroup/common",
    python_requires=">=3.5",
    install_requires=read_requirements(),
    packages=find_packages()
)
