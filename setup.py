# -*- coding: utf-8

import os

from setuptools import find_packages, setup

package_data = []

proj_dir = os.path.dirname(__file__)
templates = os.path.join(proj_dir, 'resources', 'templates')

for root, dirs, names in os.walk(templates):
    for fname in names:
        abspath = os.path.join(root, fname)
        relpath = os.path.relpath(proj_dir, abspath)
        package_data.append(relpath)
        print(relpath)

here = os.path.abspath(os.path.dirname(__file__))
# Get __version__ variable
exec(open(os.path.join(here, 'pfio', 'version.py')).read())

with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='pfio',
    version=__version__,
    description='PFN IO library',
    author='Tianqi Xu, Kota Uenishi',
    author_email='tianqi@preferred.jp, kota@preferred.jp',
    url='http://github.com/pfnet/pfio',
    classifiers=[
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Developers',

        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Operating System :: POSIX :: Linux',

        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',

        'Topic :: System :: Filesystems',
    ],
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    package_data={'pfio': package_data},
    extras_require={
        'test': ['pytest', 'flake8', 'autopep8', 'parameterized', 'isort', 'moto'],
        # When updating doc deps, docs/requirements.txt should be updated too
        'doc': ['sphinx', 'sphinx_rtd_theme'],
        'bench': ['numpy>=1.19.5', 'torch>=1.9.0', 'Pillow<=8.2.0'],
    },
    python_requires=">=3.6",
    # When updating install requires, docs/requirements.txt should be updated too
    install_requires=['pyarrow>=6.0.0', 'boto3', 'deprecation'],
    include_package_data=True,
    zip_safe=False,

    keywords='filesystem hdfs chainer development',
)
