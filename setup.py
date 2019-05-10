# -*- coding: utf-8

import os
from setuptools import setup
from setuptools import find_packages

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
exec(open(os.path.join(here, 'chainerio', 'version.py')).read())

setup(
    name='chainerio',
    version=__version__,
    description='Chainer IO library',
    author='Tianqi Xu',
    author_email='tianqi@preferred.jp',
    url='http://github.com/pfnet/chainerio',
    classifiers=[],
    packages=find_packages(),
    package_data={'chainerio' : package_data},
    extras_require={'test':['pytest', 'flake8', 'autopep8']},
    python_requires=">=3.5",
    install_requires=['krbticket', 'pyarrow',
                      # Workaround until https://github.com/chainer/chainer/pull/4534 gets released
                      # See also: https://nvd.nist.gov/vuln/detail/CVE-2019-6446
                      'numpy<1.16.3,>1.16.0'],
    include_package_data=True,
    zip_safe=False,
)
