#!/usr/bin/env python
import os
import os.path
import zipfile

from six.moves.urllib import request

request.urlretrieve(
    'https://nlp.stanford.edu/sentiment/trainDevTestTrees_PTB.zip',
    'trainDevTestTrees_PTB.zip')
zf = zipfile.ZipFile('trainDevTestTrees_PTB.zip')
for name in zf.namelist():
    (dirname, filename) = os.path.split(name)
    if not filename == '':
        zf.extract(name, '.')
