# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 08:03:00 2023

@author: Calixte
"""

import os
from os.path import isfile, join

path = r'C:\Users\Calixte\Desktop\GitHub\general_helpers'

for (dirpath, dirnames, filenames) in os.walk(path):
    print(dirnames)
    print(filenames)
    print([os.path.isfile(join(path, f)) for f in os.listdir(path)])
    break
