# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 08:03:00 2023

@author: Calixte
"""

from os import listdir
from os.path import isfile, join

mypath = r'de-dwh-dags/airflow2/coredwh'

from os import walk
from os.path import isfile
import pandas as pd

paths =[]
files = []

for (dirpath, dirnames, filenames) in walk(mypath):
    for file in filenames:
        file_to_test = str(dirpath) + r'/' + str(file)
        if isfile(file_to_test):
            paths.append(dirpath)
            files.append(file)

intermediate = list(zip(paths,files))

df = pd.DataFrame(intermediate, columns=['path','files'])

df['directory'] = df['path'].str.split("/", expand = False)

#df['file_count'] = df.files.apply(lambda x: len(x))
 
df = df.explode('directory')

df.to_csv('all_files.csv')

