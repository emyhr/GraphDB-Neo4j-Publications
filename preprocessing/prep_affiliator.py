import pandas as pd
import numpy as np
from random import choice

path_uni = r'../data/Colleges_and_Universities.csv' # path to file with universities
path_comp = r'../data/companies.csv' # path to file with companies
path_author = r'../data/output_author.csv' # path to authors.csv
path_author_new = r'../data/authors.csv'  # path to write new authors.csv

row_num = 10000 # number of rows to read

# reading files
uni = pd.read_csv(path_uni, usecols=[4], names=['orgName'], header=0) # reading university names
comp = pd.read_csv(path_comp, usecols=[0], names=['orgName'], header=0) # reading company names
author = pd.read_csv(path_author, names=['authorID', 'authorName'], header=0, sep=';', nrows=row_num)

# adding org_type column for the node property
uni['orgType'] = ['university' for i in range(len(uni))]
comp['orgType'] = ['company' for i in range(len(comp))]

organisations = pd.concat([uni, comp]) # joining universities and companies
organisations.reset_index(drop=True, inplace=True) # to make indices unique again

# randomly affiliating authors with organisations
affiliations = organisations.loc[[choice(organisations.index.to_list()) for i in range(row_num)]]
affiliations.reset_index(drop=True, inplace=True) # for correct joining 

author_final = author.join(affiliations)

# writing to file
author_final.to_csv(path_author_new, sep=';', index=False, header=False)