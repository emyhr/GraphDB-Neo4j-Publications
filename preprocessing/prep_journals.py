import pandas as pd
import numpy as np
from loremipsum import generate_sentence

row_num=50000 # number of rows to read
path = r'../output_article.csv'  # path to csv with articles


# reading rows from output_article.csv
data = pd.read_csv(path,
                    sep=';',
                    names=['articleID','doi','journal','pages','title','volume','year'],
                    usecols=[0,12,15,22,27,31,32], dtype={'volume':'str'}, nrows=row_num,
                    header=None, index_col=0)

# we are not considering articles not published in any journal
data.dropna(how='all', subset=['journal','volume'], inplace=True)
data.drop_duplicates(inplace=True)

# ---------------------------------generating synthetic data----------------------------------#

idx = data[data.journal.notnull()][data.volume.isnull()].index # row indices where volume=null
null_doi_num = data.doi.isnull().sum() # number of rows with doi=null
null_pages_num = data.pages.isnull().sum() # number of rows with pages=null

syn_volume = [str(np.random.randint(11, 1000)) for z in range(len(idx))] # casting to str is needed
                                                                         # for later concatenation
syn_doi = [generate_sentence()[2] for j in range(null_doi_num)]
syn_abstract = [generate_sentence()[2] for j in range(row_num)]
syn_pages = np.random.randint(50, 300, null_pages_num)

kws = np.array(pd.read_csv(r'../data/keyword_topic.csv',
                           names=['keyword'],
                           usecols=[0],
                           index_col=0).index.to_list()) # loading list of keywords from csv
# creating str 'kw1|kw2' where kw1 and kw2 are randomly chosen from kw
syn_kws = ['|'.join(kws[[np.random.randint(0, len(kws), 2)]]) for x in range(row_num)]

#--------------------------------------------------------------------------------------------#

# assigning synthetic data to dataframe
data.loc[data.doi.isnull(),'doi'] = syn_doi
data.loc[data.pages.isnull(),'pages'] = syn_pages
data.loc[idx, 'volume']=syn_volume
data['volumeID'] = data.journal + '-' + data.volume # creating volumeIDs
data['keywords'] = syn_kws
data['abstract'] = syn_abstract

# writing to a csv-file
data.to_csv(r'../data/articles_journals.csv', sep=';')