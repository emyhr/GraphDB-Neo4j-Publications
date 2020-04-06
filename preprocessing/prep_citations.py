import pandas as pd
from random import sample

path_article = r'../data/articles_journals.csv'  # path to journal articles
path_article_ = r'../data/articles_conferences.csv'  # path to conference articles
path_cites = r'../data/citations.csv'  # path to write cites.csv

row_num = 100000 # number of rows to read

# extracting articleIDs from both journal article and conf article files
articles = pd.read_csv(path_article, usecols=[0],
                        index_col=0, header=0,
                        nrows=row_num/2, sep=';') + pd.read_csv(path_article_, usecols=[0],
                                                                index_col=0, header=0,
                                                                nrows=row_num/2)
# for each article randomly assigning 3 reviewer from authors of other articles
cites = [(i,'|'.join(list(map(lambda x: str(x), sample(list(articles.loc[articles.index!=i].index), 5))))) for i in articles.index.to_list()]
cites_df = pd.DataFrame.from_records(cites,
                                    index='articleID',
                                    columns=['articleID','cites']) # converting into Dataframe

# writing to a csv-file
cites_df.to_csv(path_cites, sep=';', header=True)