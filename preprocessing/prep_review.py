import pandas as pd
import numpy as np
from random import sample

path_article = r'../data/articles_journals.csv'  # path to journal articles
path_article_ = r'../data/articles_conferences.csv'  # path to conference articles
path_author = r'../data/authors.csv'  # path to csv with authors
path_authored_by = r'../data/authored_by.csv'  # path to file output_author_authored_by
path_reviewed_by = r'../data/reviewed_by.csv'  # path to write the reviewed_by file

row_num = 10000 # number of rows to read

# reading articleIDs from both files
articles = pd.read_csv(path_article,
                        usecols=[0],
                        index_col=0,
                        header=0,
                        nrows=row_num/2,
                        sep=';').index.to_list() + pd.read_csv(path_article_,
                                                                usecols=[0],
                                                                index_col=0,
                                                                header=0,
                                                                nrows=row_num/2).index.to_list()

# author-article relation
authored_by = pd.read_csv(path_authored_by,
                            names=['articleID','authorID'],
                            header=None,
                            nrows=row_num,
                            sep=',')
# for each article randomly assigning 3 reviewer from authors of other articles
reviewed_by = [(i,'|'.join(list(map(lambda x: str(x),sample(list(authored_by.loc[authored_by.articleID!=i].authorID.unique()), 3))))) for i in articles]
reviewed_by_df = pd.DataFrame.from_records(reviewed_by,
                                            index='articleID',
                                            columns=['articleID','reviewerID']) # converting into Dataframe

# writing to a csv-file
reviewed_by_df.to_csv(path_reviewed_by, sep=';')