import pandas as pd
import numpy as np
from random import sample
from loremipsum import generate_sentence

path_article = r'../data/articles_journals.csv' # path to journal articles
path_article_ = r'../data/articles_conferences.csv' # path to conference articles
path_authored_by = r'../data/authored_by.csv' # path to file output_author_authored_by
path_reviewed_by = r'../data/reviewed_by_decision_by_property.csv' # path to write the reviewed_by file

row_num = 100_000 # number of rows to read

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
                            header=0,
                            nrows=row_num,
                            sep=';')
# for each article randomly assigning 3 reviewer from authors of other articles
reviewed_by = [(i,'|'.join(list(map(lambda x: str(x),sample(list(authored_by.loc[authored_by.articleID!=i].authorID.unique()), 3))))) for i in articles]
reviewed_by_df = pd.DataFrame.from_records(reviewed_by,
                                            columns=['articleID','reviewerID']) # converting into Dataframe

# generating reviews and decisions
syn_reviews = ['|'.join([generate_sentence()[2], generate_sentence()[2], generate_sentence()[2]]) for j in range(row_num)]
decisions = ['accept','accept','reject']
syn_decisions = ['|'.join(sample(decisions, len(decisions))) for i in range(row_num)]

# dataframe for reviews and decisions
rev_dec_df = pd.DataFrame.from_dict({'reviews':syn_reviews, 'decisions':syn_decisions})

# joining into a final dataframe
reviewed_by_df.reset_index(drop=True, inplace=True)
rev_dec_df.reset_index(drop=True, inplace=True)

final_df = reviewed_by_df.join(rev_dec_df)

# writing to a csv-file
final_df.to_csv(path_reviewed_by, sep=';', index=False)
