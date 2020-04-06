import pandas as pd
import numpy as np
from random import sample, shuffle, randint
from loremipsum import generate_sentence
from math import ceil

path_reviewed_by = r'../data/reviewed_by.csv' # path to reviewed_by.csv
path_reviewed_by_decision = r'../data/reviewed_by_decision.csv' # path to write the reviewed_by file

row_num = 10_000 # number of rows to read


reviewed_by = pd.read_csv(path_reviewed_by, names=['articleID','reviewerID'], header=0, index_col=0,
                            nrows=row_num, sep=';')
# generating reviews and decisions
syn_reviews = [[generate_sentence()[2], generate_sentence()[2], generate_sentence()[2]] for j in range(row_num)]
decisions = ['accept', 'accept', 'reject']

reviews_ = []
for idx, artID in enumerate(reviewed_by.index.to_list()):
    # for each article randomly assigning 3 reviewer from authors of other articles
    reviewers = reviewed_by.loc[artID]['reviewerID'].split('|')
    reviews = syn_reviews[idx] # synthetic reviews
    shuffle(decisions) # random decisions

    # magic happens here
    review_info = ['|'.join([reviewers[i], reviews[i], decisions[i]]) for i in range(3)]
    review_info.insert(0, artID)

    reviews_.append(review_info)


# joining into a final dataframe
reviews_df = pd.DataFrame().from_records(reviews_, columns=["articleID", "review1", "review2", "review3"])

# writing to a csv-file
reviews_df.to_csv(path_reviewed_by_decision, sep=';', index=False)