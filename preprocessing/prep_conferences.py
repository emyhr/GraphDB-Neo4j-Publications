import pandas as pd
import numpy as np

desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',12)

# Number of articles published in conferences
N=50000

# Read input files
inproceedings = pd.read_csv (r'../output_inproceedings.csv', sep=';', header=None, dtype='unicode')

# Select relevant columns
new_inproceedings = inproceedings.iloc[:,[0,4,24,27,28]]
new_inproceedings.columns = ['articleID', 'conferenceID', 'title', 'doi', 'year']
new_inproceedings = new_inproceedings[:N]

# Create edition = year + conferenceID
editions = new_inproceedings['year'] + " " + new_inproceedings['conferenceID'].astype(str)

# Create abstract randomly
abstracts = pd.util.testing.rands_array(100, len(new_inproceedings))
df_abstracts = pd.DataFrame(abstracts)

# Random keywords-topics
keyword_topic = pd.DataFrame(columns=['keyword', 'topic'],
                             data=[['data management', 'Computer Science'],
                                    ['indexing', 'Computer Science'],
                                    ['data modeling','Computer Science'],
                                    ['big data','Computer Science'],
                                    ['data processing','Computer Science'],
                                    ['data storage','Computer Science'],
                                    ['data querying','Computer Science'],
                                    ['hadoop','Computer Science'],
                                    ['Amplitude','Physics'],
                                    ['Quantum','Physics']])
keyword_topic.to_csv(r'../data/keyword_topic.csv',header=False, index=False)

# Random keywords
terms = keyword_topic['keyword']
k1 = terms.sample(len(new_inproceedings), replace=True)
k1.reset_index(drop=True, inplace=True)
k2 = terms.sample(len(new_inproceedings), replace=True)
k2.reset_index(drop=True, inplace=True)
keywords = k1 + "|" + k2.astype(str)

# Concat to form the final dataframe
final_df = pd.concat([new_inproceedings, df_abstracts, editions, keywords], axis=1)

# sorting by articleID
final_df.sort_values("articleID", inplace=True)

# dropping ALL duplicte values
final_df.drop_duplicates(subset="articleID",keep='first', inplace=True)

# Write to file
final_df.columns = ['articleID', 'conferenceID', 'title', 'doi', 'year', 'abstract', 'edition', 'keywords']
final_df.to_csv(r'../data/articles_conferences.csv',header=True, index=False)