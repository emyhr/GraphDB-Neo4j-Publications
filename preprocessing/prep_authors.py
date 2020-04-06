import pandas as pd

# Number of authors
N=10000
# Number of authorships
M=500000

# Read input files
authors = pd.read_csv (r'../authors.csv', sep=';', header=0, dtype='unicode')
# authored_by = pd.read_csv (r'../output_author_authored_by.csv', sep=';', header=None, dtype='unicode')

# Write
authors[:N].to_csv(r'../data/authors.csv',header=True, index=False)
# authored_by[:M].to_csv(r'../data/authored_by.csv',header=False, index=False)