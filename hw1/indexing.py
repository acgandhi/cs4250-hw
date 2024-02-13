#-------------------------------------------------------------------------
# AUTHOR: Amar Gandhi
# FILENAME: indexing.py
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #1
# TIME SPENT: <1 hour (code), ~2-3 hours for assignment (partially)
#-----------------------------------------------------------*/

# IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with standard arrays
# nooooooooooo :(

# Importing some Python libraries
import csv
from math import log

documents = []

# Reading the data in a csv file
with open('collection.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    for i, row in enumerate(reader):
        if i > 0:  # skipping the header
            documents.append(row[0].lower().split(' '))

print(documents)

# Conducting stopword removal. Hint: use a set to define your stopwords.
stopWords = {'i', 'she', 'her', 'and', 'they', 'their'}
for doc in documents:
    for word in doc:
        if word in stopWords:
            doc.remove(word)

print(documents)

# Conducting stemming. Hint: use a dictionary to map word variations to their stem.
wordToStem = {
    'cats': 'cat',
    'loves': 'love',
    'dogs': 'dog'
}
# words not in the keys of wordToStem are kept
for doc in documents:
    for i, word in enumerate(doc):
        doc[i] = wordToStem[word] if word in wordToStem else word

# Identifying the index terms.
terms = set()
for doc in documents:
    terms |= set(doc)   # union unique terms with uniques from each doc

terms = list(terms)     # we need this to be ordered so we can do MATH
nDocs = len(documents)
nTerms = len(terms)

# Building the document-term matrix by using the tf-idf weights.
# this could be like 4 lines with numpy :(((
docToOccurrences: list[dict] = [dict.fromkeys(terms, 0) for _ in range(len(documents))]
for i, doc in enumerate(documents):
    for word in doc:
        docToOccurrences[i][word] += 1

termFreq: list[dict] = [dict.fromkeys(terms, 0) for _ in range(len(documents))]

for i, doc in enumerate(documents):
    docLen = len(doc)
    for word in doc:
        termFreq[i][word] = docToOccurrences[i][word] / docLen

df = dict.fromkeys(terms, 0)

for word in terms:
    for i in range(len(documents)):
        if termFreq[i][word] > 0:
            df[word] += 1

idf = {word: log(nDocs/df[word], 10) for word in terms}

tf_idf: list[list] = [[0 for _ in range(nTerms)] for _ in range(nDocs)]

for r, tf in enumerate(termFreq):
    for c, word in enumerate(terms):
        tf_idf[r][c] = tf[word]*idf[word]

# Printing the document-term matrix.
# sketchy hand formatting
print("   TF-IDF   ")
print('        ', end='')
print('           '.join(terms))
for r, row in enumerate(tf_idf):
    print(f'd{r+1}        ', end='')
    for c, col in enumerate(row):
        print('{:.4f}'.format(tf_idf[r][c]), '        ', end='')
    print()
