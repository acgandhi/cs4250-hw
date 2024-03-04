#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #1
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

import re   # just to remove spaces and punctuation

import psycopg2
from psycopg2.extras import execute_values


# \s: space, \W non-alpha, _: _
puncSpacesRe = re.compile('[\s\W_]+')
puncRe = re.compile('[^\w\s]+')


def connectDataBase():
    # Create a database connection object using psycopg2
    DB_NAME = "cs4250"
    DB_USER = "postgres"
    DB_PASS = "73RT0eT4EZSkIvqi"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    try:
        conn = psycopg2.connect(database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASS,
                                host=DB_HOST,
                                port=DB_PORT)
        return conn
    except Exception as e:
        print(e)
        print("Database not connected successfully")


def createCategory(cur, catId, catName):
    # Insert a category in the database
    cur.execute(
        "INSERT INTO categories (id_cat, name) VALUES (%s, %s)",
        (catId, catName)
    )


def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    cur.execute(
        "SELECT id_cat FROM categories WHERE name = %s",
        (docCat, )
    )
    catId: int = cur.fetchone()[0]

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    numChars = len(puncSpacesRe.sub('', docText))
    cur.execute(
        "INSERT INTO documents (doc_number, text, title, num_chars, date, id_cat) VALUES (%s, %s, %s, %s, %s, %s)",
        (docId, docText, docTitle, numChars, docDate, catId)
    )

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document

    words = puncRe.sub('', docText).lower().split(' ')
    # find terms and counts
    docTerms: dict[str, int] = {}
    for word in words:
        docTerms[word] = docTerms.get(word, 0) + 1

    # Create new terms
    cur.execute(
        "SELECT term FROM terms"
    )
    terms = set([x[0] for x in cur.fetchall()])
    newTerms = docTerms.keys() - terms
    execute_values(
        cur,
        "INSERT INTO terms (term, num_chars) VALUES %s",
        [(term, len(term)) for term in newTerms]
    )

    # 4.3 Insert the term and its corresponding count into the database
    execute_values(
        cur,
        "INSERT INTO document_terms (doc_number, term, count) VALUES %s",
        [(docId, term, numChars) for term, numChars in docTerms.items()]
    )


def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.

    # delete all terms for the document
    cur.execute(
        "DELETE FROM document_terms WHERE doc_number = %s",
        (docId,)
    )

    # delete terms if they don't exist in document_terms (no longer exist in any document)
    cur.execute(
        """ DELETE FROM terms WHERE NOT EXISTS (
                SELECT 1 FROM document_terms WHERE document_terms.term = terms.term
        )"""
    )

    # 2 Delete the document from the database
    cur.execute(
        "DELETE FROM documents WHERE doc_number = %s",
        (docId,)
    )


def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # term, DocumentTitle, count
    cur.execute(
        "SELECT term, d.title, count FROM document_terms INNER JOIN documents as d on d.doc_number = document_terms.doc_number"
    )
    documentTerms: list[tuple[str, str, int]] = cur.fetchall()
    
    index = {}
    # similar to reduce in map-reduce?
    for term in documentTerms:
        index[term[0]] = index.get(term[0], '') + f"{term[1]}:{term[2]},"

    # remove trailing comma
    for term in index:
        index[term] = index[term][:-1]

    return index
