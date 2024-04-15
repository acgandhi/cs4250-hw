#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #3
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

# IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

# importing some Python libraries
import re   # just to remove spaces and punctuation
from datetime import datetime
import pymongo
from pymongo.collection import Collection

# \s: space, \W non-alpha, _: _
puncSpacesRe = re.compile('[\s\W_]+')
puncRe = re.compile('[^\w\s]+')

def connectDataBase():

    client = pymongo.MongoClient('localhost', 27018)
    db = client.cs4250
    return db


def createDocument(col: Collection, docId, docText, docTitle, docDate, docCat):

    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    words = puncRe.sub('', docText).lower().split(' ')
    # find terms and counts
    docTerms: dict[str, int] = {}
    for word in words:
        docTerms[word] = docTerms.get(word, 0) + 1

    # create a list of objects to include full term objects. [{"term", count, num_char}]
    terms = [
        { 'term': term, 'count': docTerms[term], 'num_char': len(term) }
        for term in docTerms
    ]

    # produce a final document as a dictionary including all the required document fields
    # only count letters for num chars
    numChars = len(puncSpacesRe.sub('', docText))
    # convert date from string to datetime
    date = datetime.strptime(docDate, '%Y-%m-%d')
    doc = {
        '_id': docId,
        'text': docText,
        'title': docTitle,
        'num_chars': numChars,
        'date': date,
        'category': docCat,
        'terms': terms
    }

    # insert the document
    col.insert_one(doc)

def deleteDocument(col: Collection, docId):

    # Delete the document from the database
    col.delete_one({'_id': docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    deleteDocument(col, docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)


def getIndex(col):
    pass

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...

