import pandas as pd
from db_connection_mongo import *

# this code isn't used for the assignment, but just used since
# I was too lazy to input each document one by one

if __name__ == '__main__':
    db = connectDataBase()
    documents = db.documents

    data = pd.read_csv('documents.csv')
    print(data)
    for row in data.to_dict('records'):
        createDocument(documents, **row)
