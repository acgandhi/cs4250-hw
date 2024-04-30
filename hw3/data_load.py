# import pandas as pd
from db_connection_mongo import *

# this code isn't used for the assignment, but just used since
# I was too lazy to input each document one by one

data = [{'docId': 1, 'docText': 'Baseball is played during summer months.', 'docTitle': 'Exercise', 'docDate': '2023-10-03', 'docCat': 'Sports'}, {'docId': 2, 'docText': 'Summer is the time for picnics here. Picnics time!', 'docTitle': 'California', 'docDate': '2023-10-03', 'docCat': 'Sports'}, {'docId': 3, 'docText': 'Months, months, months later we found out why', 'docTitle': 'Discovery', 'docDate': '2023-10-03', 'docCat': 'Seasons'}, {'docId': 4, 'docText': 'Why is summer so hot here? So hot!', 'docTitle': 'Arizona', 'docDate': '2023-10-03', 'docCat': 'Seasons'}]

if __name__ == '__main__':
    db = connectDataBase()
    documents = db.documents

    # data = pd.read_csv('documents.csv')
    # print(data.to_dict('records'))
    for row in data:
        createDocument(documents, **row)
