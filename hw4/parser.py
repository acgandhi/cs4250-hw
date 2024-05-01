#------------------------------------------------------------------------------
# AUTHOR: Amar Gandhi
# FILENAME: parser.py
# SPECIFICATION: Parse the Permanent Faculty page to find professor info, and
#   store in mongodb.
# FOR: CS 4250 - Assignment #4
# TIME SPENT: ~1 hours
#------------------------------------------------------------------------------

import re

import pymongo
from bs4 import BeautifulSoup
from pymongo.collection import Collection


def clean_string(s: str) -> str:
    return re.sub(r':+', ' ', s).strip()

def main(pages: Collection, professors: Collection):
    # get page from mongo
    prof_page = pages.find_one({'_id': 'https://www.cpp.edu/sci/computer-science/faculty-and-staff/permanent-faculty.shtml'})
    # parse with bs4
    parsed_page = BeautifulSoup(prof_page['text'])
    # extract professor data, and store in mongo
    sec = parsed_page.body.find('section', class_='text-images')
    for subsec in sec.find_all('div'):
        if subsec.h2 is None:
            # skip empty sections
            continue
        prof = {
            'name':   subsec.h2.get_text().strip(),
            'title':  clean_string(subsec.find('strong', string=re.compile('Title')).next_sibling),
            'office': clean_string(subsec.find('strong', string=re.compile('Office')).next_sibling),
            'phone':  clean_string(subsec.find('strong', string=re.compile('Phone')).next_sibling),
            'email':  clean_string(subsec.find('a').get_text())
        }
        print(prof)
        professors.insert_one(prof)


if __name__ == '__main__':
    client = pymongo.MongoClient('localhost', 27017)
    db = client.cs4250
    pages = db.pages
    professors = db.professors
    main(pages, professors)
