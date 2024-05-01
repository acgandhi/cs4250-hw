#------------------------------------------------------------------------------
# AUTHOR: Amar Gandhi
# FILENAME: crawler.py
# SPECIFICATION: Crawl the page www.cpp.edu/sci/computer-science, until the
#   permanent faculty page is reached. Parse each page to find new URLs to
#   crawl, and store each page in mongodb.
# FOR: CS 4250 - Assignment #4
# TIME SPENT: ~2.5 hours
#------------------------------------------------------------------------------

import re
from queue import Queue
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import urlopen

import pymongo
from pymongo.collection import Collection
from bs4 import BeautifulSoup

default_scheme = 'https'
default_netloc = 'www.cpp.edu'


def store_page(col, url, html):
    col.insert_one({
        '_id': url,
        'text': html
    })

def page_exists(col, url):
    return col.count_documents({'_id': url}, limit=1) != 0

def is_target_page(doc: BeautifulSoup):
    # found heading that matches -> True
    return doc.body.find('h1', string='Permanent Faculty') is not None

def find_urls(doc: BeautifulSoup) -> set:
    urls = set()
    # find the urls in page
    for tag in doc.body.find_all('a', href=re.compile('\.s?html$')):
        href = tag['href']
        # parse the url, replace scheme and netloc if missing
        parsed = urlsplit(href)
        scheme = parsed.scheme
        netloc = parsed.netloc
        if scheme == '':
            scheme = default_scheme
        if netloc == '':
            netloc = default_netloc
        # add to set
        urls.add(urlunsplit((scheme, netloc, parsed.path, parsed.query, parsed.fragment)))

    return urls


def crawler_thread(col: Collection, frontier: Queue):
    while not frontier.empty():
        url = frontier.get()
        if page_exists(col, url):
            continue
        print('Looking at', url)
        html = None
        try:
            html = str(urlopen(url).read())
        except HTTPError as e:
            print(e)
        except URLError as e:
            print('The server could not be found!')
        if html is None:
            # failed to get, go to next
            continue

        store_page(col, url, html)
        doc = BeautifulSoup(html)   # parse html now to avoid double parse

        urls = find_urls(doc)
        print('New urls', urls)
        for newUrl in urls:
            if page_exists(col, newUrl):
                continue
            frontier.put(newUrl)

        if is_target_page(doc):
            # clear frontier if target page is reached
            frontier = Queue()


def main(col: Collection, seed_urls):
    frontier = Queue()      
    for url in seed_urls:
        frontier.put(url)
        col.delete_one({'_id': url})    # drop seed urls so we can start
    crawler_thread(col, frontier)


if __name__ == '__main__':
    client = pymongo.MongoClient('localhost', 27017)
    db = client.cs4250
    col = db.pages
    main(col, ['https://www.cpp.edu/sci/computer-science/'])
