from io import BytesIO
import datetime
import logging
import re
from difflib import SequenceMatcher

import requests
import urllib3
import configparser
import huspacy
import pdfplumber
from jinja2 import Environment, FileSystemLoader, select_autoescape
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from bmmbackend import bmmbackend
import bmmtools
from bmm_kozlonydb import Bmm_KozlonyDB

def search(text, keyword):
    keyword = keyword.replace('*', '').replace('"', '')
    results = []
    matches = [m.start() for m in re.finditer(re.escape(keyword), text, re.IGNORECASE)]

    words = text.split()

    for match_index in matches:
        # Convert character index to word index
        char_count = 0
        word_index = 0

        for i, word in enumerate(words):
            if char_count <= match_index < char_count + len(word):
                word_index = i
                break
            char_count += len(word) + 1  # +1 for space

        # Get surrounding 8 words before and 6 words after the match
        before = " ".join(words[max(word_index - 10, 0) : word_index])
        after = " ".join(words[word_index + 1 : word_index + 9])
        found_word = words[word_index]
        
        match = SequenceMatcher(None, found_word.lower(), keyword.lower()).find_longest_match()
        match_before = found_word[: match.a]
        match_after = found_word[match.a + match.size :]
        common_part = found_word[match.a : match.a + match.size]

        # Build the context properly with correct spacing
        before_context = before
        if match_before:
            before_context = before_context + " " + match_before if before_context else match_before
        
        after_context = match_after
        if after:
            after_context = after_context + " " + after if after_context else after
    
        results.append(
            {
                "before": before_context + " ",
                "after": " " + after_context,
                "common": common_part,
            }
        )
    return results

def download_data(year, month):

    url = config['Download']['url']
    
    pagenum = 0
    pagecount = 0

    while True:
        pagenum = pagenum + 1

        params = {
            'year': year,
            'month' : month,
            'serial' : '',
            'page' : pagenum
        }

        page = requests.get(url, params = params, verify = False)
        logging.info(page.url)
        soupage = BeautifulSoup(page.content, 'html.parser')

        entry = {}
        journalrows = soupage.find_all('div', class_ = 'journal-row')
        for journalrow in journalrows:

            docurl = journalrow.find('meta', {'itemprop': 'url'})['content']

            logging.info(docurl)

            dochash = urlparse(docurl).path.split('/')[-2]
            if db.getDoc(dochash) is None:
                entry['scrapedate'] = datetime.datetime.now()
                entry['issuedate'] = journalrow.find('meta', {'itemprop': 'datePublished'})['content']

                logging.info(f"New: {entry['issuedate']}")

                entry['url'] = docurl
                
                anchors = journalrow.find_all('a')
                for anchor in anchors:
                    if 'hivatalos-lapok' in anchor['href'] and 'dokumentumok' in anchor['href'] and anchor.find('b', {'itemprop': 'name'}):
                        entry['pdfurl'] = anchor['href']
                        entry['title'] = anchor.find('b', {'itemprop': 'name'}).decode_contents()

                res = requests.get(entry['pdfurl'], verify = False).content
                with pdfplumber.open(BytesIO(res)) as pdf:
                    entry['content'] = ''
                    entry['lemmacontent'] = ''
                    texts = []
                    pdfpagenum = 0
                    for page in pdf.pages:
                        texts.append(page.extract_text())
                        pdfpagenum = pdfpagenum + 1
                        if pdfpagenum == 10:
                            lemmas = []
                            if config['DEFAULT']['donotlemmatize'] == '0':
                                lemmas = bmmtools.lemmatize(nlp, texts)
                            entry['lemmacontent'] = entry['lemmacontent'] + " ".join(lemmas)
                            entry['content'] = entry['content'] + "\n".join(texts)
                            pdfpagenum = 0
                            texts = []

                    lemmas = []
                    if config['DEFAULT']['donotlemmatize'] == '0':
                        lemmas = bmmtools.lemmatize(nlp, texts)
                    entry['lemmacontent'] = entry['lemmacontent'] + " ".join(lemmas)
                    entry['content'] = entry['content'] + "\n".join(texts)

                    db.saveDoc(dochash, entry)
                    db.commitConnection()


        # getting page count
        if pagecount == 0:
            pagination = soupage.find('ul', class_ = 'pagination')
            if pagination:
                href = pagination.find_all('li')[-2].find('a')['href']
                query_params = parse_qs(urlparse(href).query)
                pagecount = int(query_params.get("page", [0])[0])

        if pagenum >= pagecount:
            break


def clearIsNew(ids):
    
    for num in ids:
        logging.info(f"Clear isnew: {num}")
        db.clearIsNew(num)

    db.commitConnection()


# some certificate problems
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

config = configparser.ConfigParser()
config.read_file(open('config.ini'))
logging.basicConfig(
    filename=config['DEFAULT']['logfile_name'], 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s | %(module)s.%(funcName)s line %(lineno)d: %(message)s')

logging.info('KozlonyScraper started')

db = Bmm_KozlonyDB(config['DEFAULT']['database_name'])
backend = bmmbackend(config['DEFAULT']['monitor_url'], config['DEFAULT']['uuid'])

foundIds = []

env = Environment(
    loader=FileSystemLoader('templates'),
    autoescape=select_autoescape()
)
contenttpl = env.get_template('content.html')
contenttpl_keyword = env.get_template('content_keyword.html')

if config['DEFAULT']['donotlemmatize'] == '0':
    nlp = huspacy.load()

lastissuedate = db.getLastIssueDate()
if (lastissuedate):
    d = datetime.datetime.strptime(lastissuedate, '%Y-%m-%d')
else:
    d = datetime.datetime.now()

download_data(year = d.year, month = d.month)

# ha d nem az aktualis honap, akkor az aktualis honapra is kell futtatni download_data-t
ma = datetime.datetime.now()
if d.year != ma.year or d.month != ma.month:
    download_data(year = ma.year, month = ma.month)

events = backend.getEvents()
for event in events['data']:
    result = None

    try:
        if event['type'] == 1:
            keresoszo = bmmtools.searchstringtofts(event['parameters'])
            if keresoszo:
                result = db.searchRecords(keresoszo)
                for res in result:
                    foundIds.append(res[0])
        else:
            result = db.getAllNew()
            for res in result:
                foundIds.append(res[0])

        if result:
            content = ''
            for res in result:
                if event['type'] == 1 and event['parameters']:
                    # Search for keyword matches with context
                    search_results = search(res[6], event['parameters'])  # res[6] is content
                    if not search_results and config['DEFAULT']['donotlemmatize'] == '0':
                        # Try lemmatized search if no direct matches
                        search_results = search(res[7], event['parameters'])  # res[7] is lemmacontent
                        for sr in search_results:
                            sr['before'] = "szótövezett találat: " + sr['before']
                    
                    if search_results:
                        # Create enhanced result with search context
                        enhanced_res = list(res) + [search_results[:5], len(search_results), event['parameters']]
                        content = content + contenttpl_keyword.render(doc = enhanced_res)
                else:
                    content = content + contenttpl.render(doc = res)

            if config['DEFAULT']['donotnotify'] == '0':
                backend.notifyEvent(event['id'], content)
                logging.info(f"Notified: {event['id']} - {event['type']} - {event['parameters']}")
    except Exception as e:
        logging.error(f"Error: {e}")
        logging.error(f"Event: {event['id']} - {event['type']} - {event['parameters']}")


logging.info('foundIds: ')
logging.info(foundIds);

if config['DEFAULT']['staging'] == '0':
    clearIsNew(foundIds)

db.closeConnection()

logging.info('KozlonyScraper ready. Bye.')

print('Ready. Bye.')
