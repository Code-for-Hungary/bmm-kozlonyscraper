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

def search(text, keyword, nlp_warn=False):
    keyword = keyword.replace('*', '').replace('"', '')
    results = []
    matches = [m.start() for m in re.finditer(re.escape(keyword), text, re.IGNORECASE)]
    
    # Build a list of (word, start_position, end_position) tuples
    word_positions = []
    for match in re.finditer(r'\S+', text):
        word_positions.append((match.group(), match.start(), match.end()))

    for match_index in matches:
        # Find which word contains this character position
        word_index = 0
        for i, (word, start_pos, end_pos) in enumerate(word_positions):
            if start_pos <= match_index < end_pos:
                word_index = i
                break

        # Get surrounding 10 words before and after the match
        words = [w[0] for w in word_positions]  # Extract just the words
        before = " ".join(words[max(word_index - 16, 0) : word_index])
        after = " ".join(words[word_index + 1 : word_index + 17])
        found_word = words[word_index]
        match = SequenceMatcher(
            None, found_word, keyword
        ).find_longest_match()
        match_before = found_word[: match.a]
        if match_before != "":
            before = before + " " + match_before
        else:
            before = before + " "
        match_after = found_word[match.a + match.size :]
        if match_after != "":
            after = match_after + " " + after
        else:
            after = " " + after
        common_part = found_word[match.a : match.a + match.size]

        if nlp_warn:
            before = "szótövezett találat: " + before

        results.append(
            {
                "before": before,
                "after": after,
                "common": common_part,
            }
        )
    return results

def find_matching_multiple(keywords, entry):
    all_results = []
    for keyword in keywords:
        keyword_results = search(entry["content"], keyword)
        do_lemmatize = config['DEFAULT'].get('donotlemmatize', '0') == '0'
        if not keyword_results and do_lemmatize:
            keyword_results = search(entry["lemmacontent"], keyword, nlp_warn=True)
        all_results += keyword_results
    return all_results

def download_data(year, month):

    url = config['Download']['url']
    entries = []
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

            if journalrow.text.find('Nincs megjeleníthető tartalom.') >= 0:
                continue
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
                    entries.append((dochash, entry.copy()))


        # getting page count
        if pagecount == 0:
            pagination = soupage.find('ul', class_ = 'pagination')
            if pagination:
                href = pagination.find_all('li')[-2].find('a')['href']
                query_params = parse_qs(urlparse(href).query)
                pagecount = int(query_params.get("page", [0])[0])

        if pagenum >= pagecount:
            break
    
    return entries


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
eventgenerator_api_key = config['DEFAULT']['eventgenerator_api_key']

logging.info('KozlonyScraper started')

db = Bmm_KozlonyDB(config['DEFAULT']['database_name'])
backend = bmmbackend(config['DEFAULT']['monitor_url'], config['DEFAULT']['uuid'])

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

new_entries = db.getAllNew()

events = backend.getEvents(eventgenerator_api_key)
for event in events['data']:
    result = None

    try:
        new_ids = []
        content = ''
        if event['type'] == 1:
            for entry in new_entries:
                print(entry["title"])
                search_results = find_matching_multiple(event['parameters'].split(","), entry)
                result_entry = entry.copy()
                result_entry["result_count"] = len(search_results)
                result_entry["results"] = search_results[:5]
                if result_entry["results"]:
                    content += contenttpl_keyword.render(doc = result_entry)
                new_ids.append(entry['dochash'])
        else:
            for entry in new_entries:
                content = content + contenttpl.render(doc = entry)
                new_ids.append(entry['dochash'])

        if content and config['DEFAULT']['donotnotify'] == '0':
            backend.notifyEvent(event['id'], content, eventgenerator_api_key)
            logging.info(f"Notified: {event['id']} - {event['type']} - {event['parameters']}")
        clearIsNew(new_ids)
    except Exception as e:
        logging.error(f"Error: {e}")
        logging.error(f"Event: {event['id']} - {event['type']} - {event['parameters']}")


db.closeConnection()

logging.info('KozlonyScraper ready. Bye.')

print('Ready. Bye.')
