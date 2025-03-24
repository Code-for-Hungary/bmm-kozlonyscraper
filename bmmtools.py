import re

def searchstringtofts(searchstring):
    keresoszo = ''
    if isinstance(searchstring, str):
        keresoszo = searchstring.strip()
        keresoszo = re.sub(r'\s+', ' ', keresoszo)
        keresoszo = re.sub(r'([()\-])', '', keresoszo)
        keresoszo = re.sub(r'\/', '_', keresoszo)
        if keresoszo:
            if not re.search(r'(["+*])', keresoszo):
                keresoszo = re.sub(r'([\s])', ' + ', keresoszo) + '*'

    return keresoszo

def mnvtimestamp(tstamp):
    return int(tstamp) * 1000

def lemmatize(nlp, texts):
    lemmas = []
    docs = list(nlp.pipe(texts))
    for doc in docs:
        for token in doc:
            if token.pos_ in ['NOUN', 'ADJ', 'PROPN', 'ADP', 'ADV', 'VERB'] and token.lemma_.isalpha():
                lemmas.append(token.lemma_.lower())
    return lemmas
