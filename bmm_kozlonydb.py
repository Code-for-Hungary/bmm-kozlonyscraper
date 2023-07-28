import os
import sqlite3


class Bmm_KozlonyDB:

    def __init__(self, databasename) -> None:
        self.databasename = databasename
        if not os.path.exists(self.databasename):
            self.connection = sqlite3.connect(self.databasename)
            c = self.connection.cursor()

            c.execute('''CREATE TABLE IF NOT EXISTS docs (
                            dochash TEXT,
                            scrape_date TEXT,
                            issue_date TEXT,
                            title TEXT,
                            uri TEXT,
                            pdfuri TEXT,
                            content TEXT,
                            lemmacontent TEXT,
                            isnew INTEGER)''')

            c.execute('''CREATE VIRTUAL TABLE IF NOT EXISTS docs_fts 
                            USING FTS5 (dochash UNINDEXED, content, lemmacontent, tokenize="unicode61 remove_diacritics 2")''')

            c.execute('''CREATE TRIGGER docs_ai AFTER INSERT ON docs BEGIN
                            INSERT INTO docs_fts(dochash, content, lemmacontent) 
                            VALUES (new.dochash, new.content, new.lemmacontent);
                        END;''')

            c.execute('''CREATE TRIGGER docs_ad AFTER DELETE ON docs BEGIN
                            INSERT INTO docs_fts(docs_fts, dochash, content, lemmacontent) 
                                VALUES('delete', old.dochash, old.content, old.lemmacontent);
                        END;''')

            self.commitConnection()
            c.close()
        else:
            self.connection = sqlite3.connect(self.databasename)

    def closeConnection(self):
        self.connection.close()

    def commitConnection(self):
        self.connection.commit()

    def getDoc(self, dochash):
        c = self.connection.cursor()

        c.execute('SELECT * FROM docs WHERE dochash=?', (dochash,))
        res = c.fetchone()

        c.close()
        return res

    def getLastIssueDate(self):
        c = self.connection.cursor()

        c.execute('SELECT MAX(issue_date) FROM docs')
        res = c.fetchone()

        c.close()
        return res[0]

    def saveDoc(self, dochash, entry):
        c = self.connection.cursor()

        c.execute('INSERT INTO docs (dochash, scrape_date, issue_date, title, uri, pdfuri, content, lemmacontent, isnew) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)',
            (dochash, entry['scrapedate'], entry['issuedate'], entry['title'], entry['url'], entry['pdfurl'], entry['content'], entry['lemmacontent']))

        c.close()

    def clearIsNew(self, dochash):
        c = self.connection.cursor()
        
        c.execute('UPDATE docs SET isnew=0 WHERE dochash=?', (dochash,))

        c.close()

    def searchRecords(self, keyword):
        c = self.connection.cursor()

        c.execute('SELECT * FROM docs WHERE isnew=1 AND dochash IN '
                    '(SELECT dochash FROM docs_fts WHERE docs_fts MATCH ?)',
                    (keyword,))

        results = c.fetchall()
        c.close()
        return results

    def getAllNew(self):
        c = self.connection.cursor()

        c.execute('SELECT * FROM docs WHERE isnew=1')

        results = c.fetchall()
        c.close()
        return results
