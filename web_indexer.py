"""
Web Indexer
=================================
Author: Abhishek Dasgupta,
This script basically crawls a domain and 
then extracts all links <a href=""></a>, and finds all links
on that domain and keeps on the process. It also stores 
unique domains in a database.
"""

import bs4, requests
from bs4 import BeautifulSoup
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, func, desc, asc
from sqlalchemy import Sequence
from sqlalchemy.sql import select
from urlparse import urlparse
from urlparse import urljoin
from sqlalchemy import text
from sqlalchemy.orm import create_session
import random
import eventlet
import sys
import signal
import tldextract

eventlet.monkey_patch()
prevuname= "domain"
domain_repcount= 0
rctrack= 0
prevrc= 0

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
engine_prime = create_engine('sqlite:///url_domain.db')
engine_second = create_engine('sqlite:///url_list.db')
engine_errorurl = create_engine('sqlite:///error_url.db')


metadata_prime = MetaData(bind=engine_prime)
metadata_second = MetaData(bind=engine_second)
metadata_error = MetaData(bind=engine_errorurl)


liteSession_prime  = create_session(engine_prime)
liteSession_second  = create_session(engine_second)
liteSession_error  = create_session(engine_errorurl)


url_domain = Table('url_domain', metadata_prime,
    Column('id', Integer, Sequence('user_id_seq'), primary_key=True),
    Column('domain', String),
    Column('hits', Integer))

url_list = Table('url_list', metadata_second,
    Column('id', Integer, Sequence('user_id_seq'), primary_key=True),
    Column('url', String),
    Column('scraped', Integer))

error_url = Table('error_url', metadata_error,
    Column('id', Integer, Sequence('user_id_seq'), primary_key=True),
    Column('url', String),
    Column('status_code', String))



metadata_prime.create_all(engine_prime)
metadata_second.create_all(engine_second)
metadata_error.create_all(engine_errorurl)

conn_prime = engine_prime.connect()
conn_second = engine_second.connect()
conn_error= engine_errorurl.connect()


def signal_handler(signal, frame):
    
    print("In signal_handler")
    sys.exit(0)
    
def printFunc(elements):
    for idx in range(elements.__len__()):
        print(idx)
        print(elements[idx]['href'])

def addErrorUrls(link, e):
    ins_eu= error_url.insert()
    ins_eu= ins_eu.values(url=str(link), status_code=str(e))
    conn_error.execute(ins_eu) 

def process(url):
    try:
        with eventlet.Timeout(10):
            r = requests.get(url, headers=headers, timeout=(3.05, 10))
        soup = BeautifulSoup(r.text, 'lxml')
        # elements = soup.select('.r a')
        elements = soup.select('a[href^="https"]')
#         printFunc(elements)
        return elements
    except requests.exceptions.Timeout as e:
    # Maybe set up for a retry, or continue in a retry loop
        addErrorUrls(url, e) 
        return 0

    except requests.exceptions.TooManyRedirects as e:
    # Tell the user their URL was bad and try a different one
        addErrorUrls(url, e) 
        return 0

    except requests.exceptions.RequestException as e:
    # catastrophic error. bail.
        addErrorUrls(url, e)
        return 0

    except:
           return 0

def add_domain(uname, type):
    global domain_repcount
    global prevuname
    global prevrc
    global rctrack
    
    s = select([func.count()]).select_from(url_domain)
    ret= conn_prime.execute(s).fetchone()
    rc = ret[0]
    
    ins_ud = url_domain.insert()
    parser = urlparse(uname)
    ext = tldextract.extract(parser.netloc)
    udomain1 = parser.scheme + '://' + parser.netloc 
    udomain = parser.scheme + '://www.' + ext.domain + '.' + ext.suffix 
    s = "SELECT url_domain.domain, count(*) FROM url_domain WHERE url_domain.domain = :x GROUP BY url_domain.domain"
    qret= conn_prime.execute(s, x=udomain).fetchall()
    if len(qret) == 0:
        ins_ud= ins_ud.values(domain=udomain, hits=0)
        conn_prime.execute(ins_ud)
        prevrc = ret[0]
        prevuname = udomain
        print "actual domain: ", udomain1
        print "modified domain: ", udomain
        return 1
#     else:
#         print "repeated domain: ", udomain
               
    if(len(ret) >0 and type == 2):
        s= select([url_domain.c.hits]).select_from(url_domain).where(url_domain.c.domain == udomain)
        qret= conn_prime.execute(s).fetchone()
#         print "hits: ", qret[url_domain.c.hits]
        newhits= qret[url_domain.c.hits] + 1
        s= url_domain.update().where(url_domain.c.domain == udomain).values(hits= newhits)
        qret= conn_prime.execute(s)
        
    if(domain_repcount == 3 and type == 2):
        searchStr= '%' + ext.domain + '%'
        conn_second.execute(url_list.delete().where(url_list.c.url.like(searchStr)))
        domain_repcount = 0         
 
    else:
        if(udomain == prevuname):
            domain_repcount= domain_repcount+1
 
        else:
            domain_repcount= 0 

    if(rc == prevrc):
        rctrack= rctrack + 1 
        if(rctrack == 5):
#             print "repeated: ", udomain
            searchStr= '%' + ext.domain + '%'
            conn_second.execute(url_list.delete().where(url_list.c.url.like(searchStr)))            
#             s = "DELETE from url_list WHERE url LIKE :x "
#             conn_second.execute(s, x=searchStr)
            rctrack= 0          
    else:
        rctrack= 0   
    prevrc = ret[0]
    prevuname = udomain
    return 0
     
def add_url_list(uname):
    
    s = select([func.count()]).select_from(url_list)
    ret= conn_second.execute(s).fetchone()
    if(ret[0] == 100000):
        return 0
    else:
        ins_ul = url_list.insert()
        ins_ul = ins_ul.values(url=uname, scraped=0)
        conn_second.execute(ins_ul)
#         print "added: ", uname 
        return 1
    
def checkValidElement(url):
    
    if str(url).endswith('.swf'):
        return 0
    if str(url).endswith('.exe'):
        return 0
    if str(url).endswith('.jpg'):
        return 0
    if str(url).endswith('.JPG'):
        return 0
    if str(url).endswith('.mp4'):
        return 0
    if str(url).endswith('.mp3'):
        return 0
    if str(url).endswith('.wmv'):
        return 0
    if str(url).endswith('.WMV'):
        return 0
    if str(url).endswith('.wm'):
        return 0
    if str(url).endswith('.WM'):
        return 0
    if str(url).endswith('.png'):
        return 0
    if str(url).endswith('.gif'):
        return 0
    if str(url).endswith('.pdf'):
        return 0
    if str(url).endswith('.PDF'):
        return 0
    if str(url).endswith('.dmg'):
        return 0
    if str(url).endswith('.ogg'):
        return 0
    else:
        return url
    

def db_process(url):
    ret = checkValidElement(url)
    if(ret == 0):
        return
    elements = process(url)
    if (elements == 0):
        print "rejected"
        conn_second.execute(url_list.delete().where(url_list.c.url == url))            
        return 0
    print "processed urls: ",elements.__len__()
    for idx in range(elements.__len__()):
        try:
            uname = checkValidElement(str(elements[idx]['href']))
            if(uname == 0):
                continue
        except:
            return
        domstatus= add_domain(uname, 1)
        if(domstatus == 1):
            add_url_list(uname)

    add_domain(url, 2)

            
def print_db(db):            
    s = select([db])
    result = s.execute()
    for row in result:
        print row
    result.close()
    
def getURL():
    
    s= "SELECT * FROM url_list ORDER BY RANDOM() LIMIT 1"

    ret = conn_second.execute(s).fetchone()
    if(ret is None):
        print "url_list exhausted"
        return -1   
    if ret[url_list.c.scraped]== 1:
        conn_second.execute(url_list.delete().where(url_list.c.url == ret[url_list.c.url]))
        return 0  
    else:
        return ret[url_list.c.url]
    
def main():
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGUSR1, signal_handler)
    
#     signal.pause()
    path= "//init_url"
    db_process(path)

    var = 1
    count = 0
    result = 0
    while var == 1 :
        result = 0;       
        while result == 0:  
            result = getURL();
#             print result
            if(result == -1):
                sys.exit(0)
        ret = db_process(result)
        if(ret == 0):
            continue
        conn_second.execute(url_list.delete().where(url_list.c.url == result))
        s = select([func.count()]).select_from(url_list)
        list_count= conn_second.execute(s).fetchone()
#         print "url_list inventory: ", list_count[0]
        s = select([func.count()]).select_from(url_domain)
        list_count= conn_prime.execute(s).fetchone()
#         print "url_domain inventory: ", list_count[0]
#         print_db(url_list)
#         print_db(url_domain)
            
main()   
