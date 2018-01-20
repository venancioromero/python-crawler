#!/usr/bin/env python3

import sys, time, requests,queue, _thread, re, threading,os
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from threading import Thread

OUTPUT_FILE = "pages.txt"
LOCK = threading.Lock()
QUEUE_SIZE = 100
WAIT_TIME = 1 ## wait time
FULL_QUEUE_WAIT_TIME = 1 ## if queue is full, wait
SEPARATOR = " || "
COUNT = 0

def isVisited(url):
    return url in url_list

def writeData(url,body):
    global COUNT
    
    LOCK.acquire() 

    urlFile.write(url + SEPARATOR + body)
    COUNT += 1

    if (COUNT == number_of_pages):
        os._exit(1)

    LOCK.release()

def delEndLines(text):
    return ''.join(text.splitlines()) + "\n"

def processUrl(url):
    global FULL_QUEUE_WAIT_TIME
    body = requests.get(url).text
    writeData(url,delEndLines(body)) # remove endlines of body
    soup = BeautifulSoup(body,'html.parser')
    
    print(url)

    for link in soup.find_all('a'): 
        l = link.get('href')
        
        if l == None:
            continue

        if re.search("^http", l) == None:
            l = domain + l
        
        if isVisited(l):
            continue
        
        while(q.full()):
            time.sleep(FULL_QUEUE_WAIT_TIME)
            FULL_QUEUE_WAIT_TIME = 2 * FULL_QUEUE_WAIT_TIME
            print("Queue is full. Waiting...")

        q.put(l)
        url_list.append(l)

def checkEntry():
    if len(sys.argv) != 3:
        print ("USAGE python crawler.py <number_of_pages> <URL>")
        print ("Set number_of_pages to 0 for infinite loop.")
        sys.exit(-1)

def main():
    while(True):
        time.sleep(WAIT_TIME)
        link = q.get(block=True)
        _thread.start_new_thread(processUrl,(link,))
        
if __name__ == "__main__":
    
    checkEntry()

    number_of_pages = int(sys.argv[1])
    url_in = sys.argv[2]
    url_list = [url_in]
    
    q = queue.Queue(QUEUE_SIZE)  # Queue
    q.put(url_in)

    url = urlparse(url_in)
    domain = url.scheme +"://"+ url.netloc

    urlFile = open(OUTPUT_FILE,"w") #file
    main()
    urlFile.close()
