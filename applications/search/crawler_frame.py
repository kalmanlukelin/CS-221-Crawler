import logging
from datamodel.search.TingyuChuanchunPengjhih_datamodel import TingyuChuanchunPengjhihLink, OneTingyuChuanchunPengjhihUnProcessedLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4

import sys
from urlparse import urlparse, parse_qs, urljoin
from uuid import uuid4
from bs4 import BeautifulSoup
import tldextract

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

@Producer(TingyuChuanchunPengjhihLink)
@GetterSetter(OneTingyuChuanchunPengjhihUnProcessedLink)
class CrawlerFrame(IApplication):
    app_id = "TingyuChuanchunPengjhih"

    def __init__(self, frame):
        self.app_id = "TingyuChuanchunPengjhih"
        self.frame = frame
        self.subdomains = {}

    def initialize(self):
        self.count = 0
        links = self.frame.get_new(OneTingyuChuanchunPengjhihUnProcessedLink)
        if len(links) > 0:
            print "Resuming from the previous state."
            self.download_links(links)
        else:
            l = TingyuChuanchunPengjhihLink("http://www.ics.uci.edu/")
            print (l.full_url).encode(sys.stdin.encoding, "replace").decode(sys.stdin.encoding)
            self.frame.add(l)

    def update(self):
        unprocessed_links = self.frame.get_new(OneTingyuChuanchunPengjhihUnProcessedLink)
        if unprocessed_links:
            self.download_links(unprocessed_links)

    def download_links(self, unprocessed_links):
        for link in unprocessed_links:
            print ("Got a link to download: %s" % link.full_url).encode(sys.stdin.encoding, "replace").decode(sys.stdin.encoding)
            downloaded = link.download()
            links = extract_next_links(downloaded)
            sub = tldextract.extract(downloaded.url).subdomain
            self.subdomains[sub] = self.subdomains.get(sub,0)+1
            print self.subdomains
            for l in links:
                if is_valid(l):
                    self.frame.add(TingyuChuanchunPengjhihLink(l))

    def shutdown(self):
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")
    
max_url=None
max_outlinks=0

def extract_next_links(rawDataObj):
    outputLinks = []
    '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.
    
    Suggested library: lxml
    '''
    urls=set()
    orig_url=rawDataObj.url
    orig_cont=rawDataObj.content

    if rawDataObj.is_redirected:
        orig_url=rawDataObj.final_url

    p=BeautifulSoup(orig_cont, "lxml").findAll('a')

    for item in p:
        url=item.get('href')
        url = urljoin(orig_url, url) # Ensure abolute url.
        urls.add(url)

    for url in urls:
        outputLinks.append(url)

    # Record the url that has the most out links
    global max_outlinks
    global max_url
    len_outlinks=len(outputLinks)
    if len_outlinks > max_outlinks:
        max_outlinks=len_outlinks
        max_url=orig_url

<<<<<<< HEAD
    # Record number of urls in subdomain
    sub = tldextract.extract(rawDataObj.url).subdomain
    global subdomains
    subdomains[sub] = subdomains.get(sub,0)+1
    
    print("")
    print "relative url: "+str(relative_url)
    print "r_total: "+str(len(relative_url))

    print("")
    # print "dynamic url: "+str(dym_url)
    print "dym_total: "+str(len(dym_url))

    print("")
    #print "calendar url: "+str(calen_url)
    print "spec_total: "+str(len(spec_url))

    print("") 
    print "long url: "+str(long_url)
    print "long_total: "+str(len(long_url))

    print("")
    print "repeat url "+str(repeat_url) 
    print "repeat_total: "+str(len(repeat_url))
    
    print("")
    print ("max_url: %s" % max_url).encode(sys.stdin.encoding, "replace").decode(sys.stdin.encoding)
    print "max_outlinks: %d" % max_outlinks
    for d in subdomains:
        print (str(d) +":"+ str(subdomains[d])).encode(sys.stdin.encoding, "replace").decode(sys.stdin.encoding)
    print("")

=======
    print ("url visited: %s" % rawDataObj.url).encode(sys.stdin.encoding, "replace").decode(sys.stdin.encoding)
    print ("Number of links %d" % len(outputLinks)).encode(sys.stdin.encoding, "replace").decode(sys.stdin.encoding)
    print ("max_url: %s" % max_url).encode(sys.stdin.encoding, "replace").decode(sys.stdin.encoding)
    print "max_outlinks: %d" % max_outlinks
   
>>>>>>> 3a52829ebbd4d3a8fa7944653b18bd2ea69b4024
    return outputLinks

relative_url=[]
dym_url=[]
spec_url=["^.*calendar.*$", "^.*ganglia.*$"]
long_url=[]
repeat_url=[]

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    global relative_url
    global dym_url
    global spec_url
    global long_url
    global repeat_url

    #print "current url: "+str(url)

    # Check if the url is absolute.
    if not bool(urlparse(url).netloc): 
        relative_url.append(url)
        return False

    # Filter dynamic url
    dynam={'#', '&', '$', '+', '=', '?', '%', 'cgi'}
    for d in dynam:
        if d in url:
            dym_url.append(url)
            return False
    
    # Filter calendar url
    for s in spec_url:
        if re.search(s, url): 
            spec_url.append(url)
            return False

    # Filter long url ex: http://www.example.com/uU5dR1gpXCHX45K8aOMct11OrLtyrYJeUnw_RxaUsg.eyJpbnN0YW5jZUlkIjoiMTNkZDc
    if re.search("^.*/[^/]{300,}$", url):
        long_url.append(url)
        return False

    # Filter repeating directories ex : http://www.example.org/media/media/page/sites/css/html-reset.css
    parsed_url=urlparse(url)
    url_path=parsed_url.path
    duplic_path=set()
    for u in url_path.split('/'):
        if u != "":
            if u in duplic_path: 
                repeat_url.append(url)
                return False
            duplic_path.add(u)

    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower()) 
            #and not re.match("^.*?(/.+?/).*?\1.*$|^.*?/(.+?/)\2.*$",parsed.path.lower()) 


# ^.*?(/.+?/).*?\1.*$|^.*?/(.+?/)\2.*$
    except TypeError:
        print ("TypeError for ", parsed)
        return False
