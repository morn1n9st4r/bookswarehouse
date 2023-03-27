from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
import pandas as pd
import re



class NewBooksParser:
    def __init__(self, url,
                 userAgent='APIs-Google (+https://developers.google.com/webmasters/APIs-Google.html)'):
        self.url = url
        self.userAgent = userAgent

    def scrape_text(self):
        req = Request(self.url, headers={'User-Agent': self.userAgent})
        html = urlopen(req).read().decode('utf-8')
        soup = BeautifulSoup(html, "html.parser")

        # scrape hrefs of books from new books page
        mylinks = soup.find_all("a", {"class": "book-item__link"})
        mylinks_hrefs = [f"https://www.livelib.ru{link['href']}" for link in mylinks]
        return mylinks_hrefs
