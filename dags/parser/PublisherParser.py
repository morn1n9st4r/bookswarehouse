from bs4 import BeautifulSoup
import bs4
from urllib.request import urlopen, Request
import pandas as pd
import re


class PublisherParser:

    def __init__(self, url,
                 userAgent='APIs-Google (+https://developers.google.com/webmasters/APIs-Google.html)'):
        
                
        """
        Constructor method for PublisherParser class.

        Parameters:
        url (str): The url of the webpage to scrape.
        userAgent (str): The user agent string to use in the request headers. Defaults to a Google API user agent.
        """
        
        self.url = url
        self.userAgent = userAgent

    def scrape_text(self):
        req = Request(self.url, headers={'User-Agent': self.userAgent})
        html = urlopen(req).read().decode('utf-8')
        soup = BeautifulSoup(html, "html.parser")

        # scrape some particular areas of page
        publisherid = re.sub(r'-.*', '', re.sub(r'https\:\/\/www\.livelib\.ru\/publisher\/', '', self.url))
        name = soup.find("title", {"id": "title-head"}) or ['null']

        #isbn = (...)
        #books = soup.find("a", {"class": "header-profile-login"}) or ['null']
        books_all = [self.get_count_of_books(tag) for tag in soup.findAll("li", {"class": "standard"})]
        books = [book for book in books_all if book != None]
        #city = (...)
        year_all = [self.get_year_of_creation(tag) for tag in soup.findAll("p") or ['null']]
        years = [re.sub(r".+Год основания:|\\|\/|\<|\>|p|b|n|\t+|\n+|\s+", "", str(year)) for year in year_all if year != None]
        page = soup.find("a", {"class": "publisher-link"}) or ['null']
        favorite = soup.find("span", {"class": "count-in-fav"}) or ['null']

        return pd.DataFrame(
            data ={
                'PublisherID':[publisherid],
                'name':[self.get_text(name)],
                'books':[self.get_text(*books[0])],
                'years':[self.get_text(years)],
                'page':[self.get_text(page)],
                'favorite':[self.get_text(favorite)],
            }
        )
    
    def get_count_of_books(self, tag):
        if "Книги (" in str(tag.find('a').string):
            return tag
        else:
            pass

    def get_year_of_creation(self, tag):
        if "Год основания: " in str(tag):
            return tag
        else:
            pass

    def get_text(self, val):
        if type(val) == bs4.element.Tag:
            return re.sub('\n+', '', val.text)
        else: 
            return str(*val)