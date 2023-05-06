import re
import pandas as pd
from urllib.request import urlopen, Request
import bs4
from bs4 import BeautifulSoup


class PublisherParser:
    
    """
    The PublisherParser class is used to scrape information about a publisher
    from the livelib.ru website.

    Attributes:
        url (str): The url of the webpage to scrape.
        user_agent (str): The user agent string to use in the request headers.

    Methods:
        __init__(url,
                 user_agent='APIs-Google \
                    (+https://developers.google.com/webmasters/APIs-Google.html)'):
            Initializes a new instance of the PublisherParser class with the specified url
            and user agent.

        scrape_text():
            Scrapes information about the author from the webpage and returns the result
            as a pandas DataFrame.

        get_text(val):
            Utility method to extract text from a Beautiful Soup tag.
        
        get_count_of_books(val):
            Utility method to extract number of books from a Beautiful Soup tag.

        get_year_of_creation(val):
            Utility method to extract year from a Beautiful Soup tag.

    Usage:
        parser = PublisherParser(url='https://www.livelib.ru/publisher/54374')
        result = parser.scrape_text()
    """

    def __init__(self, url,
                 user_agent='APIs-Google \
                    (+https://developers.google.com/webmasters/APIs-Google.html)'):


        """
        Constructor method for PublisherParser class.

        Parameters:
        url (str): The url of the webpage to scrape.
        user_agent (str): The user agent string to use in the request headers.
                         Defaults to a Google API user agent.
        """

        self.url = url
        self.user_agent = user_agent

    def scrape_text(self):
        req = Request(self.url, headers={'User-Agent': self.user_agent})
        html = urlopen(req).read().decode('utf-8')
        soup = BeautifulSoup(html, "html.parser")

        # scrape some particular areas of page
        publisherid = re.sub(r'-.*', '',
                             re.sub(r'https\:\/\/www\.livelib\.ru\/publisher\/', '', self.url)
                            )
        name = soup.find("title", {"id": "title-head"}) or ['null']


        books_all = [self.get_count_of_books(tag)
                     for tag in soup.findAll("li", {"class": "standard"})]
        books = [book for book in books_all if book is not None]
        year_all = [self.get_year_of_creation(tag) for tag in soup.findAll("p") or ['null']]
        years = [re.sub(".+r", "",
                    re.sub(r".+Год основания:|\\|\/|\<|\>|p|b|n|\t+|\n+|\s+", "", str(year))
                    )
                for year in year_all if year is not None]
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

    def get_year_of_creation(self, tag):
        if "Год основания: " in str(tag):
            return tag

    def get_text(self, val):
        if isinstance(val, bs4.element.Tag):
            return re.sub('\n+', '', val.text)
        else:
            return str(*val)
