import re
import pandas as pd
from urllib.request import urlopen, Request
import bs4
from bs4 import BeautifulSoup


class AuthorParser:

    """
    The AuthorParser class is used to scrape information about a writer
    from the livelib.ru website.

    Attributes:
        url (str): The url of the webpage to scrape.
        user_agent (str): The user agent string to use in the request headers.

    Methods:
        __init__(url, 
                 user_agent='APIs-Google \
                    (+https://developers.google.com/webmasters/APIs-Google.html)'):
            Initializes a new instance of the AuthorParser class with the specified url
            and user agent.

        scrape_text():
            Scrapes information about the author from the webpage and returns the result
            as a pandas DataFrame.

        get_text(val):
            Utility method to extract text from a Beautiful Soup tag.

    Usage:
        parser = AuthorParser(url='https://www.livelib.ru/author/63979')
        result = parser.scrape_text()
    """

    def __init__(self, url,
                 user_agent='APIs-Google \
                    (+https://developers.google.com/webmasters/APIs-Google.html)'):
        """
        Constructor method for AuthorParser class.

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
        authorid = re.sub(r'-.*', '', re.sub(r'https\:\/\/www\.livelib\.ru\/author\/', '', self.url))
        name = soup.find("span", {"class": "header-profile-login"}) or ['null']
        origname = soup.find("span", {"class": "header-profile-status"}) or ['null']
        liked = soup.find("span", {"title": "Писатель понравился"}) or ['null']
        neutral = soup.find("span", {"title": "Отнеслись нейтрально"}) or ['null']
        disliked = soup.find("span", {"title": "Писатель не понравился"}) or ['null']
        favorite = soup.find("span", {"title": "Почитатели творчества"}) or ['null']
        reading = soup.find("span", {"title": "Читателей"}) or ['null']

        return pd.DataFrame(
            data ={
                'AuthorID':[authorid],
                'Name':[self.get_text(name)],
                'OriginalName':[self.get_text(origname)],
                'Liked':[self.get_text(liked)],
                'Neutral':[self.get_text(neutral)],
                'Disliked':[self.get_text(disliked)],
                'Favorite':[self.get_text(favorite)],
                'Reading':[self.get_text(reading)]
            }
        )

    def get_text(self, val):
        if isinstance(val, bs4.element.Tag):
            return re.sub('\n+', '', val.text)
        else:
            return str(*val)
