from bs4 import BeautifulSoup
import bs4
from urllib.request import urlopen, Request
import pandas as pd
import re


class AuthorParser:

    def __init__(self, url,
                 userAgent='APIs-Google (+https://developers.google.com/webmasters/APIs-Google.html)'):
        
                
        """
        Constructor method for AuthorParser class.

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
        authorid = re.sub(r'-.*', '', re.sub(r'https\:\/\/www\.livelib\.ru\/author\/', '', self.url))
        name = soup.find("span", {"class": "header-profile-login"}) or ['None']
        origname = soup.find("span", {"class": "header-profile-status"}) or ['None']
        liked = soup.find("span", {"title": "Писатель понравился"}) or [0]
        neutral = soup.find("span", {"title": "Отнеслись нейтрально"}) or [0]
        disliked = soup.find("span", {"title": "Писатель не понравился"}) or [0]
        favorite = soup.find("span", {"title": "Почитатели творчества"}) or [0]
        reading = soup.find("span", {"title": "Читателей"}) or [0]

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
        if type(val) == bs4.element.Tag:
            return re.sub('\n+', '', val.text)
        else: 
            return str(*val)