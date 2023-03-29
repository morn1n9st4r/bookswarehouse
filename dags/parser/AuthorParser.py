from bs4 import BeautifulSoup
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
        name = soup.find("span", {"class": "header-profile-login"})
        origname = soup.find("span", {"class": "header-profile-status"})
        liked = soup.find("span", {"title": "Писатель понравился"})
        neutral = soup.find("span", {"title": "Отнеслись нейтрально"})
        disliked = soup.find("span", {"title": "Писатель не понравился"})
        favorite = soup.find("span", {"title": "Почитатели творчества"})
        reading = soup.find("span", {"title": "Читателей"})
        print(authorid)
        print(name.text)
        print(origname.text)
        print(re.sub('\n+', '', liked.text))
        print(re.sub('\n+', '', neutral.text))
        print(re.sub('\n+', '', disliked.text))
        print(re.sub('\n+', '', favorite.text))
        print(re.sub('\n+', '', reading.text))
            