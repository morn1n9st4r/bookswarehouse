from urllib.request import urlopen, Request
from bs4 import BeautifulSoup


class NewBooksParser:

    """
    The NewBooksParser class is used to scrape links to individual book pages
    from the livelib.ru website's new books page.

    Attributes:
        url (str): The url of the webpage to scrape.
        user_agent (str): The user agent string to use in the request headers.

    Methods:
        __init__(url,
                 user_agent='APIs-Google \
                    (+https://developers.google.com/webmasters/APIs-Google.html)'):
            Initializes a new instance of the NewBooksParser class with the specified url
            and user agent.

        scrape_text():
            Scrapes the links to individual book pages from the new books page
            and returns them as a list of strings.

    Usage:
        parser = NewBooksParser(url='https://www.livelib.ru/books/new/all')
        links = parser.scrape_text()
    """

    def __init__(self, url,
                 user_agent='APIs-Google \
                    (+https://developers.google.com/webmasters/APIs-Google.html)'):

        """
        Constructor method for NewBooksParser class.

        Parameters:
        url (str): The url of the webpage to scrape.
        user_agent (str): The user agent string to use in the request headers.
                         Defaults to a Google API user agent.
        """

        self.url = url
        self.user_agent = user_agent

    def scrape_text(self):

        """
        Method that scrapes the links to individual book pages from the new books page.

        Returns:
        list: A list of strings containing the hrefs of the individual book pages.
        """

        req = Request(self.url, headers={'User-Agent': self.user_agent})
        html = urlopen(req).read().decode('utf-8')
        soup = BeautifulSoup(html, "html.parser")

        # scrape hrefs of books from new books page
        mylinks = soup.find_all("a", {"class": "book-item__link"})
        mylinks_hrefs = [f"https://www.livelib.ru{link['href']}" for link in mylinks]
        return mylinks_hrefs
