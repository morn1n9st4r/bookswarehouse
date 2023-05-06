import re
import pandas as pd
from urllib.request import urlopen, Request
import bs4
from bs4 import BeautifulSoup


class BookParser:

    """
    The BookParser class is used to scrape information about a book
    from the livelib.ru website.

    Attributes:
        url (str): The url of the webpage to scrape.
        user_agent (str): The user agent string to use in the request headers.

    Methods:
        __init__(url, 
                 user_agent='APIs-Google \
                    (+https://developers.google.com/webmasters/APIs-Google.html)'):
            Initializes a new instance of the BookParser class with the specified url
            and user agent.

        scrape_text():
            Scrapes block of information about the book from the webpage
            and delegates another method to scrape specific information
            and return the result as a pandas DataFrame.

        aquire_df_from_book():
            Concatenates parsed information into one pandas DataFrame.

        parse_title_and_author():
            Scrapes information about the book from the webpage and returns the result
            as a pandas DataFrame.

        parse_info():
            Scrapes information about the book from the webpage and returns the result
            as a pandas DataFrame.

        parse_rating():
            Scrapes information about the book from the webpage and returns the result
            as a pandas DataFrame.

        parse_stat():
            Scrapes information about the book from the webpage and returns the result
            as a pandas DataFrame.

        parse_edition():
            Scrapes information about the book from the webpage and returns the result
            as a pandas DataFrame.

        get_publisher():
            Scrapes information about the book from the webpage and returns the result
            as a pandas DataFrame.

        get_text(val):
            Utility method to extract text from a Beautiful Soup tag.

    Usage:
        parser = BookParser(url='https://www.livelib.ru/book/85378')
        result = parser.scrape_text()
    """

    def __init__(self, url,
                 user_agent='APIs-Google \
                    (+https://developers.google.com/webmasters/APIs-Google.html)'):

        """
        Constructor method for BookParser class.

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

        book_id = re.sub(r'-.*', '', re.sub(r'https\:\/\/www\.livelib\.ru\/book\/', '', self.url))
        autor_link = soup.find_all("a", {"class": "bc-author__link"})[0]['href']
        autor_id = re.sub(r'-.*', '',re.sub(r'\/author\/', '', autor_link))


        h1_title = re.sub(' +', ' ', re.sub('\n+', '\n',
            soup.select('h1.bc__book-title')[0].get_text().strip())
            )
        h2_author = re.sub(' +', ' ', re.sub('\n+', '\n',
            soup.select('h2.bc-author')[0].get_text().strip())
            )
        bc_info = re.sub(' +', ' ', re.sub('\n+', '\n',
            soup.select('div.bc-info')[0].get_text().strip())
            )
        bc_rating = re.sub(' +', ' ', re.sub('\n+', '\n',
            soup.select('div.bc-rating')[0].get_text().strip())
            )
        bc_stat = re.sub(' +', '', re.sub('\n+', '\n',
            soup.select('div.bc-stat')[0].get_text().strip())
            )
        bc_edition = re.sub(' +', ' ', re.sub('\n+', '\n',
            soup.select('table.bc-edition')[0].get_text().strip())
            )

        return self.aquire_df_from_book(book_id, autor_id, h1_title,
                                        h2_author, bc_info, bc_rating,
                                        bc_stat, bc_edition, soup)

    def aquire_df_from_book(self, book_id, autor_id, h1_title,
                            h2_author, bc_info, bc_rating,
                            bc_stat, bc_edition, soup):
        return pd.concat([pd.DataFrame(data={'ID' : [book_id]}),
                          self.parse_title_and_author(h1_title, h2_author),
                          pd.DataFrame(data={'AuthorID' : [autor_id]}),
                          self.parse_info(bc_info),
                          self.parse_rating(bc_rating),
                          self.parse_stat(bc_stat),
                          self.parse_edition(bc_edition, soup)], axis=1)

    def parse_title_and_author(self, h1_title, h2_author):
        data = {
            'BookTitle': [h1_title],
            'Author': [h2_author]
        }
        df = pd.DataFrame(data)
        return df

    def parse_info(self, bc_info):
        # first we take whole line with isbn's (one book can have few)
        # next step we take just numbers

        regex_isbn = r'ISBN: [-X0-9]{10,18}.+'
        regex_isbn_nums = r'[-X0-9]{10,18}'

        regex_year = r'\bГод.издания: \d+'

        # LiveLib has no one definite standart of writing of pages
        # it could be represented in next 3 forms:

        regex_pages_v1 = r'Количество.страниц.*\d+'
        regex_pages_v2 = r'Страниц.+\d+'
        regex_pages_v3 = r'\d+.стр'
        regex_pages_v4 = r'Кол-во.страниц.*\d+'

        regex_cover = r'(Тип обложки: .+)|(\n.+(П|п)ереплет.+)|(\n.+((О|о)бложка).+)'

        regex_size = r'Формат: .+'
        regex_size_v2 = r'Размер: .+'
        regex_language = r'Язык: \n .+'

        regex_books = r'\bТираж.? ((\d+\s?)+|\d+)'
        regex_restrictions = r'Возрастные.ограничения: \d+'
        regex_genres = r'(?s)(?<=Жанры:).*?(?=Теги:)'
        regex_translator = r'Перевод[чик]*[и]*: .+'

        # isbn's
        try:
            pattern = re.compile(regex_isbn, re.UNICODE)
            isbn = pattern.findall(bc_info)[0]

            pattern = re.compile(regex_isbn_nums, re.UNICODE)
            isbn = pattern.findall(isbn)
        except (IndexError, TypeError):
            isbn = 'null'

        try:
            pattern = re.compile(regex_year, re.UNICODE)
            year = re.search(r"\d+", pattern.findall(bc_info)[0])[0]
        except (IndexError, TypeError):
            year = 'null'
        # pages
        # dangerous category since it could be not listed
        # need try-except clause

        pattern = re.compile(regex_pages_v1, re.UNICODE)
        pages = pattern.findall(bc_info)
        try:
            if not pages:
                pattern = re.compile(regex_pages_v2, re.UNICODE)
                pages = pattern.findall(bc_info)

                if not pages:
                    pattern = re.compile(regex_pages_v3, re.UNICODE)
                    pages = pattern.findall(bc_info)
                    
                    if not pages:
                        pattern = re.compile(regex_pages_v4, re.UNICODE)
                        pages = pattern.findall(bc_info)

            pages = re.search(r"\d+", ''.join(pages[0]))[0]
        except (IndexError, TypeError):
            pages = 'null'

        pattern = re.compile(regex_books, re.UNICODE)
        copies = pattern.findall(bc_info)

        # same with copies numbers
        try:
            copies = re.search(r"(\d+\s?)+", str(copies[0]))[0].strip()
        except (IndexError, TypeError):
            copies = 'null'

        pattern = re.compile(regex_size, re.UNICODE)
        size = pattern.findall(bc_info)
        try:
            if not size:
                pattern = re.compile(regex_size_v2, re.UNICODE)
                size = pattern.findall(bc_info)
            size = size[0]
            size = re.sub("Размер:", "", 
                        re.sub("Формат:", "",
                            re.sub("Возрастные ограничения: .+", "", size)
                            )
                        ).strip()
        except (IndexError, TypeError):
            size = 'null'

        pattern = re.compile(regex_cover, re.UNICODE)
        cover = pattern.findall(bc_info)

        try:
            cover = re.search(r"((М|м)ягкий)|((М|м)ягкая)|((Т|т)в(е|ё)рдый)|((Т|т)в(е|ё)рдая)",
                              str(cover[0]))[0]
        except (IndexError, TypeError):
            cover = 'null'

        pattern = re.compile(regex_language, re.UNICODE)
        language = pattern.findall(bc_info)

        try:
            language = re.search(r" .+", str(language[0]))[0].strip()
        except (IndexError, TypeError):
            language = 'null'

        pattern = re.compile(regex_restrictions, re.UNICODE)

        try:
            restrictions = re.search(r"\d+", pattern.findall(bc_info)[0])[0]
        except (IndexError, TypeError):
            restrictions = 'null'

        pattern = re.compile(regex_genres, re.UNICODE)
        try:
            genres = pattern.findall(bc_info)[0]
            genres = re.sub("\xa0", '', genres)
            genres = re.sub("\u2002", '', genres)
            genres = re.sub(" \n ", '', genres)
            genres = re.sub("\n", '', genres)
            genres = re.sub(r"№\d+в", '', genres)
        except (IndexError, TypeError):
            genres = 'null'
        # and translator name
        # (book can be not translated)

        pattern = re.compile(regex_translator, re.UNICODE)
        translator = pattern.findall(bc_info)
        try:
            translator = re.sub(r'Перевод[чик]*[и]*: ', '', translator[0]).strip()
            translator = translator.split(',')
        except (IndexError, TypeError):
            translator = 'null'

        data = {
            'ISBN': [isbn],
            'EditionYear': [year],
            'Pages': [pages],
            'Size': [size],
            'CoverType': [cover],
            'Language': [language],
            'CopiesIssued': [copies],
            'AgeRestrictions': [restrictions],
            'Genres': [genres.split(',')],
            'TranslatorName': [translator]
        }
        df = pd.DataFrame(data)

        return df

    def parse_rating(self, bc_rating):
        splitted_rating = re.split("\n", re.sub(u"\xa0", '', bc_rating))
        rating = splitted_rating[0]
        data = {
            'Rating': [rating]
        }
        df = pd.DataFrame(data)

        return df

    def parse_stat(self, bc_stat):
        splitted_stat = re.split("\n", re.sub(u"\xa0", '', bc_stat))
        try:
            have_read = splitted_stat[0]
        except (IndexError, TypeError):
            have_read = 'null'

        try:
            planned = splitted_stat[2]
        except (IndexError, TypeError):
            planned = 'null'

        try:
            reviews = splitted_stat[4]
        except (IndexError, TypeError):
            reviews = 'null'

        try:
            quotes = splitted_stat[7]
        except (IndexError, TypeError):
            quotes = 'null'

        data = {
            'HaveRead': [have_read],
            'Planned': [planned],
            'Reviews': [reviews],
            'Quotes': [quotes]
        }
        df = pd.DataFrame(data)

        return df

    def parse_edition(self, bc_edition, soup):
        splitted_edition = re.split("\n", re.sub(u"\xa0", '', bc_edition))
        try:
            series = splitted_edition[1].strip()
        except (IndexError, TypeError):
            series = 'null'

        try:
            edition = re.sub(r'-.*', '', re.sub(r'\/publisher\/', '', self.get_publisher(soup) ))
        except (IndexError, TypeError):
            edition = 'null'
        data = {
            'Series': [series],
            'PublisherID': [edition]
        }
        df = pd.DataFrame(data)

        return df
        
    def get_publisher(self, soup):
        pubs = soup.findAll("a", {"class": "bc-edition__link"})
        for pub in pubs:
            if "publisher" in pub['href']:
                return pub['href']

    def get_text(self, val):
        if isinstance(val, bs4.element.Tag):
            return re.sub('\n+', '', val.text)
        else: 
            return str(*val)
