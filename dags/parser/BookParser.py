from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
import pandas as pd
import re


class BookParser:
    """
        BookParser class if created to parse book page on LiveLib website.
        Next features are being scraped (if present on page, None if not):
            BookTitle, Author, ISBN,
            EditionYear, Pages, CopiesIssued,
            AgeRestrictions, Genres,
            TranslatorName,
            Rating,
            HaveRead, Planned, Reviews, Quotes,
            Series, Edition
        Attributes:
            url: A string url to book.
            userAgent: A string identification of application for server.
    """

    def __init__(self, url,
                 userAgent='APIs-Google (+https://developers.google.com/webmasters/APIs-Google.html)'):
        self.url = url
        self.userAgent = userAgent

    def scrape_text(self):
        req = Request(self.url, headers={'User-Agent': self.userAgent})
        html = urlopen(req).read().decode('utf-8')
        soup = BeautifulSoup(html, "html.parser")

        # scrape some particular areas of page

        h1_title = re.sub(' +', ' ', re.sub('\n+', '\n', soup.select('h1.bc__book-title')[0].get_text().strip()))
        h2_author = re.sub(' +', ' ', re.sub('\n+', '\n', soup.select('h2.bc-author')[0].get_text().strip()))
        bc_info = re.sub(' +', ' ', re.sub('\n+', '\n', soup.select('div.bc-info')[0].get_text().strip()))
        bc_rating = re.sub(' +', ' ', re.sub('\n+', '\n', soup.select('div.bc-rating')[0].get_text().strip()))
        bc_stat = re.sub(' +', '', re.sub('\n+', '\n', soup.select('div.bc-stat')[0].get_text().strip()))
        bc_edition = re.sub(' +', ' ', re.sub('\n+', '\n', soup.select('table.bc-edition')[0].get_text().strip()))

        return self.aquire_df_from_book(h1_title, h2_author, bc_info, bc_rating, bc_stat, bc_edition)

    def aquire_df_from_book(self, h1_title, h2_author, bc_info, bc_rating, bc_stat, bc_edition):
        return pd.concat([self.parse_title_and_author(h1_title, h2_author),
                          self.parse_info(bc_info),
                          self.parse_rating(bc_rating),
                          self.parse_stat(bc_stat),
                          self.parse_edition(bc_edition)], axis=1)

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

        regex_pages_v1 = r'\bКоличество.страниц: \d+'
        regex_pages_v2 = r'\bСтраниц: \d+'
        regex_pages_v3 = r'\d+\s*стр'

        regex_cover = r'(Тип обложки: .+)|(\n.+(П|п)ереплет.+)|(\n.+((О|о)бложка).+)'

        regex_size = r'Формат: .+'
        regex_language = r'Язык: \n .+'

        regex_books = r'\bТираж.? ((\d+\s?)+|\d+)'
        regex_restrictions = r'\Возрастные.ограничения: \d+'
        regex_genres = r'(?s)(?<=Жанры:).*?(?=Теги:)'
        regex_translator = r'Перевод[чик]*[и]*: .+'

        # isbn's
        try:
            pattern = re.compile(regex_isbn, re.UNICODE)
            isbn = pattern.findall(bc_info)[0]

            pattern = re.compile(regex_isbn_nums, re.UNICODE)
            isbn = pattern.findall(isbn)
        except (IndexError, TypeError):
            isbn = 'None'

        try:
            pattern = re.compile(regex_year, re.UNICODE)
            year = re.search(r"\d+", pattern.findall(bc_info)[0])[0]
        except (IndexError, TypeError):
            year = 'None'
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

            pages = re.search(r"\d+", ''.join(pages[0]))[0]
        except (IndexError, TypeError):
            pages = 'None'

        pattern = re.compile(regex_books, re.UNICODE)
        copies = pattern.findall(bc_info)

        # same with copies numbers

        try:
            copies = re.search(r"(\d+\s?)+", str(copies[0]))[0].strip()
        except (IndexError, TypeError):
            copies = 'None'

        pattern = re.compile(regex_size, re.UNICODE)
        size = pattern.findall(bc_info)

        try:
            size = size[0]
            size = re.sub("Формат:", "", re.sub("Возрастные ограничения: .+", "", size)).strip()
        except (IndexError, TypeError):
            size = 'None'

        pattern = re.compile(regex_cover, re.UNICODE)
        cover = pattern.findall(bc_info)

        try:
            cover = re.search(r"((М|м)ягкий)|((М|м)ягкая)|((Т|т)в(е|ё)рдый)|((Т|т)в(е|ё)рдая)", str(cover[0]))[0]
        except (IndexError, TypeError):
            cover = 'None'

        pattern = re.compile(regex_language, re.UNICODE)
        language = pattern.findall(bc_info)

        try:
            language = re.search(r" .+", str(language[0]))[0].strip()
        except (IndexError, TypeError):
            language = 'None'

        pattern = re.compile(regex_restrictions, re.UNICODE)

        try:
            restrictions = re.search(r"\d+", pattern.findall(bc_info)[0])[0]
        except (IndexError, TypeError):
            restrictions = 'None'

        pattern = re.compile(regex_genres, re.UNICODE)
        try:
            genres = pattern.findall(bc_info)[0]
            genres = re.sub(u"\xa0", '', genres)
            genres = re.sub(u"\u2002", '', genres)
            genres = re.sub(u" \n ", '', genres)
            genres = re.sub(u"\n", '', genres)
            genres = re.sub(r"№\d+в", '', genres)
        except (IndexError, TypeError):
            genres = 'None'
        # and translator name
        # (book can be not translated)

        pattern = re.compile(regex_translator, re.UNICODE)
        translator = pattern.findall(bc_info)
        try:
            translator = re.sub(r'Перевод[чик]*[и]*: ', '', translator[0]).strip()
            translator = translator.split(',')
        except (IndexError, TypeError):
            translator = 'None'

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
            have_read = 'None'

        try:
            planned = splitted_stat[2]
        except (IndexError, TypeError):
            planned = 'None'

        try:
            reviews = splitted_stat[4]
        except (IndexError, TypeError):
            reviews = 'None'

        try:
            quotes = splitted_stat[7]
        except (IndexError, TypeError):
            quotes = 'None'

        data = {
            'HaveRead': [have_read],
            'Planned': [planned],
            'Reviews': [reviews],
            'Quotes': [quotes]
        }
        df = pd.DataFrame(data)

        return df

    def parse_edition(self, bc_edition):
        splitted_edition = re.split("\n", re.sub(u"\xa0", '', bc_edition))

        try:
            series = splitted_edition[1].strip()
        except (IndexError, TypeError):
            series = 'None'
        # index 3 if not part of the cycle
        # e.g. 'Game of Thrones' is part of 'Song of Fire and Ice'
        # that's why we take index 5

        try:
            edition = splitted_edition[5] if len(splitted_edition) > 4 else splitted_edition[3]
        except (IndexError, TypeError):
            edition = 'None'
        data = {
            'Series': [series],
            'Edition': [edition]
        }
        df = pd.DataFrame(data)

        return df
