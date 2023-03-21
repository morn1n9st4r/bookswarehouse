from multiprocessing import Pool
import tqdm
import pandas as pd

from BookParser import BookParser


def parse(url):
    bp = BookParser(url)
    try:
        df = bp.scrape_text()
        ret_array = df.loc[0, :].values.tolist()
        return [ret_array]
    except IndexError:
        pass


if __name__ == '__main__':

    postfix = 99

    filename = 'dags/parser/links_books.txt'
    target = 'dags/parser/livelib_books.csv'

    urls = []
    with open(filename) as f:
        urls = f.read().splitlines()

    with Pool(processes=4) as pool, tqdm.tqdm(total=len(urls)) as pbar:
        data_list = []
        for data in pool.imap_unordered(parse, urls):
            try:
                data_list.extend(data)
                pbar.update()
            except (IndexError, TypeError):
                pbar.update()
                continue

    df = pd.DataFrame(data_list, columns=['BookTitle', 'Author', 'ISBN', 'EditionYear', 'Pages', 'Size',
                                         'CoverType', 'Language', 'CopiesIssued', 'AgeRestrictions',
                                         'Genres', 'TranslatorName', 'Rating', 'HaveRead', 'Planned',
                                         'Reviews', 'Quotes', 'Series', 'Edition'])
    print(df.shape)
    print(df.sample(15).to_string())
    df.to_csv(target, encoding='utf-8-sig', index=False)