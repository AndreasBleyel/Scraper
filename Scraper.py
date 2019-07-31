import requests
from bs4 import BeautifulSoup
import re
import datetime
import hashlib


class Scraper:
    def __init__(self):
        self.events = []
        self.hash = hashlib.sha256()

    def get_data(self):
        self.get_kufstein_city_events()
        self.get_fh_events()
        return self.events

    def parse_date(self, date_string):
        matches = re.search(r'([0-9]{2})\.([0-9]{2})\.(2[0-9]{3})', date_string)
        year = int(matches[3])
        month = int(matches[2])
        day = int(matches[1])
        return datetime.datetime(year, month, day)

    def parse_location(self, location_string):
        matches = re.search(r'Ort: (.*)$', location_string.strip())

        if matches:
            return matches[1]
        else:
            return ""

    def create_hash(self, title, location, date):
        self.hash = hashlib.sha3_256()
        self.hash.update(bytes(title, 'utf-8'))
        self.hash.update(bytes(location, 'utf-8'))
        self.hash.update(bytes(str(date.time()), 'utf-8'))

    def get_kufstein_city_events(self):

        LINK_PRE = 'http://www.kufstein.at'

        response = requests.get("http://www.kufstein.at/de/events.html")
        soup = BeautifulSoup(response.content, 'html.parser')

        articles, links = [], []
        articles = soup.select('article')

        for article in articles:
            link_tag = article['data-href']
            links.append(LINK_PRE + link_tag)

        for link in links:
            response = requests.get(link)
            soup = BeautifulSoup(response.content, 'html.parser')

            title = soup.title.text
            content = soup.select('.content')
            date = self.parse_date(content[0].contents[1].text.strip())
            location = content[0].contents[2].text.replace('Wo:', '')

            description = soup.select('.description')
            text = description[0].text.strip()

            if text == '':
                text = 'no text available'

            # print('Title: {0}, Date: {1}, Location: {2}, Text: {3}, Link: {4}'.format(title, date, location, text, link))

            self.create_hash(title, location, date)

            self.events.append({
                "name": title,
                "date": date,
                "location": location,
                "link": link,
                "short": text,
                "source": "Stadt Kufstein Events",
                "identifier": self.hash.hexdigest()
            })

    def get_fh_events(self):
        response = requests.get("https://www.fh-kufstein.ac.at/ger/Veranstaltungen")
        soup = BeautifulSoup(response.content, 'html.parser')

        items = soup.select('.news-item')
        for item in items:
            title_tag = item.select_one('.title')
            date_tag = item.select_one('.date')
            link_tag = item.select_one('a')
            short_tag = item.select_one('.eztext-field')

            title = title_tag.contents[0].strip()
            location = self.parse_location(date_tag.contents[2])
            date = self.parse_date(date_tag.contents[0])

            self.create_hash(title, location, date)

            self.events.append({
                "name": title,
                "date": date,
                "location": location,
                "link": "https://www.fh-kufstein.ac.at" + link_tag['href'].strip(),
                "short": short_tag.contents[0].strip(),
                "source": "FH Kufstein Homepage",
                "identifier": self.hash.hexdigest()
            })

