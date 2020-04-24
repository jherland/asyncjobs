#!/usr/bin/env python3
# Fetch a random wikipedia article and follow links from it
import asyncio
from asyncjobs import Job, Scheduler
import requests
import sys
import xml.etree.ElementTree as ET

num_articles = 10
num_workers = 4


class Article:
    base_url = 'https://en.wikipedia.org'
    random_url = base_url + '/wiki/Special:Random'

    def __init__(self, url):
        resp = requests.get(url=url, allow_redirects=True)
        self.html = ET.fromstring(resp.content)

    def title(self):
        ret = self.html.find('head').find('title').text
        if ret.endswith('- Wikipedia'):
            ret = ret.rsplit('-', 1)[0].strip()
        return ret

    def hrefs(self):
        for a in self.html.iterfind('.//p//a'):
            href = a.attrib.get('href', '')
            if href.startswith('/wiki/'):
                yield self.base_url + href


class Fetcher(Job):
    def __init__(self, url, level=1, **kwargs):
        super().__init__(name=url, **kwargs)
        self.url = url
        self.level = level

    def thread_func(self):
        indent = '  ' * self.level
        print(f'{indent}  fetching {self.url}...')
        a = Article(self.url)
        title = a.title()
        hrefs = list(a.hrefs())
        print(f'{indent}* [{title}] links to {len(hrefs)} articles')
        return title, hrefs

    async def __call__(self, scheduler):
        title, hrefs = await super().__call__(scheduler)
        for href in hrefs:
            if len(scheduler.jobs) < num_articles and href not in scheduler:
                scheduler.add(self.__class__(href, self.level + 1))


events = []
scheduler = Scheduler(workers=num_workers, event_handler=events.append)
scheduler.add(Fetcher(Article.random_url))
asyncio.run(scheduler.run())
if 'plot' in sys.argv:
    from asyncjobs.plot_schedule import plot_schedule

    plot_schedule(
        f'Fetch {num_articles} Wikipedia articles with {num_workers} workers',
        events,
    ).show()
