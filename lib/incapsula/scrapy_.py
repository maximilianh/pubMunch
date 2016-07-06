import random
import time

from scrapy import Request
from BeautifulSoup import BeautifulSoup

from methods import *


class IncapsulaMiddleware(object):

    cookie_count = 0
    logger = logging.getLogger('incapsula')

    def __init__(self, crawler):
        self.crawler = crawler
        self.priority_adjust = crawler.settings.getint('RETRY_PRIORITY_ADJUST')

    def _get_session_cookies(self, request):
        cookies_ = []
        for cookie_key, cookie_value in request.cookies.items():
            if 'incap_ses_' in cookie_key:
                cookies_.append(cookie_value)
        return cookies_

    def get_incap_cookie(self, request, response):
        extensions = load_plugin_extensions(navigator['plugins'])
        extensions.append(load_plugin(navigator['plugins']))
        extensions.extend(load_config())
        cookies = self._get_session_cookies(request)
        digests = []
        for cookie in cookies:
            digests.append(simple_digest(",".join(extensions) + cookie))
        res = ",".join(extensions) + ",digest=" + ",".join(str(digests))
        cookie = create_cookie('___utmvc', res, 20, request.url)
        return cookie

    def process_response(self, request, response, spider):
        if not request.meta.get('incap_set', False):
            soup = BeautifulSoup(response.body.decode('ascii', errors='ignore'))
            meta = soup.find('meta', {'name': 'robots'})
            if not meta:
                return response
            self.crawler.stats.inc_value('incap_blocked')
            self.logger.info('cracking incapsula blocked resource <{}>'.format(request.url))

            # Set generated cookie to request more cookies from incapsula resource
            cookie = self.get_incap_cookie(request, response)
            scheme, host = urlparse.urlsplit(request.url)[:2]
            url = '{scheme}://{host}/_Incapsula_Resource?SWKMTFSR=1&e={rdm}'.format(scheme=scheme, host=host, rdm=random.random())
            cpy = request.copy()
            cpy.meta['incap_set'] = True
            cpy.meta['org_response'] = response
            cpy.meta['org_request'] = request
            cpy.cookies.update(cookie)
            cpy._url = url
            cpy.priority = request.priority + self.priority_adjust
            return cpy
        elif request.meta.get('incap_set', False) and not request.meta.get('incap_request_1', False):
            timing = []
            start = now_in_seconds()
            timing.append('s:{}'.format(now_in_seconds() - start))
            code = get_obfuscated_code(request.meta.get('org_response').body.decode('ascii', errors='ignore'))
            parsed = parse_obfuscated_code(code)
            resource1, resource2 = get_resources(parsed, response.url)[1:]
            cpy = request.copy()
            cpy._url = str(resource1)
            cpy.meta['resource2'] = resource2
            cpy.meta['tstart'] = start
            cpy.meta['timing'] = timing
            cpy.meta['incap_request_1'] = True
            cpy.priority = request.priority + self.priority_adjust
            return cpy
        elif request.meta.get('incap_request_1', False) and request.meta.get('incap_completed', False):
            timing = request.meta.get('timing', [])
            resource2 = request.meta.get('resource2')
            start = request.meta.get('tstart')
            timing.append('c:{}'.format(now_in_seconds() - start))
            time.sleep(0.02)
            timing.append('r:{}'.format(now_in_seconds() - start))
            cpy = request.copy()
            cpy.meta['completed_incap'] = True
            cpy._url = str(resource2) + urllib.quote('complete ({})'.format(",".join(timing)))
            cpy.priority = request.priority + self.priority_adjust
            return cpy
        self.crawler.stats.inc_value('incap_cracked')
        cpy = request.meta.get('org_request').copy()
        cpy.cookies = request.cookies
        cpy.dont_filter = True
        cpy.priority = request.priority + self.priority_adjust
        return cpy

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)
