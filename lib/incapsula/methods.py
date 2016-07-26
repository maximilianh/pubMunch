import datetime
import re
import os
import logging
import json
import urllib.request, urllib.parse, urllib.error
import urllib.parse

from .config import config

logger = logging.getLogger('incapsula')

_nav_fp = os.path.join(os.path.dirname(__file__), 'navigator.json')

logger.debug('loading navigator.json')
with open(_nav_fp, 'r') as f:
    navigator = json.loads(f.read().decode('ascii', errors='ignore'))


def load_plugin_extensions(plugins):
    _extensions = []
    for k, v in list(plugins.items()):
        logger.debug('calculating plugin_extension key={}'.format(k))
        if not isinstance(v, dict):
            continue
        filename = v.get('filename')
        if not filename:
            _extensions.append(urllib.parse.quote('plugin_ext=plugins[i] is undefined'))
            break
        if len(filename.split('.')) == 2:
            extension = filename.split('.')[-1]
            if extension not in _extensions:
                _extensions.append(extension)
    return [urllib.parse.quote('plugin_ext={}'.format(x)) for x in _extensions]


def load_plugin(plugins):
    for k, v in list(plugins.items()):
        logger.debug('calculating plugin key={}'.format(k))
        if '.' in v.get('filename', ''):
            filename, extension = v['filename'].split('.')
            return urllib.parse.quote('plugin={}'.format(extension))


def load_config(conf=None):
    conf = config if not conf else conf
    data = []
    if conf['navigator']['exists']:
        data.append(urllib.parse.quote('navigator=true'))
    else:
        data.append(urllib.parse.quote('navigator=false'))
    data.append(urllib.parse.quote('navigator.vendor=' + conf['navigator']['vendor']))
    if conf['navigator']['vendor'] is None:
        data.append(urllib.parse.quote('navigator.vendor=nil'))
    else:
        data.append(urllib.parse.quote('navigator.vendor=' + conf['navigator']['vendor']))
    if conf['opera']['exists']:
        data.append(urllib.parse.quote('opera=true'))
    else:
        data.append(urllib.parse.quote('opera=false'))
    if conf['ActiveXObject']['exists']:
        data.append(urllib.parse.quote('ActiveXObject=true'))
    else:
        data.append(urllib.parse.quote('ActiveXObject=false'))
    data.append(urllib.parse.quote('navigator.appName=' + conf['navigator']['appName']))
    if conf['navigator']['appName'] is None:
        data.append(urllib.parse.quote('navigator.appName=nil'))
    else:
        data.append(urllib.parse.quote('navigator.appName=' + conf['navigator']['appName']))
    if conf['webkitURL']['exists']:
        data.append(urllib.parse.quote('webkitURL=true'))
    else:
        data.append(urllib.parse.quote('webkitURL=false'))
    if len(navigator.get('plugins', {})) == 0:
        data.append(urllib.parse.quote('navigator.plugins.length==0=false'))
    else:
        data.append(urllib.parse.quote('navigator.plugins.length==0=true'))
    if not navigator.get('plugins'):
        data.append(urllib.parse.quote('navigator.plugins.length==0=nil'))
    else:
        data.append(
            urllib.parse.quote(
                'navigator.plugins.length==0=' + 'false' if len(navigator.get('plugins', {})) == 0 else 'true'))
    if conf['_phantom']['exists']:
        data.append(urllib.parse.quote('_phantom=true'))
    else:
        data.append(urllib.parse.quote('_phantom=false'))
    return data


def simple_digest(mystr):
    res = 0
    for c in mystr:
        res += ord(c)
    return res


def create_cookie(name, value, seconds, url):
    cookie = {
        'version': '0',
        'name': name,
        'value': value,
        'port': None,
        'domain': urllib.parse.urlsplit(url).netloc,
        'path': '/',
        'secure': False,
        'expires': now_in_seconds() + seconds,
        'discard': True,
        'comment': None,
        'comment_url': None,
        'rest': {},
        'rfc2109': False
    }
    return cookie


def now_in_seconds():
    return (datetime.datetime.now() - datetime.datetime(1970, 1, 1)).total_seconds()


def get_resources(code, url):
    scheme, host = urllib.parse.urlsplit(url)[:2]
    resources = re.findall('(/_Incapsula_Resource.*?)\"', code)
    logger.debug('resources found: {}'.format(resources))
    return [scheme + '://' + host + r for r in resources]


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]


def get_obfuscated_code(html):
    code = re.findall('var\s?b\s?=\s?\"(.*?)\"', html)
    return code[0]


def parse_obfuscated_code(code):
    data = []
    for chunk in chunks(code, 2):
        data.append(int("".join(chunk), 16))
    code = [chr(x) for x in data]
    deobfuscated_code = ''.join(code)
    logger.debug('deobfuscated_code={}'.format(deobfuscated_code))
    return deobfuscated_code
