from __future__ import print_function
import sys
import os
root = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
print(root)

sys.path.append(root)

import logging
from incapsula import crack, IncapSession
from incapsula.requests_ import incap_blocked
import requests


target_url = 'https://www.karger.com/Article/Abstract/437330'


session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0'})


def test_blocked():
    """
    Check to make sure that the resource is blocked by incapsula.
    :return:
    """
    r = session.get(target_url)
    with open('blocked.html', 'wb') as f:
        f.write(r.content)
    return incap_blocked(r)


def unblock():
    """
    Unblock the target url/session
    :return:
    """
    r = session.get(target_url)
    r = crack(session, r)
    with open('unblocked.html', 'wb') as f:
        f.write(r.content)
    return incap_blocked(r)


def test_incap_session():
    """
    Unblock using the session wrapper.
    :return:
    """
    sess = IncapSession()
    r = sess.get(target_url)
    return incap_blocked(r)


if __name__ == '__main__':
    logging.basicConfig(level=10)
    is_blocked = test_blocked()
    print('incap blocked:', is_blocked)
    unblock()
    is_blocked = test_blocked()
    print('incap blocked after unblock:', is_blocked)
    print(session.cookies)

    # incap_session_blocked = test_incap_session()
    # print 'incap session blocked:', incap_session_blocked
