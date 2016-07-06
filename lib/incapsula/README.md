# Description

This module is used to wrap any request to a webpage blocked by incapsula.

# Usage

## With Requests

```
from incapsula import crack
import requests

session = requests.Session()
response = session.get('http://example.com')  # url is blocked by incapsula
response = crack(session, response)  # url is no longer blocked by incapsula
```

```
from incapsula import IncapSession
session = IncapSession()
response = session.get('http://example.com')  # url is not blocked by incapsula
```

## With Scrapy

### settings.py

```
DOWNLOADER_MIDDLEWARES = {
    'incapsula.IncapsulaMiddleware': 900
}
```

# Setup

`pip install incapsula-cracker`

There should be no problems using incapsula-cracker right out of the box.

If there are issues, try the following

* Open incapsula/serialize.html in browser
* Copy and paste the json data into incapsula/navigator.json

# Notes

* config.py, navigator.json, and serialize.html have all only been tested using firefox. 
* As of now, this is only proven to work with bjs.com.
* I understand that there's minimal commenting and that's because I'm not sure exactly why incapsula is sending requests to certain pages other than to obtain cookies. This is just a literal reverse engineer of incapsulas javascript code.
* If you would like to contribute or if there are any other sites that you would like me to add, contact me at sdscdeveloper@gmail.com.