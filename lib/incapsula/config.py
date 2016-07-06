"""
This module should work out of the box.

If there are problems, then it may need to be manually configured.

Configure by typing the following variables into your web browser console and checking their output:
    navigator
        if this returns undefined then config['navigator']['exists'] = False, otherwise True.
    navigator.vendor
        if this returns undefined then config['navigator']['vendor'] = None, otherwise set to what ever value is
        returned, even if the value is an empty string.
    opera
        if this returns undefined then config['opera']['exists'] = False, otherwise True.
    ActiveXObject
        if this returns undefined then config['ActiveXObject']['exists'] = False, otherwise True.
    navigator.appName
        if this returns undefined then config['navigator']['appName'] = None, otherwise set to whatever value
        is returned, even if the value is an empty string.
    webkitURL
        if this returns undefined then config['webkitURL']['exists'] = False, otherwise True.
    _phantom
        if this returns undefined then config['_phantom']['exists'] = False, otherwise True.
"""

config = {
    'navigator': {
        'exists': True,
        'vendor': "",
        'appName': "Netscape"
    },
    'opera': {
        'exists': False
    },
    'webkitURL': {
        'exists': False,
    },
    '_phantom': {
        'exists': False
    },
    'ActiveXObject': {
        'exists': False
    }
}

host = ''

scheme = 'http'

# Edit these endpoints based on the url params following the host's incapsula resource url
# Ex. www.whoscored.com's incapsula resource is /_IncapsulaResource?SWJIYLWA=2977d8d74f63d7f8fedbea018b7a1d05&ns=1
# so each of the params is it's own key/value pair.
# If you want to add one manually, in the net panel of firebug, you will be looking for a request sent to _IncapsulaResource
# with a response body that contains a bunch of numbers and javascript code. Please look at incapsula/example_incap_response.txt
# for an example.
endpoints = {
    'www.whoscored.com': {
        'SWJIYLWA': '2977d8d74f63d7f8fedbea018b7a1d05',
        'ns': '1'
    },
    'www.bjs.com': {
        'SWJIYLWA': '2977d8d74f63d7f8fedbea018b7a1d05',
        'ns': '1'
    }
}
