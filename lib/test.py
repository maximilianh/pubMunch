from incapsula import crack
import requests
session = requests.Session()
response = session.get('http://www.karger.com/Article/Abstract/437330')  # url is blocked by incapsula
response = crack(session, response)  # url is no longer blocked by incapsula
response = session.get('http://www.karger.com/ProdukteDB/miscArchiv/000/437/330/000437330_sm.html')  # url is blocked by incapsula
print response.content
