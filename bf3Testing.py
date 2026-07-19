import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

mainURL = 'https://battlelog.battlefield.com'
statsURL = ''
playerName = 'BigE1293'
playerId = ''

headers = {'User-Agent': 'Mozilla/5.0'}

playerURL = mainURL + '/bf3/user/' + playerName + '/'

results = requests.get(playerURL,headers=headers)
soup = BeautifulSoup(results.text, "html.parser")

div = soup.find("div", {"class": "soldier-name"})
endpoint = div.find('a')['href']
regex = re.findall(r'/\d+/pc', endpoint)
playerId = regex[0][1:-3]

overviewAPI = 'https://battlelog.battlefield.com/bf3/overviewPopulateStats/' + str(playerId) + '/None/1/'
weaponsAPI = 'https://battlelog.battlefield.com/bf3/weaponsPopulateStats/' + str(playerId) + '/1/'
vehiclesAPI = 'https://battlelog.battlefield.com/bf3/vehiclesPopulateStats/' + str(playerId) + '/1/'
