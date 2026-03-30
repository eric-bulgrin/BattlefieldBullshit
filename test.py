import sqlite3
import json
import requests
import time
from datetime import datetime

playerId = 940870640
platform = 'pc'

warsawRibbonStats = 'https://battlelog.battlefield.com/bf4/warsawawardspopulate/' + str(playerId)
if platform == 'pc':
  warsawRibbonStats = warsawRibbonStats + '/1/'
elif platform == 'ps4':
  warsawRibbonStats = warsawRibbonStats + '/32/'
elif platfirm == 'xb1':
  warsawRibbonStats = warsawRibbonStats + '/64/'

ribbonStatsResponse = requests.get(warsawRibbonStats)
ribbonStatsJson = ribbonStatsResponse.json()
ribbonStatsData = ribbonStatsJson['data']
ribbonStats = ribbonStatsData['ribbonAwardByCode']

value = ribbonStats['xp0rFD']['timesTaken']
print(value)