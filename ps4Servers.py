import json
import requests
from requests.adapters import HTTPAdapter, Retry

url = 'https://api.gametools.network/bf4/servers/?name=%20&platform=ps4&limit=250&region=all&is_password_protected=false&lang=en-us'
oldList = [] # List for keeping track of active servers

s = requests.Session()
retries = Retry(total=10, backoff_factor=0.5, status_forcelist=[400, 403, 404, 408, 422, 429, 500, 501, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
s.mount("http://", adapter)
s.mount("https://", adapter)

# Define interval in seconds for how often we'll poll the servers
n = 5.0

try:
  while True:
    search_servers(s, url, oldList)
    time.sleep(n)
except KeyboardInterrupt:
  print("Stopped.\n")

def search_servers(s, url, oldList):
  newList = []

  # Set all the servers in the oldList to false (aka not new)
  if len(oldList) > 0:
    for i in range(len(oldList)):
      oldList[i].newserver = False

  response = s.get(url)

  # Grab all the servers and their current maps/modes/etc
  if (response.ok):
    responseJson = response.json()
    servers = responseJson['servers']

    for x in servers:
      newList.append(server(x["serverId"], x["currentMap"], x["mode"], x["playerAmount"], x["serverLink"], True))
  else:
    print('API status code was %s, not 200', response.status_code)
    return oldList

  # Remove all the servers that don't meet the criteria (Altai games in this case)
  for i in reversed(range(len(newList))):
    if newList[i].serverMap != "Altai Range":
      newList.pop(i)
    elif newList[i].gameMode != "Conquest large":
      newList.pop(i)
    elif newList[i].playerCount <= 40:
      newList.pop(i)

  # Remove the servers that are no longer on Altai
  finishedServers = [item for item in oldList if item not in newList]
  for item in finishedServers:
    oldList.remove(item)

  # If server in both the new and old lists then its still an ongoing Altai game and we should not alert for that
  for item1 in oldList:
    for item2 in newList:
      if item1 == item2:
        newList.remove(item2)

  # Add new Altai servers to our list
  for item in newList:
    oldList.append(item)

  # Alert that we found a new Altai server
  if len(oldList) > 0:
    for i in range(len(oldList)):
      if oldList[i].newserver == True:
        print(f'A new Altai server just started: {oldList[i].serverLink}')

# Class object for each server
class server:
  def __init__(self, serverId, serverMap, gameMode, playerCount, serverLink, newServer):
    self.serverId = serverId
    self.serverMap = serverMap
    self.gameMode = gameMode
    self.playerCount = playerCount
    self.serverLink = serverLink
    self.newServer = newServer

  def __key(self):
    return self.serverId

  def __repr__(self):
    return "\nServer: % s \nMap: % s \nGamemode: % s \nPlayercount: % s" % (self.serverId, self.serverMap, self.gameMode, self.playerCount)
  
  def __hash__(self):
    return hash(self.__key)

  def __eq__(self, other):
    return self.serverId == other.serverId