import sqlite3
import json
import time
import numpy as np
import threading
from datetime import datetime

def threaded_process(data):
  try:
    conn = sqlite3.connect('C:/Program Files/DB Browser for SQLite/Battlefield Database.db', timeout=300)
    cursor = conn.cursor()

    with open("PC_marksmen_results.txt", "w") as f:
      # iterate through json
      for x in data:
        for key, value in x.items():
          if key == '_id':
            cursor.execute("""SELECT BF4_RIBBONS.MARKSMAN_RIBBON, BF4_WEAPONS_SNIPER_STATS.* FROM BF4_RIBBONS INNER JOIN BF4_WEAPONS_SNIPER_STATS ON BF4_RIBBONS.player_id = BF4_WEAPONS_SNIPER_STATS.player_id WHERE BF4_RIBBONS.player_id = ?""", (value,))
            player_info = cursor.fetchall()
            for row in player_info:
              marsmanRibbons = row[0]
              playerId = row[1]
              timePlayed = row[6] + row[11] + row[16] + row[21] + row[26] + row[31] + row[36] + row[41] + row[46] + row[51] + row[56] + row[61] + row[66] + row[71] + row[76]
              perHour = marsmanRibbons / (timePlayed/3600)
              f.write(f"{playerId} : {perHour:.3f}\n")

  except sqlite3.Error as error:
    print('Error occured - ', error)

  finally:
    if conn:
      conn.close()
      print('Connection closed')

# get json with all the playerIds
json_file = 'C:/Users/bige3/OneDrive/Documents/PC_marksmen.json'
with open(json_file) as json_data:
  data = json.load(json_data)

n_threads = 1
json_chunk = np.array_split(data, n_threads)

thread_list = []
for thr in range(n_threads):
  thread = threading.Thread(target=threaded_process, args=(json_chunk[thr],))
  thread_list.append(thread)
  thread_list[thr].start()

for thread in thread_list:
  thread.join()