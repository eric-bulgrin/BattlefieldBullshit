import sqlite3
import json
import requests
import time
import numpy as np
import threading
from datetime import datetime
from requests.adapters import HTTPAdapter, Retry

def get_stats(playerId, userId, s, conn, platform):
  start = time.time()
  cursor = conn.cursor()

  """ PLAYER DATA """
  currentName = ''

  findNameByUserId = 'http://battlelog.battlefield.com/bf4/user/overviewBoxStats/' + str(userId) + '/'

  startAPIrequest = time.time()
  nameResponse = s.get(findNameByUserId)
  endAPIrequest = time.time()
  elapsed_seconds = (endAPIrequest - startAPIrequest)
  print(f"Overview API request took {elapsed_seconds:.2f} seconds.")

  nameResponseJson = nameResponse.json()
  
  # occasionally these APIs fail but still get 200 success so need to reload the data
  while 'soldiersBox' not in nameResponseJson['data']:
    nameResponse = s.get(findNameByUserId)
    nameResponseJson = nameResponse.json()

  soldiers = nameResponseJson['data']['soldiersBox']
  
  for soldier in soldiers:
    if soldier['persona']['personaId'] == str(playerId):
      currentName = soldier['persona']['personaName']
      break

  if currentName == '':
    print(f"Returning early - couldn't find player name via userId\n\n")
    return

  # First we check to see if this player already exists and needs UPDATED vs INSERTED
  update = False
  
  cursor.execute("""SELECT player_id, current_name FROM BF4_PLAYERS WHERE player_id=?""", (playerId,))
  player_id_list = cursor.fetchall()

  for row in player_id_list:
    if playerId == row[0]:
      update = True
      oldName = row[1]
      break
  
  try:
    with conn:
      if update:
        cursor.execute("""UPDATE BF4_PLAYERS SET current_name=? WHERE player_id=?""", (currentName, playerId))
        conn.commit()
      else:
        cursor.execute("""INSERT INTO BF4_PLAYERS (player_id, current_name, leaderboard_eligible, platform, adjusted_playtime, last_updated) VALUES (?, ?, ?, ?, ?, ?);""", (playerId, currentName, 1, platform, 0, datetime.today().strftime('%Y-%m-%d')))
        conn.commit()
  except sqlite3.IntegrityError:
    # record already exists
    print("Record Already Exists")
  finally:
    # close cursor continue on with the code
    cursor.close()

  cursor = conn.cursor()

  """ ALIAS CHECK """

  # Next we check to see if their current userName is different
  insertAlias = False

  if update and (currentName != oldName):
    insertAlias = True

    # we must also check to see if they are using a previous alias as we don't want to add duplicates
    cursor.execute("""SELECT username FROM BF4_ALIASES WHERE player_id=?""", (playerId,))
    name_list = cursor.fetchall()
    for row in name_list:
      if oldName == row[0]:
        insertAlias = False
        break
  
  try:
    with conn:
      if insertAlias:
        cursor.execute("""INSERT INTO BF4_ALIASES (player_id, username, timestamp) VALUES (?, ?, ?);""", (playerId, oldName, datetime.today().strftime('%Y-%m-%d')))
        conn.commit()
  except sqlite3.IntegrityError:
    # record already exists
    print("Record Already Exists")
  finally:
    # close cursor continue on with the code
    cursor.close()

  cursor = conn.cursor()
   
  """ BASE STATS """

  warsawDetailedStats = 'http://battlelog.battlefield.com/bf4/warsawdetailedstatspopulate/' + str(playerId)
  if platform == 'pc':
    warsawDetailedStats = warsawDetailedStats + '/1/'
  elif platform == 'ps4':
    warsawDetailedStats = warsawDetailedStats + '/32/'
  elif platform == 'xb1':
    warsawDetailedStats = warsawDetailedStats + '/64/'

  startAPIrequest = time.time()
  detailedStatsResponse = s.get(warsawDetailedStats)
  endAPIrequest = time.time()
  elapsed_seconds = (endAPIrequest - startAPIrequest)
  print(f"Details API request took {elapsed_seconds:.2f} seconds.")

  detailedStatsJson = detailedStatsResponse.json()

  # occasionally these APIs fail but still get 200 success so need to reload the data
  while 'generalStats' not in detailedStatsJson['data']:
    detailedStatsResponse = s.get(warsawDetailedStats)
    detailedStatsJson = detailedStatsResponse.json()

  detailedStats = detailedStatsJson['data']['generalStats']

  score = int(detailedStats['score'])
  secondsPlayed = int(detailedStats['timePlayed'])
  kills = int(detailedStats['kills'])
  deaths = int(detailedStats['deaths'])
  killAssists = int(detailedStats['killAssists'])
  wins = int(detailedStats['numWins'])
  losses = int(detailedStats['numLosses'])
  headshots = int(detailedStats['headshots'])
  longestHeadshot = float(detailedStats['longestHeadshot'])
  shotsFired = int(detailedStats['shotsFired'])
  shotsHit = int(detailedStats['shotsHit'])
  highestKillStreak = int(detailedStats['killStreakBonus'])
  revives = int(detailedStats['revives'])
  repairs = int(detailedStats['repairs'])
  heals = int(detailedStats['heals'])
  resupplies = int(detailedStats['resupplies'])
  dogtagsTaken = int(detailedStats['dogtagsTaken'])

  if update:
    cursor.execute("""SELECT score FROM BF4_BASE_STATS WHERE player_id=? LIMIT 1""", (playerId,))
    oldScore = cursor.fetchone()[0]
    if score == oldScore:
      print(f"Returning early since player stats are already up to date!\n\n")
      return
    else :
      cursor.execute("""UPDATE BF4_PLAYERS SET last_updated=? WHERE player_id=?""", (datetime.today().strftime('%Y-%m-%d'), playerId))
      conn.commit()

  try:
    with conn:
      if update:
        cursor.execute("""UPDATE BF4_BASE_STATS SET score=?, seconds_played=?, kills=?, deaths=?, kill_assists=?, wins=?, losses=?, headshots=?, longest_headshot=?, shots_fired=?, shots_hit=?, highest_kill_streak=?, revives=?, repairs=?, heals=?, resupplies=?, dogtags_taken=? WHERE player_id=?""", (score, secondsPlayed, kills, deaths, killAssists, wins, losses, headshots, longestHeadshot, shotsFired, shotsHit, highestKillStreak, revives, repairs, heals, resupplies, dogtagsTaken, playerId))
        conn.commit()
      else:
        cursor.execute("""INSERT INTO BF4_BASE_STATS (player_id, score, seconds_played, kills, deaths, kill_assists, wins, losses, headshots, longest_headshot, shots_fired, shots_hit, highest_kill_streak, revives, repairs, heals, resupplies, dogtags_taken) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""", (playerId, score, secondsPlayed, kills, deaths, killAssists, wins, losses, headshots, longestHeadshot, shotsFired, shotsHit, highestKillStreak, revives, repairs, heals, resupplies, dogtagsTaken))
        conn.commit()
  except sqlite3.IntegrityError:
    # record already exists
    print("Record Already Exists")
  finally:
    # close cursor continue on with the code
    cursor.close()

  cursor = conn.cursor()

  """ CLASS STATS """

  assaultScore = int(detailedStats['assault'])
  engineerScore = int(detailedStats['engineer'])
  supportScore = int(detailedStats['support'])
  reconScore = int(detailedStats['recon'])
  commanderScore = int(detailedStats['commander'])
  squadScore = int(detailedStats['sc_squad'])
  vehicleScore = int(detailedStats['sc_vehicle'])

  try:
    with conn:
      if update:
        cursor.execute("""UPDATE BF4_CLASS_STATS SET assault_score=?, engineer_score=?, support_score=?, recon_score=?, commander_score=?, squad_score=?, vehicle_score=? WHERE player_id=?""", (assaultScore, engineerScore, supportScore, reconScore, commanderScore, squadScore, vehicleScore, playerId))
        conn.commit()
      else:
        cursor.execute("""INSERT INTO BF4_CLASS_STATS (player_id, recon_score, assault_score, engineer_score, support_score, commander_score, squad_score, vehicle_score) VALUES (?, ?, ?, ?, ?, ?, ?, ?);""", (playerId, reconScore, assaultScore, engineerScore, supportScore, commanderScore, squadScore, vehicleScore))
        conn.commit()
  except sqlite3.IntegrityError:
    # record already exists
    print("Record Already Exists")
  finally:
    # close cursor continue on with the code
    cursor.close()

  cursor = conn.cursor()

  """ WEAPON STATS """

  warsawWeaponStats = 'http://battlelog.battlefield.com/bf4/warsawWeaponsPopulateStats/' + str(playerId)
  if platform == 'pc':
    warsawWeaponStats = warsawWeaponStats + '/1/stats/'
  elif platform == 'ps4':
    warsawWeaponStats = warsawWeaponStats + '/32/stats/'
  elif platform == 'xb1':
    warsawWeaponStats = warsawWeaponStats + '/64/stats/'

  startAPIrequest = time.time()
  weaponStatsResponse = s.get(warsawWeaponStats)
  endAPIrequest = time.time()
  elapsed_seconds = (endAPIrequest - startAPIrequest)
  print(f"Weapons API request took {elapsed_seconds:.2f} seconds.")

  weaponStatsJson = weaponStatsResponse.json()

  # occasionally these APIs fail but still get 200 success so need to reload the data
  while 'mainWeaponStats' not in weaponStatsJson['data'] or not weaponStatsJson['data']['mainWeaponStats']:
    weaponStatsResponse = s.get(warsawWeaponStats)
    weaponStatsJson = weaponStatsResponse.json()

  weaponStats = weaponStatsJson['data']['mainWeaponStats']

  # m82 variables
  m82kills = 0
  m82headshots = 0
  m82shots = 0
  m82hits = 0
  m82time = 0

  # amr2 variables
  amr2kills = 0
  amr2headshots = 0
  amr2shots = 0
  amr2hits = 0
  amr2time = 0

  # usas12 variables
  usas12kills = 0
  usas12headshots = 0
  usas12shots = 0
  usas12hits = 0
  usas12time = 0

  # phantom variables
  phantomkills = 0
  phantomheadshots = 0
  phantomshots = 0
  phantomhits = 0
  phantomtime = 0

  for x in weaponStats:
    match x['slug']:
      # SNIPER RIFLES
      case 'm40a5':
        m40kills = x['kills']
        m40headshots = x['headshots']
        m40shots = x['shotsFired']
        m40hits = x['shotsHit']
        m40time = x['timeEquipped']
      case 'scout-elite':
        scoutkills = x['kills']
        scoutheadshots = x['headshots']
        scoutshots = x['shotsFired']
        scouthits = x['shotsHit']
        scouttime = x['timeEquipped']
      case 'sv-98':
        sv98kills = x['kills']
        sv98headshots = x['headshots']
        sv98shots = x['shotsFired']
        sv98hits = x['shotsHit']
        sv98time = x['timeEquipped']
      case 'jng-90':
        jng90kills = x['kills']
        jng90headshots = x['headshots']
        jng90shots = x['shotsFired']
        jng90hits = x['shotsHit']
        jng90time = x['timeEquipped']
      case '338-recon':
        recon338kills = x['kills']
        recon338headshots = x['headshots']
        recon338shots = x['shotsFired']
        recon338hits = x['shotsHit']
        recon338time = x['timeEquipped']
      case 'm98b':
        m98bkills = x['kills']
        m98bheadshots = x['headshots']
        m98bshots = x['shotsFired']
        m98bhits = x['shotsHit']
        m98btime = x['timeEquipped']
      case 'srr-61':
        srr61kills = x['kills']
        srr61headshots = x['headshots']
        srr61shots = x['shotsFired']
        srr61hits = x['shotsHit']
        srr61time = x['timeEquipped']
      case 'cs-lr4':
        cslr4kills = x['kills']
        cslr4headshots = x['headshots']
        cslr4shots = x['shotsFired']
        cslr4hits = x['shotsHit']
        cslr4time = x['timeEquipped']
      case 'l115':
        l115kills = x['kills']
        l115headshots = x['headshots']
        l115shots = x['shotsFired']
        l115hits = x['shotsHit']
        l115time = x['timeEquipped']
      case 'gol-magnum':
        golkills = x['kills']
        golheadshots = x['headshots']
        golshots = x['shotsFired']
        golhits = x['shotsHit']
        goltime = x['timeEquipped']
      case 'fy-js':
        fyjskills = x['kills']
        fyjsheadshots = x['headshots']
        fyjsshots = x['shotsFired']
        fyjshits = x['shotsHit']
        fyjstime = x['timeEquipped']
      case 'sr338':
        sr338kills = x['kills']
        sr338headshots = x['headshots']
        sr338shots = x['shotsFired']
        sr338hits = x['shotsHit']
        sr338time = x['timeEquipped']
      case 'cs5':
        cs5kills = x['kills']
        cs5headshots = x['headshots']
        cs5shots = x['shotsFired']
        cs5hits = x['shotsHit']
        cs5time = x['timeEquipped']
      case 'm82a3-mid':
        m82kills = m82kills + x['kills']
        m82headshots = m82headshots + x['headshots']
        m82shots = m82shots + x['shotsFired']
        m82hits = m82hits + x['shotsHit']
        m82time = m82time + x['timeEquipped']
      case 'm82a3-cqb':
        m82kills = m82kills + x['kills']
        m82headshots = m82headshots + x['headshots']
        m82shots = m82shots + x['shotsFired']
        m82hits = m82hits + x['shotsHit']
        m82time = m82time + x['timeEquipped']
      case 'm82a3':
        m82kills = m82kills + x['kills']
        m82headshots = m82headshots + x['headshots']
        m82shots = m82shots + x['shotsFired']
        m82hits = m82hits + x['shotsHit']
        m82time = m82time + x['timeEquipped']
      case 'amr-2-mid':
        amr2kills = amr2kills + x['kills']
        amr2headshots = amr2headshots + x['headshots']
        amr2shots = amr2shots + x['shotsFired']
        amr2hits = amr2hits + x['shotsHit']
        amr2time = amr2time + x['timeEquipped']
      case 'amr-2-cqb':
        amr2kills = amr2kills + x['kills']
        amr2headshots = amr2headshots + x['headshots']
        amr2shots = amr2shots + x['shotsFired']
        amr2hits = amr2hits + x['shotsHit']
        amr2time = amr2time + x['timeEquipped']
      case 'amr-2':
        amr2kills = amr2kills + x['kills']
        amr2headshots = amr2headshots + x['headshots']
        amr2shots = amr2shots + x['shotsFired']
        amr2hits = amr2hits + x['shotsHit']
        amr2time = amr2time + x['timeEquipped']
      # ASSAULT RIFLES
      case 'scar-h':
        scarhkills = x['kills']
        scarhheadshots = x['headshots']
        scarhshots = x['shotsFired']
        scarhhits = x['shotsHit']
        scarhtime = x['timeEquipped']
      case 'm416':
        m416kills = x['kills']
        m416headshots = x['headshots']
        m416shots = x['shotsFired']
        m416hits = x['shotsHit']
        m416time = x['timeEquipped']
      case 'sar-21':
        sar21kills = x['kills']
        sar21headshots = x['headshots']
        sar21shots = x['shotsFired']
        sar21hits = x['shotsHit']
        sar21time = x['timeEquipped']
      case 'aek-971':
        aek971kills = x['kills']
        aek971headshots = x['headshots']
        aek971shots = x['shotsFired']
        aek971hits = x['shotsHit']
        aek971time = x['timeEquipped']
      case 'famas':
        famaskills = x['kills']
        famasheadshots = x['headshots']
        famasshots = x['shotsFired']
        famashits = x['shotsHit']
        famastime = x['timeEquipped']
      case 'aug-a3':
        auga3kills = x['kills']
        auga3headshots = x['headshots']
        auga3shots = x['shotsFired']
        auga3hits = x['shotsHit']
        auga3time = x['timeEquipped']
      case 'm16a4':
        m16a4kills = x['kills']
        m16a4headshots = x['headshots']
        m16a4shots = x['shotsFired']
        m16a4hits = x['shotsHit']
        m16a4time = x['timeEquipped']
      case 'cz-805':
        cz805kills = x['kills']
        cz805headshots = x['headshots']
        cz805shots = x['shotsFired']
        cz805hits = x['shotsHit']
        cz805time = x['timeEquipped']
      case 'ak-12':
        ak12kills = x['kills']
        ak12headshots = x['headshots']
        ak12shots = x['shotsFired']
        ak12hits = x['shotsHit']
        ak12time = x['timeEquipped']
      case 'an-94':
        an94kills = x['kills']
        an94headshots = x['headshots']
        an94shots = x['shotsFired']
        an94hits = x['shotsHit']
        an94time = x['timeEquipped']
      case 'f2000':
        f2000kills = x['kills']
        f2000headshots = x['headshots']
        f2000shots = x['shotsFired']
        f2000hits = x['shotsHit']
        f2000time = x['timeEquipped']
      case 'ace-23':
        ace23kills = x['kills']
        ace23headshots = x['headshots']
        ace23shots = x['shotsFired']
        ace23hits = x['shotsHit']
        ace23time = x['timeEquipped']
      case 'qbz-95-1':
        qbz95kills = x['kills']
        qbz95headshots = x['headshots']
        qbz95shots = x['shotsFired']
        qbz95hits = x['shotsHit']
        qbz95time = x['timeEquipped']
      case 'bulldog':
        bulldogkills = x['kills']
        bulldogheadshots = x['headshots']
        bulldogshots = x['shotsFired']
        bulldoghits = x['shotsHit']
        bulldogtime = x['timeEquipped']
      case 'ar160':
        ar160kills = x['kills']
        ar160headshots = x['headshots']
        ar160shots = x['shotsFired']
        ar160hits = x['shotsHit']
        ar160time = x['timeEquipped']
      case 'l85a2':
        l85a2kills = x['kills']
        l85a2headshots = x['headshots']
        l85a2shots = x['shotsFired']
        l85a2hits = x['shotsHit']
        l85a2time = x['timeEquipped']
      # CARBINES
      case 'acw-r':
        acwrkills = x['kills']
        acwrheadshots = x['headshots']
        acwrshots = x['shotsFired']
        acwrhits = x['shotsHit']
        acwrtime = x['timeEquipped']
      case 'sg553':
        sg553kills = x['kills']
        sg553headshots = x['headshots']
        sg553shots = x['shotsFired']
        sg553hits = x['shotsHit']
        sg553time = x['timeEquipped']
      case 'aku-12':
        aku12kills = x['kills']
        aku12headshots = x['headshots']
        aku12shots = x['shotsFired']
        aku12hits = x['shotsHit']
        aku12time = x['timeEquipped']
      case 'a-91':
        a91kills = x['kills']
        a91headshots = x['headshots']
        a91shots = x['shotsFired']
        a91hits = x['shotsHit']
        a91time = x['timeEquipped']
      case 'ace-52-cqb':
        ace52kills = x['kills']
        ace52headshots = x['headshots']
        ace52shots = x['shotsFired']
        ace52hits = x['shotsHit']
        ace52time = x['timeEquipped']
      case 'g36c':
        g36ckills = x['kills']
        g36cheadshots = x['headshots']
        g36cshots = x['shotsFired']
        g36chits = x['shotsHit']
        g36ctime = x['timeEquipped']
      case 'm4':
        m4kills = x['kills']
        m4headshots = x['headshots']
        m4shots = x['shotsFired']
        m4hits = x['shotsHit']
        m4time = x['timeEquipped']
      case 'ace-21-cqb':
        ace21kills = x['kills']
        ace21headshots = x['headshots']
        ace21shots = x['shotsFired']
        ace21hits = x['shotsHit']
        ace21time = x['timeEquipped']
      case 'type-95b-1':
        type95bkills = x['kills']
        type95bheadshots = x['headshots']
        type95bshots = x['shotsFired']
        type95bhits = x['shotsHit']
        type95btime = x['timeEquipped']
      case 'groza-1':
        groza1kills = x['kills']
        groza1headshots = x['headshots']
        groza1shots = x['shotsFired']
        groza1hits = x['shotsHit']
        groza1time = x['timeEquipped']
      case 'ak-5c':
        ak5ckills = x['kills']
        ak5cheadshots = x['headshots']
        ak5cshots = x['shotsFired']
        ak5chits = x['shotsHit']
        ak5ctime = x['timeEquipped']
      case 'mtar-21':
        mtar21kills = x['kills']
        mtar21headshots = x['headshots']
        mtar21shots = x['shotsFired']
        mtar21hits = x['shotsHit']
        mtar21time = x['timeEquipped']
      case 'phantom':
        phantomkills = x['kills']
        phantomheadshots = x['headshots']
        phantomshots = x['shotsFired']
        phantomhits = x['shotsHit']
        phantomtime = x['timeEquipped']
      # DMRS
      case 'mk11-mod-0':
        mk11kills = x['kills']
        mk11headshots = x['headshots']
        mk11shots = x['shotsFired']
        mk11hits = x['shotsHit']
        mk11time = x['timeEquipped']
      case 'sks':
        skskills = x['kills']
        sksheadshots = x['headshots']
        sksshots = x['shotsFired']
        skshits = x['shotsHit']
        skstime = x['timeEquipped']
      case 'svd-12':
        svd12kills = x['kills']
        svd12headshots = x['headshots']
        svd12shots = x['shotsFired']
        svd12hits = x['shotsHit']
        svd12time = x['timeEquipped']
      case 'qbu-88':
        qbu88kills = x['kills']
        qbu88headshots = x['headshots']
        qbu88shots = x['shotsFired']
        qbu88hits = x['shotsHit']
        qbu88time = x['timeEquipped']
      case 'm39-emr':
        m39kills = x['kills']
        m39headshots = x['headshots']
        m39shots = x['shotsFired']
        m39hits = x['shotsHit']
        m39time = x['timeEquipped']
      case 'ace-53-sv':
        ace53kills = x['kills']
        ace53headshots = x['headshots']
        ace53shots = x['shotsFired']
        ace53hits = x['shotsHit']
        ace53time = x['timeEquipped']
      case 'scar-h-sv':
        scarhsvkills = x['kills']
        scarhsvheadshots = x['headshots']
        scarhsvshots = x['shotsFired']
        scarhsvhits = x['shotsHit']
        scarhsvtime = x['timeEquipped']
      case 'rfb':
        rfbkills = x['kills']
        rfbheadshots = x['headshots']
        rfbshots = x['shotsFired']
        rfbhits = x['shotsHit']
        rfbtime = x['timeEquipped']
      # GADGETS
      case 'xm25-airburst':
        xm25airkills = x['kills']
        xm25airheadshots = x['headshots']
        xm25airshots = x['shotsFired']
        xm25airhits = x['shotsHit']
        xm25airtime = x['timeEquipped']
      case 'xm25-dart':
        xm25dartkills = x['kills']
        xm25dartheadshots = x['headshots']
        xm25dartshots = x['shotsFired']
        xm25darthits = x['shotsHit']
        xm25darttime = x['timeEquipped']
      case 'xm25-smoke':
        xm25smokekills = x['kills']
        xm25smokeheadshots = x['headshots']
        xm25smokeshots = x['shotsFired']
        xm25smokehits = x['shotsHit']
        xm25smoketime = x['timeEquipped']
      case 'c4-explosive':
        c4kills = x['kills']
        c4headshots = x['headshots']
        c4shots = x['shotsFired']
        c4hits = x['shotsHit']
        c4time = x['timeEquipped']
      case 'm15-at-mine':
        m15minekills = x['kills']
        m15mineheadshots = x['headshots']
        m15mineshots = x['shotsFired']
        m15minehits = x['shotsHit']
        m15minetime = x['timeEquipped']
      case 'm2-slam':
        m2slamkills = x['kills']
        m2slamheadshots = x['headshots']
        m2slamshots = x['shotsFired']
        m2slamhits = x['shotsHit']
        m2slamtime = x['timeEquipped']
      case 'm18-claymore':
        claymorekills = x['kills']
        claymoreheadshots = x['headshots']
        claymoreshots = x['shotsFired']
        claymorehits = x['shotsHit']
        claymoretime = x['timeEquipped']
      case 'repair-tool':
        repairtoolkills = x['kills']
        repairtoolheadshots = x['headshots']
        repairtoolshots = x['shotsFired']
        repairtoolhits = x['shotsHit']
        repairtooltime = x['timeEquipped']
      case 'defibrillator':
        defibkills = x['kills']
        defibheadshots = x['headshots']
        defibshots = x['shotsFired']
        defibhits = x['shotsHit']
        defibtime = x['timeEquipped']
      case 'mbt-law':
        lawkills = x['kills']
        lawheadshots = x['headshots']
        lawshots = x['shotsFired']
        lawhits = x['shotsHit']
        lawtime = x['timeEquipped']
      case 'fim-92-stinger':
        stingerkills = x['kills']
        stingerheadshots = x['headshots']
        stingershots = x['shotsFired']
        stingerhits = x['shotsHit']
        stingertime = x['timeEquipped']
      case 'rpg-7v2':
        rpgkills = x['kills']
        rpgheadshots = x['headshots']
        rpgshots = x['shotsFired']
        rpghits = x['shotsHit']
        rpgtime = x['timeEquipped']
      case 'sa-18-igla':
        iglakills = x['kills']
        iglaheadshots = x['headshots']
        iglashots = x['shotsFired']
        iglahits = x['shotsHit']
        iglatime = x['timeEquipped']
      case 'mk153-smaw':
        smawkills = x['kills']
        smawheadshots = x['headshots']
        smawshots = x['shotsFired']
        smawhits = x['shotsHit']
        smawtime = x['timeEquipped']
      case 'fgm-148-javelin':
        javelinkills = x['kills']
        javelinheadshots = x['headshots']
        javelinshots = x['shotsFired']
        javelinhits = x['shotsHit']
        javelintime = x['timeEquipped']
      case 'fgm-172-sraw':
        srawkills = x['kills']
        srawheadshots = x['headshots']
        srawshots = x['shotsFired']
        srawhits = x['shotsHit']
        srawtime = x['timeEquipped']
      case 'hvm-ii':
        hvmkills = x['kills']
        hvmheadshots = x['headshots']
        hvmshots = x['shotsFired']
        hvmhits = x['shotsHit']
        hvmtime = x['timeEquipped']
      case 'm136-cs':
        m136kills = x['kills']
        m136headshots = x['headshots']
        m136shots = x['shotsFired']
        m136hits = x['shotsHit']
        m136time = x['timeEquipped']
      case 'm320-he':
        m320hekills = x['kills']
        m320heheadshots = x['headshots']
        m320heshots = x['shotsFired']
        m320hehits = x['shotsHit']
        m320hetime = x['timeEquipped']
      case 'm320-lvg':
        m320lvgkills = x['kills']
        m320lvgheadshots = x['headshots']
        m320lvgshots = x['shotsFired']
        m320lvghits = x['shotsHit']
        m320lvgtime = x['timeEquipped']
      case 'm320-smk':
        m320smkkills = x['kills']
        m320smkheadshots = x['headshots']
        m320smkshots = x['shotsFired']
        m320smkhits = x['shotsHit']
        m320smktime = x['timeEquipped']
      case 'm320-dart':
        m320dartkills = x['kills']
        m320dartheadshots = x['headshots']
        m320dartshots = x['shotsFired']
        m320darthits = x['shotsHit']
        m320darttime = x['timeEquipped']
      case 'm320-fb':
        m320fbkills = x['kills']
        m320fbheadshots = x['headshots']
        m320fbshots = x['shotsFired']
        m320fbhits = x['shotsHit']
        m320fbtime = x['timeEquipped']
      case 'm320-3gl':
        m3203glkills = x['kills']
        m3203glheadshots = x['headshots']
        m3203glshots = x['shotsFired']
        m3203glhits = x['shotsHit']
        m3203gltime = x['timeEquipped']
      case 'ballistic-shield':
        shieldkills = x['kills']
        shieldheadshots = x['headshots']
        shieldshots = x['shotsFired']
        shieldhits = x['shotsHit']
        shieldtime = x['timeEquipped']
      case 'rorsch-mk-1':
        railgunkills = x['kills']
        railgunheadshots = x['headshots']
        railgunshots = x['shotsFired']
        railgunhits = x['shotsHit']
        railguntime = x['timeEquipped']
      case 'm32-mgl':
        m32kills = x['kills']
        m32headshots = x['headshots']
        m32shots = x['shotsFired']
        m32hits = x['shotsHit']
        m32time = x['timeEquipped']
      # GRENADES
      case 'v40-mini':
        v40minikills = x['kills']
        v40miniheadshots = x['headshots']
        v40minishots = x['shotsFired']
        v40minihits = x['shotsHit']
        v40minitime = x['timeEquipped']
      case 'rgo-impact':
        rgokills = x['kills']
        rgoheadshots = x['headshots']
        rgoshots = x['shotsFired']
        rgohits = x['shotsHit']
        rgotime = x['timeEquipped']
      case 'm34-incendiary':
        m34kills = x['kills']
        m34headshots = x['headshots']
        m34shots = x['shotsFired']
        m34hits = x['shotsHit']
        m34time = x['timeEquipped']
      case 'm18-smoke':
        m18smokekills = x['kills']
        m18smokeheadshots = x['headshots']
        m18smokeshots = x['shotsFired']
        m18smokehits = x['shotsHit']
        m18smoketime = x['timeEquipped']
      case 'm84-flashbang':
        m84kills = x['kills']
        m84headshots = x['headshots']
        m84shots = x['shotsFired']
        m84hits = x['shotsHit']
        m84time = x['timeEquipped']
      case 'hand-flare':
        flarekills = x['kills']
        flareheadshots = x['headshots']
        flareshots = x['shotsFired']
        flarehits = x['shotsHit']
        flaretime = x['timeEquipped']
      case 'm67-frag':
        m67kills = x['kills']
        m67headshots = x['headshots']
        m67shots = x['shotsFired']
        m67hits = x['shotsHit']
        m67time = x['timeEquipped']
      # KNIVES
      case 'bj-2':
        bj2 = x['kills']
      case 'weaver':
        weaver = x['kills']
      case 'bayonet':
        bayonet = x['kills']
      case 'scout':
        scout = x['kills']
      case 'acb-90':
        acb90 = x['kills']
      case 'seal':
        seal = x['kills']
      case 'trench':
        trench = x['kills']
      case 'bowie':
        bowie = x['kills']
      case 'precision':
        precision = x['kills']
      case 'survival':
        survival = x['kills']
      case 'carbon-fiber':
        carbonFiber = x['kills']
      case 'improvised':
        improvised = x['kills']
      case 'tanto':
        tanto = x['kills']
      case 'neck':
        neck = x['kills']
      case 'tactical':
        tactical = x['kills']
      case 'boot':
        boot = x['kills']
      case 'dive':
        dive = x['kills']
      case 'shank':
        shank = x['kills']
      case 'machete':
        machete = x['kills']
      case 'c100':
        c100 = x['kills']
      # LMGS
      case 'type-88-lmg':
        type88kills = x['kills']
        type88headshots = x['headshots']
        type88shots = x['shotsFired']
        type88hits = x['shotsHit']
        type88time = x['timeEquipped']
      case 'lsat':
        lsatkills = x['kills']
        lsatheadshots = x['headshots']
        lsatshots = x['shotsFired']
        lsathits = x['shotsHit']
        lsattime = x['timeEquipped']
      case 'pkp-pecheneg':
        pkpkills = x['kills']
        pkpheadshots = x['headshots']
        pkpshots = x['shotsFired']
        pkphits = x['shotsHit']
        pkptime = x['timeEquipped']
      case 'qbb-95-1':
        qbb95kills = x['kills']
        qbb95headshots = x['headshots']
        qbb95shots = x['shotsFired']
        qbb95hits = x['shotsHit']
        qbb95time = x['timeEquipped']
      case 'm240b':
        m240bkills = x['kills']
        m240bheadshots = x['headshots']
        m240bshots = x['shotsFired']
        m240bhits = x['shotsHit']
        m240btime = x['timeEquipped']
      case 'mg4':
        mg4kills = x['kills']
        mg4headshots = x['headshots']
        mg4shots = x['shotsFired']
        mg4hits = x['shotsHit']
        mg4time = x['timeEquipped']
      case 'u-100-mk5':
        u100kills = x['kills']
        u100headshots = x['headshots']
        u100shots = x['shotsFired']
        u100hits = x['shotsHit']
        u100time = x['timeEquipped']
      case 'l86a2':
        l86a2kills = x['kills']
        l86a2headshots = x['headshots']
        l86a2shots = x['shotsFired']
        l86a2hits = x['shotsHit']
        l86a2time = x['timeEquipped']
      case 'aws':
        awskills = x['kills']
        awsheadshots = x['headshots']
        awsshots = x['shotsFired']
        awshits = x['shotsHit']
        awstime = x['timeEquipped']
      case 'm60-e4':
        m60kills = x['kills']
        m60headshots = x['headshots']
        m60shots = x['shotsFired']
        m60hits = x['shotsHit']
        m60time = x['timeEquipped']
      case 'rpk':
        rpkkills = x['kills']
        rpkheadshots = x['headshots']
        rpkshots = x['shotsFired']
        rpkhits = x['shotsHit']
        rpktime = x['timeEquipped']
      case 'm249':
        m249kills = x['kills']
        m249headshots = x['headshots']
        m249shots = x['shotsFired']
        m249hits = x['shotsHit']
        m249time = x['timeEquipped']
      case 'rpk-12':
        rpk12kills = x['kills']
        rpk12headshots = x['headshots']
        rpk12shots = x['shotsFired']
        rpk12hits = x['shotsHit']
        rpk12time = x['timeEquipped']
      case 'id-p-xp6-iname-m60ult':
        m60ultkills = x['kills']
        m60ultheadshots = x['headshots']
        m60ultshots = x['shotsFired']
        m60ulthits = x['shotsHit']
        m60ulttime = x['timeEquipped']
      # PDWS
      case 'pp-2000':
        pp2000kills = x['kills']
        pp2000headshots = x['headshots']
        pp2000shots = x['shotsFired']
        pp2000hits = x['shotsHit']
        pp2000time = x['timeEquipped']
      case 'ump-45':
        ump45kills = x['kills']
        ump45headshots = x['headshots']
        ump45shots = x['shotsFired']
        ump45hits = x['shotsHit']
        ump45time = x['timeEquipped']
      case 'cbj-ms':
        cbjmskills = x['kills']
        cbjmsheadshots = x['headshots']
        cbjmsshots = x['shotsFired']
        cbjmshits = x['shotsHit']
        cbjmstime = x['timeEquipped']
      case 'pdw-r':
        pdwrkills = x['kills']
        pdwrheadshots = x['headshots']
        pdwrshots = x['shotsFired']
        pdwrhits = x['shotsHit']
        pdwrtime = x['timeEquipped']
      case 'cz-3a1':
        cz3a1kills = x['kills']
        cz3a1headshots = x['headshots']
        cz3a1shots = x['shotsFired']
        cz3a1hits = x['shotsHit']
        cz3a1time = x['timeEquipped']
      case 'js2':
        js2kills = x['kills']
        js2headshots = x['headshots']
        js2shots = x['shotsFired']
        js2hits = x['shotsHit']
        js2time = x['timeEquipped']
      case 'groza-4':
        groza4kills = x['kills']
        groza4headshots = x['headshots']
        groza4shots = x['shotsFired']
        groza4hits = x['shotsHit']
        groza4time = x['timeEquipped']
      case 'mx4':
        mx4kills = x['kills']
        mx4headshots = x['headshots']
        mx4shots = x['shotsFired']
        mx4hits = x['shotsHit']
        mx4time = x['timeEquipped']
      case 'as-val':
        asvalkills = x['kills']
        asvalheadshots = x['headshots']
        asvalshots = x['shotsFired']
        asvalhits = x['shotsHit']
        asvaltime = x['timeEquipped']
      case 'p90':
        p90kills = x['kills']
        p90headshots = x['headshots']
        p90shots = x['shotsFired']
        p90hits = x['shotsHit']
        p90time = x['timeEquipped']
      case 'mpx':
        mpxkills = x['kills']
        mpxheadshots = x['headshots']
        mpxshots = x['shotsFired']
        mpxhits = x['shotsHit']
        mpxtime = x['timeEquipped']
      case 'ump-9':
        ump9kills = x['kills']
        ump9headshots = x['headshots']
        ump9shots = x['shotsFired']
        ump9hits = x['shotsHit']
        ump9time = x['timeEquipped']
      case 'mp7':
        mp7kills = x['kills']
        mp7headshots = x['headshots']
        mp7shots = x['shotsFired']
        mp7hits = x['shotsHit']
        mp7time = x['timeEquipped']
      case 'sr-2':
        sr2kills = x['kills']
        sr2headshots = x['headshots']
        sr2shots = x['shotsFired']
        sr2hits = x['shotsHit']
        sr2time = x['timeEquipped']
      # SHOTGUNS
      case '870-mcs':
        mcs870kills = x['kills']
        mcs870headshots = x['headshots']
        mcs870shots = x['shotsFired']
        mcs870hits = x['shotsHit']
        mcs870time = x['timeEquipped']
      case 'm1014':
        m1014kills = x['kills']
        m1014headshots = x['headshots']
        m1014shots = x['shotsFired']
        m1014hits = x['shotsHit']
        m1014time = x['timeEquipped']
      case 'hawk-12g':
        hawk12gkills = x['kills']
        hawk12gheadshots = x['headshots']
        hawk12gshots = x['shotsFired']
        hawk12ghits = x['shotsHit']
        hawk12gtime = x['timeEquipped']
      case 'saiga-12k':
        saigakills = x['kills']
        saigaheadshots = x['headshots']
        saigashots = x['shotsFired']
        saigahits = x['shotsHit']
        saigatime = x['timeEquipped']
      case 'spas-12':
        spas12kills = x['kills']
        spas12headshots = x['headshots']
        spas12shots = x['shotsFired']
        spas12hits = x['shotsHit']
        spas12time = x['timeEquipped']
      case 'uts-15':
        uts15kills = x['kills']
        uts15headshots = x['headshots']
        uts15shots = x['shotsFired']
        uts15hits = x['shotsHit']
        uts15time = x['timeEquipped']
      case 'dbv-12':
        dbv12kills = x['kills']
        dbv12headshots = x['headshots']
        dbv12shots = x['shotsFired']
        dbv12hits = x['shotsHit']
        dbv12time = x['timeEquipped']
      case 'qbs-09':
        qbs09kills = x['kills']
        qbs09headshots = x['headshots']
        qbs09shots = x['shotsFired']
        qbs09hits = x['shotsHit']
        qbs09time = x['timeEquipped']
      case 'dao-12':
        dao12kills = x['kills']
        dao12headshots = x['headshots']
        dao12shots = x['shotsFired']
        dao12hits = x['shotsHit']
        dao12time = x['timeEquipped']
      case 'usas-12':
        usas12kills = usas12kills + x['kills']
        usas12headshots = usas12headshots + x['headshots']
        usas12shots = usas12shots + x['shotsFired']
        usas12hits = usas12hits + x['shotsHit']
        usas12time = usas12time + x['timeEquipped']
      case 'usas-12-flir':
        usas12kills = usas12kills + x['kills']
        usas12headshots = usas12headshots + x['headshots']
        usas12shots = usas12shots + x['shotsFired']
        usas12hits = usas12hits + x['shotsHit']
        usas12time = usas12time + x['timeEquipped']
      case 'm26-mass':
        m26masskills = x['kills']
        m26massheadshots = x['headshots']
        m26massshots = x['shotsFired']
        m26masshits = x['shotsHit']
        m26masstime = x['timeEquipped']
      case 'm26-dart':
        m26dartkills = x['kills']
        m26dartheadshots = x['headshots']
        m26dartshots = x['shotsFired']
        m26darthits = x['shotsHit']
        m26darttime = x['timeEquipped']
      case 'm26-slug':
        m26slugkills = x['kills']
        m26slugheadshots = x['headshots']
        m26slugshots = x['shotsFired']
        m26slughits = x['shotsHit']
        m26slugtime = x['timeEquipped']
      case 'm26-frag':
        m26fragkills = x['kills']
        m26fragheadshots = x['headshots']
        m26fragshots = x['shotsFired']
        m26fraghits = x['shotsHit']
        m26fragtime = x['timeEquipped']
      # SIDEARMS
      case 'm9':
        m9kills = x['kills']
        m9headshots = x['headshots']
        m9shots = x['shotsFired']
        m9hits = x['shotsHit']
        m9time = x['timeEquipped']
      case 'qsz-92':
        qsz92kills = x['kills']
        qsz92headshots = x['headshots']
        qsz92shots = x['shotsFired']
        qsz92hits = x['shotsHit']
        qsz92time = x['timeEquipped']
      case 'mp443':
        mp443kills = x['kills']
        mp443headshots = x['headshots']
        mp443shots = x['shotsFired']
        mp443hits = x['shotsHit']
        mp443time = x['timeEquipped']
      case 'shorty-12g':
        shortykills = x['kills']
        shortyheadshots = x['headshots']
        shortyshots = x['shotsFired']
        shortyhits = x['shotsHit']
        shortytime = x['timeEquipped']
      case 'g18':
        g18kills = x['kills']
        g18headshots = x['headshots']
        g18shots = x['shotsFired']
        g18hits = x['shotsHit']
        g18time = x['timeEquipped']
      case 'fn57':
        fn57kills = x['kills']
        fn57headshots = x['headshots']
        fn57shots = x['shotsFired']
        fn57hits = x['shotsHit']
        fn57time = x['timeEquipped']
      case 'm1911':
        m1911kills = x['kills']
        m1911headshots = x['headshots']
        m1911shots = x['shotsFired']
        m1911hits = x['shotsHit']
        m1911time = x['timeEquipped']
      case '93r':
        r93kills = x['kills']
        r93headshots = x['headshots']
        r93shots = x['shotsFired']
        r93hits = x['shotsHit']
        r93time = x['timeEquipped']
      case 'cz-75':
        cz75kills = x['kills']
        cz75headshots = x['headshots']
        cz75shots = x['shotsFired']
        cz75hits = x['shotsHit']
        cz75time = x['timeEquipped']
      case '44-magnum':
        magnumkills = x['kills']
        magnumheadshots = x['headshots']
        magnumshots = x['shotsFired']
        magnumhits = x['shotsHit']
        magnumtime = x['timeEquipped']
      case 'compact-45':
        compactkills = x['kills']
        compactheadshots = x['headshots']
        compactshots = x['shotsFired']
        compacthits = x['shotsHit']
        compacttime = x['timeEquipped']
      case 'p226':
        p226kills = x['kills']
        p226headshots = x['headshots']
        p226shots = x['shotsFired']
        p226hits = x['shotsHit']
        p226time = x['timeEquipped']
      case 'mare-s-leg':
        mareskills = x['kills']
        maresheadshots = x['headshots']
        maresshots = x['shotsFired']
        mareshits = x['shotsHit']
        marestime = x['timeEquipped']
      case 'm412-rex':
        m412kills = x['kills']
        m412headshots = x['headshots']
        m412shots = x['shotsFired']
        m412hits = x['shotsHit']
        m412time = x['timeEquipped']
      case 'deagle-44':
        deaglekills = x['kills']
        deagleheadshots = x['headshots']
        deagleshots = x['shotsFired']
        deaglehits = x['shotsHit']
        deagletime = x['timeEquipped']
      case 'unica-6':
        unicakills = x['kills']
        unicaheadshots = x['headshots']
        unicashots = x['shotsFired']
        unicahits = x['shotsHit']
        unicatime = x['timeEquipped']
      case 'sw40':
        sw40kills = x['kills']
        sw40headshots = x['headshots']
        sw40shots = x['shotsFired']
        sw40hits = x['shotsHit']
        sw40time = x['timeEquipped']

  # SNIPER RIFLES
  try:
    with conn:
      if update:
        cursor.execute("""UPDATE BF4_WEAPONS_ASSAULT_STATS SET scarh_kills=?, scarh_headshots=?, scarh_shots_fired=?, scarh_shots_hit=?, scarh_time_equipped=?, m416_kills=?, m416_headshots=?, m416_shots_fired=?, m416_shots_hit=?, m416_time_equipped=?, sar21_kills=?, sar21_headshots=?, sar21_shots_fired=?, sar21_shots_hit=?, sar21_time_equipped=?, aek971_kills=?, aek971_headshots=?, aek971_shots_fired=?, aek971_shots_hit=?, aek971_time_equipped=?, famas_kills=?, famas_headshots=?, famas_shots_fired=?, famas_shots_hit=?, famas_time_equipped=?, auga3_kills=?, auga3_headshots=?, auga3_shots_fired=?, auga3_shots_hit=?, auga3_time_equipped=?, m16a4_kills=?, m16a4_headshots=?, m16a4_shots_fired=?, m16a4_shots_hit=?, m16a4_time_equipped=?, cz805_kills=?, cz805_headshots=?, cz805_shots_fired=?, cz805_shots_hit=?, cz805_time_equipped=?, ak12_kills=?, ak12_headshots=?, ak12_shots_fired=?, ak12_shots_hit=?, ak12_time_equipped=?, an94_kills=?, an94_headshots=?, an94_shots_fired=?, an94_shots_hit=?, an94_time_equipped=?, f2000_kills=?, f2000_headshots=?, f2000_shots_fired=?, f2000_shots_hit=?, f2000_time_equipped=?, ace23_kills=?, ace23_headshots=?, ace23_shots_fired=?, ace23_shots_hit=?, ace23_time_equipped=?, qbz95_kills=?, qbz95_headshots=?, qbz95_shots_fired=?, qbz95_shots_hit=?, qbz95_time_equipped=?, bulldog_kills=?, bulldog_headshots=?, bulldog_shots_fired=?, bulldog_shots_hit=?, bulldog_time_equipped=?, ar160_kills=?, ar160_headshots=?, ar160_shots_fired=?, ar160_shots_hit=?, ar160_time_equipped=?, l85a2_kills=?, l85a2_headshots=?, l85a2_shots_fired=?, l85a2_shots_hit=?, l85a2_time_equipped=? WHERE player_id=?""", (scarhkills, scarhheadshots, scarhshots, scarhhits, scarhtime, m416kills, m416headshots, m416shots, m416hits, m416time, sar21kills, sar21headshots, sar21shots, sar21hits, sar21time, aek971kills, aek971headshots, aek971shots, aek971hits, aek971time, famaskills, famasheadshots, famasshots, famashits, famastime, auga3kills, auga3headshots, auga3shots, auga3hits, auga3time, m16a4kills, m16a4headshots, m16a4shots, m16a4hits, m16a4time, cz805kills, cz805headshots, cz805shots, cz805hits, cz805time, ak12kills, ak12headshots, ak12shots, ak12hits, ak12time, an94kills, an94headshots, an94shots, an94hits, an94time, f2000kills, f2000headshots, f2000shots, f2000hits, f2000time, ace23kills, ace23headshots, ace23shots, ace23hits, ace23time, qbz95kills, qbz95headshots, qbz95shots, qbz95hits, qbz95time, bulldogkills, bulldogheadshots, bulldogshots, bulldoghits, bulldogtime, ar160kills, ar160headshots, ar160shots, ar160hits, ar160time, l85a2kills, l85a2headshots, l85a2shots, l85a2hits, l85a2time, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_WEAPONS_CARBINE_STATS SET acwr_kills=?, acwr_headshots=?, acwr_shots_fired=?, acwr_shots_hit=?, acwr_time_equipped=?, sg553_kills=?, sg553_headshots=?, sg553_shots_fired=?, sg553_shots_hit=?, sg553_time_equipped=?, aku12_kills=?, aku12_headshots=?, aku12_shots_fired=?, aku12_shots_hit=?, aku12_time_equipped=?, a91_kills=?, a91_headshots=?, a91_shots_fired=?, a91_shots_hit=?, a91_time_equipped=?, ace52_kills=?, ace52_headshots=?, ace52_shots_fired=?, ace52_shots_hit=?, ace52_time_equipped=?, g36c_kills=?, g36c_headshots=?, g36c_shots_fired=?, g36c_shots_hit=?, g36c_time_equipped=?, m4_kills=?, m4_headshots=?, m4_shots_fired=?, m4_shots_hit=?, m4_time_equipped=?, ace21_kills=?, ace21_headshots=?, ace21_shots_fired=?, ace21_shots_hit=?, ace21_time_equipped=?, type95b_kills=?, type95b_headshots=?, type95b_shots_fired=?, type95b_shots_hit=?, type95b_time_equipped=?, groza1_kills=?, groza1_headshots=?, groza1_shots_fired=?, groza1_shots_hit=?, groza1_time_equipped=?, ak5c_kills=?, ak5c_headshots=?, ak5c_shots_fired=?, ak5c_shots_hit=?, ak5c_time_equipped=?, mtar21_kills=?, mtar21_headshots=?, mtar21_shots_fired=?, mtar21_shots_hit=?, mtar21_time_equipped=?, phantom_kills=?, phantom_headshots=?, phantom_shots_fired=?, phantom_shots_hit=?, phantom_time_equipped=? WHERE player_id=?""", (acwrkills, acwrheadshots, acwrshots, acwrhits, acwrtime, sg553kills, sg553headshots, sg553shots, sg553hits, sg553time, aku12kills, aku12headshots, aku12shots, aku12hits, aku12time, a91kills, a91headshots, a91shots, a91hits, a91time, ace52kills, ace52headshots, ace52shots, ace52hits, ace52time, g36ckills, g36cheadshots, g36cshots, g36chits, g36ctime, m4kills, m4headshots, m4shots, m4hits, m4time, ace21kills, ace21headshots, ace21shots, ace21hits, ace21time, type95bkills, type95bheadshots, type95bshots, type95bhits, type95btime, groza1kills, groza1headshots, groza1shots, groza1hits, groza1time, ak5ckills, ak5cheadshots, ak5cshots, ak5chits, ak5ctime, mtar21kills, mtar21headshots, mtar21shots, mtar21hits, mtar21time, phantomkills, phantomheadshots, phantomshots, phantomhits, phantomtime, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_WEAPONS_DMR_STATS SET mk11_kills=?, mk11_headshots=?, mk11_shots_fired=?, mk11_shots_hit=?, mk11_time_equipped=?, sks_kills=?, sks_headshots=?, sks_shots_fired=?, sks_shots_hit=?, sks_time_equipped=?, svd12_kills=?, svd12_headshots=?, svd12_shots_fired=?, svd12_shots_hit=?, svd12_time_equipped=?, qbu88_kills=?, qbu88_headshots=?, qbu88_shots_fired=?, qbu88_shots_hit=?, qbu88_time_equipped=?, m39_kills=?, m39_headshots=?, m39_shots_fired=?, m39_shots_hit=?, m39_time_equipped=?, ace53_kills=?, ace53_headshots=?, ace53_shots_fired=?, ace53_shots_hit=?, ace53_time_equipped=?, scarhsv_kills=?, scarhsv_headshots=?, scarhsv_shots_fired=?, scarhsv_shots_hit=?, scarhsv_time_equipped=?, rfb_kills=?, rfb_headshots=?, rfb_shots_fired=?, rfb_shots_hit=?, rfb_time_equipped=? WHERE player_id=?""", (mk11kills, mk11headshots, mk11shots, mk11hits, mk11time, skskills, sksheadshots, sksshots, skshits, skstime, svd12kills, svd12headshots, svd12shots, svd12hits, svd12time, qbu88kills, qbu88headshots, qbu88shots, qbu88hits, qbu88time, m39kills, m39headshots, m39shots, m39hits, m39time, ace53kills, ace53headshots, ace53shots, ace53hits, ace53time, scarhsvkills, scarhsvheadshots, scarhsvshots, scarhsvhits, scarhsvtime, rfbkills, rfbheadshots, rfbshots, rfbhits, rfbtime, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_WEAPONS_GADGET_STATS SET xm25_airburst_kills=?, xm25_airburst_headshots=?, xm25_airburst_shots_fired=?, xm25_airburst_shots_hit=?, xm25_airburst_time_equipped=?, xm25_dart_kills=?, xm25_dart_headshots=?, xm25_dart_shots_fired=?, xm25_dart_shots_hit=?, xm25_dart_time_equipped=?, xm25_smoke_kills=?, xm25_smoke_headshots=?, xm25_smoke_shots_fired=?, xm25_smoke_shots_hit=?, xm25_smoke_time_equipped=?, c4_kills=?, c4_headshots=?, c4_shots_fired=?, c4_shots_hit=?, c4_time_equipped=?, m15_mine_kills=?, m15_mine_headshots=?, m15_mine_shots_fired=?, m15_mine_shots_hit=?, m15_mine_time_equipped=?, m2_slam_kills=?, m2_slam_headshots=?, m2_slam_shots_fired=?, m2_slam_shots_hit=?, m2_slam_time_equipped=?, claymore_kills=?, claymore_headshots=?, claymore_shots_fired=?, claymore_shots_hit=?, claymore_time_equipped=?, repair_tool_kills=?, repair_tool_headshots=?, repair_tool_shots_fired=?, repair_tool_shots_hit=?, repair_tool_time_equipped=?, defibrillator_kills=?, defibrillator_headshots=?, defibrillator_shots_fired=?, defibrillator_shots_hit=?, defibrillator_time_equipped=?, mbt_law_kills=?, mbt_law_headshots=?, mbt_law_shots_fired=?, mbt_law_shots_hit=?, mbt_law_time_equipped=?, stinger_kills=?, stinger_headshots=?, stinger_shots_fired=?, stinger_shots_hit=?, stinger_time_equipped=?, rpg_kills=?, rpg_headshots=?, rpg_shots_fired=?, rpg_shots_hit=?, rpg_time_equipped=?, igla_kills=?, igla_headshots=?, igla_shots_fired=?, igla_shots_hit=?, igla_time_equipped=?, smaw_kills=?, smaw_headshots=?, smaw_shots_fired=?, smaw_shots_hit=?, smaw_time_equipped=?, javelin_kills=?, javelin_headshots=?, javelin_shots_fired=?, javelin_shots_hit=?, javelin_time_equipped=?, sraw_kills=?, sraw_headshots=?, sraw_shots_fired=?, sraw_shots_hit=?, sraw_time_equipped=?, hvm_kills=?, hvm_headshots=?, hvm_shots_fired=?, hvm_shots_hit=?, hvm_time_equipped=?, m136_kills=?, m136_headshots=?, m136_shots_fired=?, m136_shots_hit=?, m136_time_equipped=?, m320_he_kills=?, m320_he_headshots=?, m320_he_shots_fired=?, m320_he_shots_hit=?, m320_he_time_equipped=?, m320_lvg_kills=?, m320_lvg_headshots=?, m320_lvg_shots_fired=?, m320_lvg_shots_hit=?, m320_lvg_time_equipped=?, m320_smk_kills=?, m320_smk_headshots=?, m320_smk_shots_fired=?, m320_smk_shots_hit=?, m320_smk_time_equipped=?, m320_dart_kills=?, m320_dart_headshots=?, m320_dart_shots_fired=?, m320_dart_shots_hit=?, m320_dart_time_equipped=?, m320_fb_kills=?, m320_fb_headshots=?, m320_fb_shots_fired=?, m320_fb_shots_hit=?, m320_fb_time_equipped=?, m320_3gl_kills=?, m320_3gl_headshots=?, m320_3gl_shots_fired=?, m320_3gl_shots_hit=?, m320_3gl_time_equipped=?, ballistic_shield_kills=?, ballistic_shield_headshots=?, ballistic_shield_shots_fired=?, ballistic_shield_shots_hit=?, ballistic_shield_time_equipped=?, rorsch_mk1_kills=?, rorsch_mk1_headshots=?, rorsch_mk1_shots_fired=?, rorsch_mk1_shots_hit=?, rorsch_mk1_time_equipped=?, m32_mgl_kills=?, m32_mgl_headshots=?, m32_mgl_shots_fired=?, m32_mgl_shots_hit=?, m32_mgl_time_equipped=? WHERE player_id=?""", (xm25airkills, xm25airheadshots, xm25airshots, xm25airhits, xm25airtime, xm25dartkills, xm25dartheadshots, xm25dartshots, xm25darthits, xm25darttime, xm25smokekills, xm25smokeheadshots, xm25smokeshots, xm25smokehits, xm25smoketime, c4kills, c4headshots, c4shots, c4hits, c4time, m15minekills, m15mineheadshots, m15mineshots, m15minehits, m15minetime, m2slamkills, m2slamheadshots, m2slamshots, m2slamhits, m2slamtime, claymorekills, claymoreheadshots, claymoreshots, claymorehits, claymoretime, repairtoolkills, repairtoolheadshots, repairtoolshots, repairtoolhits, repairtooltime, defibkills, defibheadshots, defibshots, defibhits, defibtime, lawkills, lawheadshots, lawshots, lawhits, lawtime, stingerkills, stingerheadshots, stingershots, stingerhits, stingertime, rpgkills, rpgheadshots, rpgshots, rpghits, rpgtime, iglakills, iglaheadshots, iglashots, iglahits, iglatime, smawkills, smawheadshots, smawshots, smawhits, smawtime, javelinkills, javelinheadshots, javelinshots, javelinhits, javelintime, srawkills, srawheadshots, srawshots, srawhits, srawtime, hvmkills, hvmheadshots, hvmshots, hvmhits, hvmtime, m136kills, m136headshots, m136shots, m136hits, m136time, m320hekills, m320heheadshots, m320heshots, m320hehits, m320hetime, m320lvgkills, m320lvgheadshots, m320lvgshots, m320lvghits, m320lvgtime, m320smkkills, m320smkheadshots, m320smkshots, m320smkhits, m320smktime, m320dartkills, m320dartheadshots, m320dartshots, m320darthits, m320darttime, m320fbkills, m320fbheadshots, m320fbshots, m320fbhits, m320fbtime, m3203glkills, m3203glheadshots, m3203glshots, m3203glhits, m3203gltime, shieldkills, shieldheadshots, shieldshots, shieldhits, shieldtime, railgunkills, railgunheadshots, railgunshots, railgunhits, railguntime, m32kills, m32headshots, m32shots, m32hits, m32time, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_WEAPONS_GRENADE_STATS SET v40_mini_kills=?, v40_mini_headshots=?, v40_mini_shots_fired=?, v40_mini_shots_hit=?, v40_mini_time_equipped=?, rgo_impact_kills=?, rgo_impact_headshots=?, rgo_impact_shots_fired=?, rgo_impact_shots_hit=?, rgo_impact_time_equipped=?, m34_incendiary_kills=?, m34_incendiary_headshots=?, m34_incendiary_shots_fired=?, m34_incendiary_shots_hit=?, m34_incendiary_time_equipped=?, m18_smoke_kills=?, m18_smoke_headshots=?, m18_smoke_shots_fired=?, m18_smoke_shots_hit=?, m18_smoke_time_equipped=?, m84_flashbang_kills=?, m84_flashbang_headshots=?, m84_flashbang_shots_fired=?, m84_flashbang_shots_hit=?, m84_flashbang_time_equipped=?, hand_flare_kills=?, hand_flare_headshots=?, hand_flare_shots_fired=?, hand_flare_shots_hit=?, hand_flare_time_equipped=?, m67_frag_kills=?, m67_frag_headshots=?, m67_frag_shots_fired=?, m67_frag_shots_hit=?, m67_frag_time_equipped=? WHERE player_id=?""", (v40minikills, v40miniheadshots, v40minishots, v40minihits, v40minitime, rgokills, rgoheadshots, rgoshots, rgohits, rgotime, m34kills, m34headshots, m34shots, m34hits, m34time, m18smokekills, m18smokeheadshots, m18smokeshots, m18smokehits, m18smoketime, m84kills, m84headshots, m84shots, m84hits, m84time, flarekills, flareheadshots, flareshots, flarehits, flaretime, m67kills, m67headshots, m67shots, m67hits, m67time, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_WEAPONS_KNIFE_STATS SET bj2_kills=?, weaver_kills=?, bayonet_kills=?, scout_kills=?, acb90_kills=?, seal_kills=?, trench_kills=?, bowie_kills=?, precision_kills=?, survival_kills=?, carbon_fiber_kills=?, improvised_kills=?, tanto_kills=?, neck_kills=?, tactical_kills=?, boot_kills=?, dive_kills=?, shank_kills=?, machete_kills=?, c100_kills=? WHERE player_id=?""", (bj2, weaver, bayonet, scout, acb90, seal, trench, bowie, precision, survival, carbonFiber, improvised, tanto, neck, tactical, boot, dive, shank, machete, c100, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_WEAPONS_LMG_STATS SET type88_kills=?, type88_headshots=?, type88_shots_fired=?, type88_shots_hit=?, type88_time_equipped=?, lsat_kills=?, lsat_headshots=?, lsat_shots_fired=?, lsat_shots_hit=?, lsat_time_equipped=?, pkp_kills=?, pkp_headshots=?, pkp_shots_fired=?, pkp_shots_hit=?, pkp_time_equipped=?, qbb95_kills=?, qbb95_headshots=?, qbb95_shots_fired=?, qbb95_shots_hit=?, qbb95_time_equipped=?, m240b_kills=?, m240b_headshots=?, m240b_shots_fired=?, m240b_shots_hit=?, m240b_time_equipped=?, mg4_kills=?, mg4_headshots=?, mg4_shots_fired=?, mg4_shots_hit=?, mg4_time_equipped=?, u100_kills=?, u100_headshots=?, u100_shots_fired=?, u100_shots_hit=?, u100_time_equipped=?, l86a2_kills=?, l86a2_headshots=?, l86a2_shots_fired=?, l86a2_shots_hit=?, l86a2_time_equipped=?, aws_kills=?, aws_headshots=?, aws_shots_fired=?, aws_shots_hit=?, aws_time_equipped=?, m60e4_kills=?, m60e4_headshots=?, m60e4_shots_fired=?, m60e4_shots_hit=?, m60e4_time_equipped=?, rpk_kills=?, rpk_headshots=?, rpk_shots_fired=?, rpk_shots_hit=?, rpk_time_equipped=?, m249_kills=?, m249_headshots=?, m249_shots_fired=?, m249_shots_hit=?, m249_time_equipped=?, rpk12_kills=?, rpk12_headshots=?, rpk12_shots_fired=?, rpk12_shots_hit=?, rpk12_time_equipped=?, m60ult_kills=?, m60ult_headshots=?, m60ult_shots_fired=?, m60ult_shots_hit=?, m60ult_time_equipped=? WHERE player_id=?""", (type88kills, type88headshots, type88shots, type88hits, type88time, lsatkills, lsatheadshots, lsatshots, lsathits, lsattime, pkpkills, pkpheadshots, pkpshots, pkphits, pkptime, qbb95kills, qbb95headshots, qbb95shots, qbb95hits, qbb95time, m240bkills, m240bheadshots, m240bshots, m240bhits, m240btime, mg4kills, mg4headshots, mg4shots, mg4hits, mg4time, u100kills, u100headshots, u100shots, u100hits, u100time, l86a2kills, l86a2headshots, l86a2shots, l86a2hits, l86a2time, awskills, awsheadshots, awsshots, awshits, awstime, m60kills, m60headshots, m60shots, m60hits, m60time, rpkkills, rpkheadshots, rpkshots, rpkhits, rpktime, m249kills, m249headshots, m249shots, m249hits, m249time, rpk12kills, rpk12headshots, rpk12shots, rpk12hits, rpk12time, m60ultkills, m60ultheadshots, m60ultshots, m60ulthits, m60ulttime, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_WEAPONS_PDW_STATS SET pp2000_kills=?, pp2000_headshots=?, pp2000_shots_fired=?, pp2000_shots_hit=?, pp2000_time_equipped=?, ump45_kills=?, ump45_headshots=?, ump45_shots_fired=?, ump45_shots_hit=?, ump45_time_equipped=?, cbjms_kills=?, cbjms_headshots=?, cbjms_shots_fired=?, cbjms_shots_hit=?, cbjms_time_equipped=?, pdwr_kills=?, pdwr_headshots=?, pdwr_shots_fired=?, pdwr_shots_hit=?, pdwr_time_equipped=?, cz3a1_kills=?, cz3a1_headshots=?, cz3a1_shots_fired=?, cz3a1_shots_hit=?, cz3a1_time_equipped=?, js2_kills=?, js2_headshots=?, js2_shots_fired=?, js2_shots_hit=?, js2_time_equipped=?, groza4_kills=?, groza4_headshots=?, groza4_shots_fired=?, groza4_shots_hit=?, groza4_time_equipped=?, mx4_kills=?, mx4_headshots=?, mx4_shots_fired=?, mx4_shots_hit=?, mx4_time_equipped=?, asval_kills=?, asval_headshots=?, asval_shots_fired=?, asval_shots_hit=?, asval_time_equipped=?, p90_kills=?, p90_headshots=?, p90_shots_fired=?, p90_shots_hit=?, p90_time_equipped=?, mpx_kills=?, mpx_headshots=?, mpx_shots_fired=?, mpx_shots_hit=?, mpx_time_equipped=?, ump9_kills=?, ump9_headshots=?, ump9_shots_fired=?, ump9_shots_hit=?, ump9_time_equipped=?, mp7_kills=?, mp7_headshots=?, mp7_shots_fired=?, mp7_shots_hit=?, mp7_time_equipped=?, sr2_kills=?, sr2_headshots=?, sr2_shots_fired=?, sr2_shots_hit=?, sr2_time_equipped=? WHERE player_id=?""", (pp2000kills, pp2000headshots, pp2000shots, pp2000hits, pp2000time, ump45kills, ump45headshots, ump45shots, ump45hits, ump45time, cbjmskills, cbjmsheadshots, cbjmsshots, cbjmshits, cbjmstime, pdwrkills, pdwrheadshots, pdwrshots, pdwrhits, pdwrtime, cz3a1kills, cz3a1headshots, cz3a1shots, cz3a1hits, cz3a1time, js2kills, js2headshots, js2shots, js2hits, js2time, groza4kills, groza4headshots, groza4shots, groza4hits, groza4time, mx4kills, mx4headshots, mx4shots, mx4hits, mx4time, asvalkills, asvalheadshots, asvalshots, asvalhits, asvaltime, p90kills, p90headshots, p90shots, p90hits, p90time, mpxkills, mpxheadshots, mpxshots, mpxhits, mpxtime, ump9kills, ump9headshots, ump9shots, ump9hits, ump9time, mp7kills, mp7headshots, mp7shots, mp7hits, mp7time, sr2kills, sr2headshots, sr2shots, sr2hits, sr2time, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_WEAPONS_SHOTGUN_STATS SET mcs870_kills=?, mcs870_headshots=?, mcs870_shots_fired=?, mcs870_shots_hit=?, mcs870_time_equipped=?, m1014_kills=?, m1014_headshots=?, m1014_shots_fired=?, m1014_shots_hit=?, m1014_time_equipped=?, hawk12g_kills=?, hawk12g_headshots=?, hawk12g_shots_fired=?, hawk12g_shots_hit=?, hawk12g_time_equipped=?, saiga12k_kills=?, saiga12k_headshots=?, saiga12k_shots_fired=?, saiga12k_shots_hit=?, saiga12k_time_equipped=?, spas12_kills=?, spas12_headshots=?, spas12_shots_fired=?, spas12_shots_hit=?, spas12_time_equipped=?, uts15_kills=?, uts15_headshots=?, uts15_shots_fired=?, uts15_shots_hit=?, uts15_time_equipped=?, dbv12_kills=?, dbv12_headshots=?, dbv12_shots_fired=?, dbv12_shots_hit=?, dbv12_time_equipped=?, m26_frag_kills=?, m26_frag_headshots=?, m26_frag_shots_fired=?, m26_frag_shots_hit=?, m26_frag_time_equipped=?, m26_slug_kills=?, m26_slug_headshots=?, m26_slug_shots_fired=?, m26_slug_shots_hit=?, m26_slug_time_equipped=?, m26_dart_kills=?, m26_dart_headshots=?, m26_dart_shots_fired=?, m26_dart_shots_hit=?, m26_dart_time_equipped=?, m26_mass_kills=?, m26_mass_headshots=?, m26_mass_shots_fired=?, m26_mass_shots_hit=?, m26_mass_time_equipped=?, qbs09_kills=?, qbs09_headshots=?, qbs09_shots_fired=?, qbs09_shots_hit=?, qbs09_time_equipped=?, dao12_kills=?, dao12_headshots=?, dao12_shots_fired=?, dao12_shots_hit=?, dao12_time_equipped=?, usas12_kills=?, usas12_headshots=?, usas12_shots_fired=?, usas12_shots_hit=?, usas12_time_equipped=? WHERE player_id=?""", (mcs870kills, mcs870headshots, mcs870shots, mcs870hits, mcs870time, m1014kills, m1014headshots, m1014shots, m1014hits, m1014time, hawk12gkills, hawk12gheadshots, hawk12gshots, hawk12ghits, hawk12gtime, saigakills, saigaheadshots, saigashots, saigahits, saigatime, spas12kills, spas12headshots, spas12shots, spas12hits, spas12time, uts15kills, uts15headshots, uts15shots, uts15hits, uts15time, dbv12kills, dbv12headshots, dbv12shots, dbv12hits, dbv12time, m26fragkills, m26fragheadshots, m26fragshots, m26fraghits, m26fragtime, m26slugkills, m26slugheadshots, m26slugshots, m26slughits, m26slugtime, m26dartkills, m26dartheadshots, m26dartshots, m26darthits, m26darttime, m26masskills, m26massheadshots, m26massshots, m26masshits, m26masstime, qbs09kills, qbs09headshots, qbs09shots, qbs09hits, qbs09time, dao12kills, dao12headshots, dao12shots, dao12hits, dao12time, usas12kills, usas12headshots, usas12shots, usas12hits, usas12time, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_WEAPONS_SIDEARM_STATS SET m9_kills=?, m9_headshots=?, m9_shots_fired=?, m9_shots_hit=?, m9_time_equipped=?, qsz92_kills=?, qsz92_headshots=?, qsz92_shots_fired=?, qsz92_shots_hit=?, qsz92_time_equipped=?, mp443_kills=?, mp443_headshots=?, mp443_shots_fired=?, mp443_shots_hit=?, mp443_time_equipped=?, shorty_kills=?, shorty_headshots=?, shorty_shots_fired=?, shorty_shots_hit=?, shorty_time_equipped=?, g18_kills=?, g18_headshots=?, g18_shots_fired=?, g18_shots_hit=?, g18_time_equipped=?, fn57_kills=?, fn57_headshots=?, fn57_shots_fired=?, fn57_shots_hit=?, fn57_time_equipped=?, m1911_kills=?, m1911_headshots=?, m1911_shots_fired=?, m1911_shots_hit=?, m1911_time_equipped=?, r93_kills=?, r93_headshots=?, r93_shots_fired=?, r93_shots_hit=?, r93_time_equipped=?, cz75_kills=?, cz75_headshots=?, cz75_shots_fired=?, cz75_shots_hit=?, cz75_time_equipped=?, magnum44_kills=?, magnum44_headshots=?, magnum44_shots_fired=?, magnum44_shots_hit=?, magnum44_time_equipped=?, compact45_kills=?, compact45_headshots=?, compact45_shots_fired=?, compact45_shots_hit=?, compact45_time_equipped=?, p226_kills=?, p226_headshots=?, p226_shots_fired=?, p226_shots_hit=?, p226_time_equipped=?, mares_leg_kills=?, mares_leg_headshots=?, mares_leg_shots_fired=?, mares_leg_shots_hit=?, mares_leg_time_equipped=?, mp412_kills=?, mp412_headshots=?, mp412_shots_fired=?, mp412_shots_hit=?, mp412_time_equipped=?, deagle_kills=?, deagle_headshots=?, deagle_shots_fired=?, deagle_shots_hit=?, deagle_time_equipped=?, unica_kills=?, unica_headshots=?, unica_shots_fired=?, unica_shots_hit=?, unica_time_equipped=?, sw40_kills=?, sw40_headshots=?, sw40_shots_fired=?, sw40_shots_hit=?, sw40_time_equipped=? WHERE player_id=?""", (m9kills, m9headshots, m9shots, m9hits, m9time, qsz92kills, qsz92headshots, qsz92shots, qsz92hits, qsz92time, mp443kills, mp443headshots, mp443shots, mp443hits, mp443time, shortykills, shortyheadshots, shortyshots, shortyhits, shortytime, g18kills, g18headshots, g18shots, g18hits, g18time, fn57kills, fn57headshots, fn57shots, fn57hits, fn57time, m1911kills, m1911headshots, m1911shots, m1911hits, m1911time, r93kills, r93headshots, r93shots, r93hits, r93time, cz75kills, cz75headshots, cz75shots, cz75hits, cz75time, magnumkills, magnumheadshots, magnumshots, magnumhits, magnumtime, compactkills, compactheadshots, compactshots, compacthits, compacttime, p226kills, p226headshots, p226shots, p226hits, p226time, mareskills, maresheadshots, maresshots, mareshits, marestime, m412kills, m412headshots, m412shots, m412hits, m412time, deaglekills, deagleheadshots, deagleshots, deaglehits, deagletime, unicakills, unicaheadshots, unicashots, unicahits, unicatime, sw40kills, sw40headshots, sw40shots, sw40hits, sw40time, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_WEAPONS_SNIPER_STATS SET m40a5_kills=?, m40a5_headshots=?, m40a5_shots_fired=?, m40a5_shots_hit=?, m40a5_time_equipped=?, scout_elite_kills=?, scout_elite_headshots=?, scout_elite_shots_fired=?, scout_elite_shots_hit=?, scout_elite_time_equipped=?, sv98_kills=?, sv98_headshots=?, sv98_shots_fired=?, sv98_shots_hit=?, sv98_time_equipped=?, jng90_kills=?, jng90_headshots=?, jng90_shots_fired=?, jng90_shots_hit=?, jng90_time_equipped=?, recon338_kills=?, recon338_headshots=?, recon338_shots_fired=?, recon338_shots_hit=?, recon338_time_equipped=?, m98b_kills=?, m98b_headshots=?, m98b_shots_fired=?, m98b_shots_hit=?, m98b_time_equipped=?, srr61_kills=?, srr61_headshots=?, srr61_shots_fired=?, srr61_shots_hit=?, srr61_time_equipped=?, cslr4_kills=?, cslr4_headshots=?, cslr4_shots_fired=?, cslr4_shots_hit=?, cslr4_time_equipped=?, l115_kills=?, l115_headshots=?, l115_shots_fired=?, l115_shots_hit=?, l115_time_equipped=?, gol_magnum_kills=?, gol_magnum_headshots=?, gol_magnum_shots_fired=?, gol_magnum_shots_hit=?, gol_magnum_time_equipped=?, fyjs_kills=?, fyjs_headshots=?, fyjs_shots_fired=?, fyjs_shots_hit=?, fyjs_time_equipped=?, sr338_kills=?, sr338_headshots=?, sr338_shots_fired=?, sr338_shots_hit=?, sr338_time_equipped=?, cs5_kills=?, cs5_headshots=?, cs5_shots_fired=?, cs5_shots_hit=?, cs5_time_equipped=?, m82_kills=?, m82_headshots=?, m82_shots_fired=?, m82_shots_hit=?, m82_time_equipped=?, amr2_kills=?, amr2_headshots=?, amr2_shots_fired=?, amr2_shots_hit=?, amr2_time_equipped=? WHERE player_id=?""", (m40kills, m40headshots, m40shots, m40hits, m40time, scoutkills, scoutheadshots, scoutshots, scouthits, scouttime, sv98kills, sv98headshots, sv98shots, sv98hits, sv98time, jng90kills, jng90headshots, jng90shots, jng90hits, jng90time, recon338kills, recon338headshots, recon338shots, recon338hits, recon338time, m98bkills, m98bheadshots, m98bshots, m98bhits, m98btime, srr61kills, srr61headshots, srr61shots, srr61hits, srr61time, cslr4kills, cslr4headshots, cslr4shots, cslr4hits, cslr4time, l115kills, l115headshots, l115shots, l115hits, l115time, golkills, golheadshots, golshots, golhits, goltime, fyjskills, fyjsheadshots, fyjsshots, fyjshits, fyjstime, sr338kills, sr338headshots, sr338shots, sr338hits, sr338time, cs5kills, cs5headshots, cs5shots, cs5hits, cs5time, m82kills, m82headshots, m82shots, m82hits, m82time, amr2kills, amr2headshots, amr2shots, amr2hits, amr2time, playerId))
        conn.commit()
      else:
        cursor.execute("""INSERT INTO BF4_WEAPONS_ASSAULT_STATS (scarh_kills, scarh_headshots, scarh_shots_fired, scarh_shots_hit, scarh_time_equipped, m416_kills, m416_headshots, m416_shots_fired, m416_shots_hit, m416_time_equipped, sar21_kills, sar21_headshots, sar21_shots_fired, sar21_shots_hit, sar21_time_equipped, aek971_kills, aek971_headshots, aek971_shots_fired, aek971_shots_hit, aek971_time_equipped, famas_kills, famas_headshots, famas_shots_fired, famas_shots_hit, famas_time_equipped, auga3_kills, auga3_headshots, auga3_shots_fired, auga3_shots_hit, auga3_time_equipped, m16a4_kills, m16a4_headshots, m16a4_shots_fired, m16a4_shots_hit, m16a4_time_equipped, cz805_kills, cz805_headshots, cz805_shots_fired, cz805_shots_hit, cz805_time_equipped, ak12_kills, ak12_headshots, ak12_shots_fired, ak12_shots_hit, ak12_time_equipped, an94_kills, an94_headshots, an94_shots_fired, an94_shots_hit, an94_time_equipped, f2000_kills, f2000_headshots, f2000_shots_fired, f2000_shots_hit, f2000_time_equipped, ace23_kills, ace23_headshots, ace23_shots_fired, ace23_shots_hit, ace23_time_equipped, qbz95_kills, qbz95_headshots, qbz95_shots_fired, qbz95_shots_hit, qbz95_time_equipped, bulldog_kills, bulldog_headshots, bulldog_shots_fired, bulldog_shots_hit, bulldog_time_equipped, ar160_kills, ar160_headshots, ar160_shots_fired, ar160_shots_hit, ar160_time_equipped, l85a2_kills, l85a2_headshots, l85a2_shots_fired, l85a2_shots_hit, l85a2_time_equipped, player_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""", (scarhkills, scarhheadshots, scarhshots, scarhhits, scarhtime, m416kills, m416headshots, m416shots, m416hits, m416time, sar21kills, sar21headshots, sar21shots, sar21hits, sar21time, aek971kills, aek971headshots, aek971shots, aek971hits, aek971time, famaskills, famasheadshots, famasshots, famashits, famastime, auga3kills, auga3headshots, auga3shots, auga3hits, auga3time, m16a4kills, m16a4headshots, m16a4shots, m16a4hits, m16a4time, cz805kills, cz805headshots, cz805shots, cz805hits, cz805time, ak12kills, ak12headshots, ak12shots, ak12hits, ak12time, an94kills, an94headshots, an94shots, an94hits, an94time, f2000kills, f2000headshots, f2000shots, f2000hits, f2000time, ace23kills, ace23headshots, ace23shots, ace23hits, ace23time, qbz95kills, qbz95headshots, qbz95shots, qbz95hits, qbz95time, bulldogkills, bulldogheadshots, bulldogshots, bulldoghits, bulldogtime, ar160kills, ar160headshots, ar160shots, ar160hits, ar160time, l85a2kills, l85a2headshots, l85a2shots, l85a2hits, l85a2time, playerId))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_WEAPONS_CARBINE_STATS (acwr_kills, acwr_headshots, acwr_shots_fired, acwr_shots_hit, acwr_time_equipped, sg553_kills, sg553_headshots, sg553_shots_fired, sg553_shots_hit, sg553_time_equipped, aku12_kills, aku12_headshots, aku12_shots_fired, aku12_shots_hit, aku12_time_equipped, a91_kills, a91_headshots, a91_shots_fired, a91_shots_hit, a91_time_equipped, ace52_kills, ace52_headshots, ace52_shots_fired, ace52_shots_hit, ace52_time_equipped, g36c_kills, g36c_headshots, g36c_shots_fired, g36c_shots_hit, g36c_time_equipped, m4_kills, m4_headshots, m4_shots_fired, m4_shots_hit, m4_time_equipped, ace21_kills, ace21_headshots, ace21_shots_fired, ace21_shots_hit, ace21_time_equipped, type95b_kills, type95b_headshots, type95b_shots_fired, type95b_shots_hit, type95b_time_equipped, groza1_kills, groza1_headshots, groza1_shots_fired, groza1_shots_hit, groza1_time_equipped, ak5c_kills, ak5c_headshots, ak5c_shots_fired, ak5c_shots_hit, ak5c_time_equipped, mtar21_kills, mtar21_headshots, mtar21_shots_fired, mtar21_shots_hit, mtar21_time_equipped, phantom_kills, phantom_headshots, phantom_shots_fired, phantom_shots_hit, phantom_time_equipped, player_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""", (acwrkills, acwrheadshots, acwrshots, acwrhits, acwrtime, sg553kills, sg553headshots, sg553shots, sg553hits, sg553time, aku12kills, aku12headshots, aku12shots, aku12hits, aku12time, a91kills, a91headshots, a91shots, a91hits, a91time, ace52kills, ace52headshots, ace52shots, ace52hits, ace52time, g36ckills, g36cheadshots, g36cshots, g36chits, g36ctime, m4kills, m4headshots, m4shots, m4hits, m4time, ace21kills, ace21headshots, ace21shots, ace21hits, ace21time, type95bkills, type95bheadshots, type95bshots, type95bhits, type95btime, groza1kills, groza1headshots, groza1shots, groza1hits, groza1time, ak5ckills, ak5cheadshots, ak5cshots, ak5chits, ak5ctime, mtar21kills, mtar21headshots, mtar21shots, mtar21hits, mtar21time, phantomkills, phantomheadshots, phantomshots, phantomhits, phantomtime, playerId))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_WEAPONS_DMR_STATS (mk11_kills, mk11_headshots, mk11_shots_fired, mk11_shots_hit, mk11_time_equipped, sks_kills, sks_headshots, sks_shots_fired, sks_shots_hit, sks_time_equipped, svd12_kills, svd12_headshots, svd12_shots_fired, svd12_shots_hit, svd12_time_equipped, qbu88_kills, qbu88_headshots, qbu88_shots_fired, qbu88_shots_hit, qbu88_time_equipped, m39_kills, m39_headshots, m39_shots_fired, m39_shots_hit, m39_time_equipped, ace53_kills, ace53_headshots, ace53_shots_fired, ace53_shots_hit, ace53_time_equipped, scarhsv_kills, scarhsv_headshots, scarhsv_shots_fired, scarhsv_shots_hit, scarhsv_time_equipped, rfb_kills, rfb_headshots, rfb_shots_fired, rfb_shots_hit, rfb_time_equipped, player_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""", (mk11kills, mk11headshots, mk11shots, mk11hits, mk11time, skskills, sksheadshots, sksshots, skshits, skstime, svd12kills, svd12headshots, svd12shots, svd12hits, svd12time, qbu88kills, qbu88headshots, qbu88shots, qbu88hits, qbu88time, m39kills, m39headshots, m39shots, m39hits, m39time, ace53kills, ace53headshots, ace53shots, ace53hits, ace53time, scarhsvkills, scarhsvheadshots, scarhsvshots, scarhsvhits, scarhsvtime, rfbkills, rfbheadshots, rfbshots, rfbhits, rfbtime, playerId))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_WEAPONS_GADGET_STATS (xm25_airburst_kills, xm25_airburst_headshots, xm25_airburst_shots_fired, xm25_airburst_shots_hit, xm25_airburst_time_equipped, xm25_dart_kills, xm25_dart_headshots, xm25_dart_shots_fired, xm25_dart_shots_hit, xm25_dart_time_equipped, xm25_smoke_kills, xm25_smoke_headshots, xm25_smoke_shots_fired, xm25_smoke_shots_hit, xm25_smoke_time_equipped, c4_kills, c4_headshots, c4_shots_fired, c4_shots_hit, c4_time_equipped, m15_mine_kills, m15_mine_headshots, m15_mine_shots_fired, m15_mine_shots_hit, m15_mine_time_equipped, m2_slam_kills, m2_slam_headshots, m2_slam_shots_fired, m2_slam_shots_hit, m2_slam_time_equipped, claymore_kills, claymore_headshots, claymore_shots_fired, claymore_shots_hit, claymore_time_equipped, repair_tool_kills, repair_tool_headshots, repair_tool_shots_fired, repair_tool_shots_hit, repair_tool_time_equipped, defibrillator_kills, defibrillator_headshots, defibrillator_shots_fired, defibrillator_shots_hit, defibrillator_time_equipped, mbt_law_kills, mbt_law_headshots, mbt_law_shots_fired, mbt_law_shots_hit, mbt_law_time_equipped, stinger_kills, stinger_headshots, stinger_shots_fired, stinger_shots_hit, stinger_time_equipped, rpg_kills, rpg_headshots, rpg_shots_fired, rpg_shots_hit, rpg_time_equipped, igla_kills, igla_headshots, igla_shots_fired, igla_shots_hit, igla_time_equipped, smaw_kills, smaw_headshots, smaw_shots_fired, smaw_shots_hit, smaw_time_equipped, javelin_kills, javelin_headshots, javelin_shots_fired, javelin_shots_hit, javelin_time_equipped, sraw_kills, sraw_headshots, sraw_shots_fired, sraw_shots_hit, sraw_time_equipped, hvm_kills, hvm_headshots, hvm_shots_fired, hvm_shots_hit, hvm_time_equipped, m136_kills, m136_headshots, m136_shots_fired, m136_shots_hit, m136_time_equipped, m320_he_kills, m320_he_headshots, m320_he_shots_fired, m320_he_shots_hit, m320_he_time_equipped, m320_lvg_kills, m320_lvg_headshots, m320_lvg_shots_fired, m320_lvg_shots_hit, m320_lvg_time_equipped, m320_smk_kills, m320_smk_headshots, m320_smk_shots_fired, m320_smk_shots_hit, m320_smk_time_equipped, m320_dart_kills, m320_dart_headshots, m320_dart_shots_fired, m320_dart_shots_hit, m320_dart_time_equipped, m320_fb_kills, m320_fb_headshots, m320_fb_shots_fired, m320_fb_shots_hit, m320_fb_time_equipped, m320_3gl_kills, m320_3gl_headshots, m320_3gl_shots_fired, m320_3gl_shots_hit, m320_3gl_time_equipped, ballistic_shield_kills, ballistic_shield_headshots, ballistic_shield_shots_fired, ballistic_shield_shots_hit, ballistic_shield_time_equipped, rorsch_mk1_kills, rorsch_mk1_headshots, rorsch_mk1_shots_fired, rorsch_mk1_shots_hit, rorsch_mk1_time_equipped, m32_mgl_kills, m32_mgl_headshots, m32_mgl_shots_fired, m32_mgl_shots_hit, m32_mgl_time_equipped, player_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""", (xm25airkills, xm25airheadshots, xm25airshots, xm25airhits, xm25airtime, xm25dartkills, xm25dartheadshots, xm25dartshots, xm25darthits, xm25darttime, xm25smokekills, xm25smokeheadshots, xm25smokeshots, xm25smokehits, xm25smoketime, c4kills, c4headshots, c4shots, c4hits, c4time, m15minekills, m15mineheadshots, m15mineshots, m15minehits, m15minetime, m2slamkills, m2slamheadshots, m2slamshots, m2slamhits, m2slamtime, claymorekills, claymoreheadshots, claymoreshots, claymorehits, claymoretime, repairtoolkills, repairtoolheadshots, repairtoolshots, repairtoolhits, repairtooltime, defibkills, defibheadshots, defibshots, defibhits, defibtime, lawkills, lawheadshots, lawshots, lawhits, lawtime, stingerkills, stingerheadshots, stingershots, stingerhits, stingertime, rpgkills, rpgheadshots, rpgshots, rpghits, rpgtime, iglakills, iglaheadshots, iglashots, iglahits, iglatime, smawkills, smawheadshots, smawshots, smawhits, smawtime, javelinkills, javelinheadshots, javelinshots, javelinhits, javelintime, srawkills, srawheadshots, srawshots, srawhits, srawtime, hvmkills, hvmheadshots, hvmshots, hvmhits, hvmtime, m136kills, m136headshots, m136shots, m136hits, m136time, m320hekills, m320heheadshots, m320heshots, m320hehits, m320hetime, m320lvgkills, m320lvgheadshots, m320lvgshots, m320lvghits, m320lvgtime, m320smkkills, m320smkheadshots, m320smkshots, m320smkhits, m320smktime, m320dartkills, m320dartheadshots, m320dartshots, m320darthits, m320darttime, m320fbkills, m320fbheadshots, m320fbshots, m320fbhits, m320fbtime, m3203glkills, m3203glheadshots, m3203glshots, m3203glhits, m3203gltime, shieldkills, shieldheadshots, shieldshots, shieldhits, shieldtime, railgunkills, railgunheadshots, railgunshots, railgunhits, railguntime, m32kills, m32headshots, m32shots, m32hits, m32time, playerId))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_WEAPONS_GRENADE_STATS (v40_mini_kills, v40_mini_headshots, v40_mini_shots_fired, v40_mini_shots_hit, v40_mini_time_equipped, rgo_impact_kills, rgo_impact_headshots, rgo_impact_shots_fired, rgo_impact_shots_hit, rgo_impact_time_equipped, m34_incendiary_kills, m34_incendiary_headshots, m34_incendiary_shots_fired, m34_incendiary_shots_hit, m34_incendiary_time_equipped, m18_smoke_kills, m18_smoke_headshots, m18_smoke_shots_fired, m18_smoke_shots_hit, m18_smoke_time_equipped, m84_flashbang_kills, m84_flashbang_headshots, m84_flashbang_shots_fired, m84_flashbang_shots_hit, m84_flashbang_time_equipped, hand_flare_kills, hand_flare_headshots, hand_flare_shots_fired, hand_flare_shots_hit, hand_flare_time_equipped, m67_frag_kills, m67_frag_headshots, m67_frag_shots_fired, m67_frag_shots_hit, m67_frag_time_equipped, player_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""", (v40minikills, v40miniheadshots, v40minishots, v40minihits, v40minitime, rgokills, rgoheadshots, rgoshots, rgohits, rgotime, m34kills, m34headshots, m34shots, m34hits, m34time, m18smokekills, m18smokeheadshots, m18smokeshots, m18smokehits, m18smoketime, m84kills, m84headshots, m84shots, m84hits, m84time, flarekills, flareheadshots, flareshots, flarehits, flaretime, m67kills, m67headshots, m67shots, m67hits, m67time, playerId))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_WEAPONS_KNIFE_STATS (bj2_kills, weaver_kills, bayonet_kills, scout_kills, acb90_kills, seal_kills, trench_kills, bowie_kills, precision_kills, survival_kills, carbon_fiber_kills, improvised_kills, tanto_kills, neck_kills, tactical_kills, boot_kills, dive_kills, shank_kills, machete_kills, c100_kills, player_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""", (bj2, weaver, bayonet, scout, acb90, seal, trench, bowie, precision, survival, carbonFiber, improvised, tanto, neck, tactical, boot, dive, shank, machete, c100, playerId))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_WEAPONS_LMG_STATS (type88_kills, type88_headshots, type88_shots_fired, type88_shots_hit, type88_time_equipped, lsat_kills, lsat_headshots, lsat_shots_fired, lsat_shots_hit, lsat_time_equipped, pkp_kills, pkp_headshots, pkp_shots_fired, pkp_shots_hit, pkp_time_equipped, qbb95_kills, qbb95_headshots, qbb95_shots_fired, qbb95_shots_hit, qbb95_time_equipped, m240b_kills, m240b_headshots, m240b_shots_fired, m240b_shots_hit, m240b_time_equipped, mg4_kills, mg4_headshots, mg4_shots_fired, mg4_shots_hit, mg4_time_equipped, u100_kills, u100_headshots, u100_shots_fired, u100_shots_hit, u100_time_equipped, l86a2_kills, l86a2_headshots, l86a2_shots_fired, l86a2_shots_hit, l86a2_time_equipped, aws_kills, aws_headshots, aws_shots_fired, aws_shots_hit, aws_time_equipped, m60e4_kills, m60e4_headshots, m60e4_shots_fired, m60e4_shots_hit, m60e4_time_equipped, rpk_kills, rpk_headshots, rpk_shots_fired, rpk_shots_hit, rpk_time_equipped, m249_kills, m249_headshots, m249_shots_fired, m249_shots_hit, m249_time_equipped, rpk12_kills, rpk12_headshots, rpk12_shots_fired, rpk12_shots_hit, rpk12_time_equipped, m60ult_kills, m60ult_headshots, m60ult_shots_fired, m60ult_shots_hit, m60ult_time_equipped, player_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""", (type88kills, type88headshots, type88shots, type88hits, type88time, lsatkills, lsatheadshots, lsatshots, lsathits, lsattime, pkpkills, pkpheadshots, pkpshots, pkphits, pkptime, qbb95kills, qbb95headshots, qbb95shots, qbb95hits, qbb95time, m240bkills, m240bheadshots, m240bshots, m240bhits, m240btime, mg4kills, mg4headshots, mg4shots, mg4hits, mg4time, u100kills, u100headshots, u100shots, u100hits, u100time, l86a2kills, l86a2headshots, l86a2shots, l86a2hits, l86a2time, awskills, awsheadshots, awsshots, awshits, awstime, m60kills, m60headshots, m60shots, m60hits, m60time, rpkkills, rpkheadshots, rpkshots, rpkhits, rpktime, m249kills, m249headshots, m249shots, m249hits, m249time, rpk12kills, rpk12headshots, rpk12shots, rpk12hits, rpk12time, m60ultkills, m60ultheadshots, m60ultshots, m60ulthits, m60ulttime, playerId))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_WEAPONS_PDW_STATS (pp2000_kills, pp2000_headshots, pp2000_shots_fired, pp2000_shots_hit, pp2000_time_equipped, ump45_kills, ump45_headshots, ump45_shots_fired, ump45_shots_hit, ump45_time_equipped, cbjms_kills, cbjms_headshots, cbjms_shots_fired, cbjms_shots_hit, cbjms_time_equipped, pdwr_kills, pdwr_headshots, pdwr_shots_fired, pdwr_shots_hit, pdwr_time_equipped, cz3a1_kills, cz3a1_headshots, cz3a1_shots_fired, cz3a1_shots_hit, cz3a1_time_equipped, js2_kills, js2_headshots, js2_shots_fired, js2_shots_hit, js2_time_equipped, groza4_kills, groza4_headshots, groza4_shots_fired, groza4_shots_hit, groza4_time_equipped, mx4_kills, mx4_headshots, mx4_shots_fired, mx4_shots_hit, mx4_time_equipped, asval_kills, asval_headshots, asval_shots_fired, asval_shots_hit, asval_time_equipped, p90_kills, p90_headshots, p90_shots_fired, p90_shots_hit, p90_time_equipped, mpx_kills, mpx_headshots, mpx_shots_fired, mpx_shots_hit, mpx_time_equipped, ump9_kills, ump9_headshots, ump9_shots_fired, ump9_shots_hit, ump9_time_equipped, mp7_kills, mp7_headshots, mp7_shots_fired, mp7_shots_hit, mp7_time_equipped, sr2_kills, sr2_headshots, sr2_shots_fired, sr2_shots_hit, sr2_time_equipped, player_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""", (pp2000kills, pp2000headshots, pp2000shots, pp2000hits, pp2000time, ump45kills, ump45headshots, ump45shots, ump45hits, ump45time, cbjmskills, cbjmsheadshots, cbjmsshots, cbjmshits, cbjmstime, pdwrkills, pdwrheadshots, pdwrshots, pdwrhits, pdwrtime, cz3a1kills, cz3a1headshots, cz3a1shots, cz3a1hits, cz3a1time, js2kills, js2headshots, js2shots, js2hits, js2time, groza4kills, groza4headshots, groza4shots, groza4hits, groza4time, mx4kills, mx4headshots, mx4shots, mx4hits, mx4time, asvalkills, asvalheadshots, asvalshots, asvalhits, asvaltime, p90kills, p90headshots, p90shots, p90hits, p90time, mpxkills, mpxheadshots, mpxshots, mpxhits, mpxtime, ump9kills, ump9headshots, ump9shots, ump9hits, ump9time, mp7kills, mp7headshots, mp7shots, mp7hits, mp7time, sr2kills, sr2headshots, sr2shots, sr2hits, sr2time, playerId))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_WEAPONS_SHOTGUN_STATS (mcs870_kills, mcs870_headshots, mcs870_shots_fired, mcs870_shots_hit, mcs870_time_equipped, m1014_kills, m1014_headshots, m1014_shots_fired, m1014_shots_hit, m1014_time_equipped, hawk12g_kills, hawk12g_headshots, hawk12g_shots_fired, hawk12g_shots_hit, hawk12g_time_equipped, saiga12k_kills, saiga12k_headshots, saiga12k_shots_fired, saiga12k_shots_hit, saiga12k_time_equipped, spas12_kills, spas12_headshots, spas12_shots_fired, spas12_shots_hit, spas12_time_equipped, uts15_kills, uts15_headshots, uts15_shots_fired, uts15_shots_hit, uts15_time_equipped, dbv12_kills, dbv12_headshots, dbv12_shots_fired, dbv12_shots_hit, dbv12_time_equipped, m26_frag_kills, m26_frag_headshots, m26_frag_shots_fired, m26_frag_shots_hit, m26_frag_time_equipped, m26_slug_kills, m26_slug_headshots, m26_slug_shots_fired, m26_slug_shots_hit, m26_slug_time_equipped, m26_dart_kills, m26_dart_headshots, m26_dart_shots_fired, m26_dart_shots_hit, m26_dart_time_equipped, m26_mass_kills, m26_mass_headshots, m26_mass_shots_fired, m26_mass_shots_hit, m26_mass_time_equipped, qbs09_kills, qbs09_headshots, qbs09_shots_fired, qbs09_shots_hit, qbs09_time_equipped, dao12_kills, dao12_headshots, dao12_shots_fired, dao12_shots_hit, dao12_time_equipped, usas12_kills, usas12_headshots, usas12_shots_fired, usas12_shots_hit, usas12_time_equipped, player_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""", (mcs870kills, mcs870headshots, mcs870shots, mcs870hits, mcs870time, m1014kills, m1014headshots, m1014shots, m1014hits, m1014time, hawk12gkills, hawk12gheadshots, hawk12gshots, hawk12ghits, hawk12gtime, saigakills, saigaheadshots, saigashots, saigahits, saigatime, spas12kills, spas12headshots, spas12shots, spas12hits, spas12time, uts15kills, uts15headshots, uts15shots, uts15hits, uts15time, dbv12kills, dbv12headshots, dbv12shots, dbv12hits, dbv12time, m26fragkills, m26fragheadshots, m26fragshots, m26fraghits, m26fragtime, m26slugkills, m26slugheadshots, m26slugshots, m26slughits, m26slugtime, m26dartkills, m26dartheadshots, m26dartshots, m26darthits, m26darttime, m26masskills, m26massheadshots, m26massshots, m26masshits, m26masstime, qbs09kills, qbs09headshots, qbs09shots, qbs09hits, qbs09time, dao12kills, dao12headshots, dao12shots, dao12hits, dao12time, usas12kills, usas12headshots, usas12shots, usas12hits, usas12time, playerId))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_WEAPONS_SIDEARM_STATS (m9_kills, m9_headshots, m9_shots_fired, m9_shots_hit, m9_time_equipped, qsz92_kills, qsz92_headshots, qsz92_shots_fired, qsz92_shots_hit, qsz92_time_equipped, mp443_kills, mp443_headshots, mp443_shots_fired, mp443_shots_hit, mp443_time_equipped, shorty_kills, shorty_headshots, shorty_shots_fired, shorty_shots_hit, shorty_time_equipped, g18_kills, g18_headshots, g18_shots_fired, g18_shots_hit, g18_time_equipped, fn57_kills, fn57_headshots, fn57_shots_fired, fn57_shots_hit, fn57_time_equipped, m1911_kills, m1911_headshots, m1911_shots_fired, m1911_shots_hit, m1911_time_equipped, r93_kills, r93_headshots, r93_shots_fired, r93_shots_hit, r93_time_equipped, cz75_kills, cz75_headshots, cz75_shots_fired, cz75_shots_hit, cz75_time_equipped, magnum44_kills, magnum44_headshots, magnum44_shots_fired, magnum44_shots_hit, magnum44_time_equipped, compact45_kills, compact45_headshots, compact45_shots_fired, compact45_shots_hit, compact45_time_equipped, p226_kills, p226_headshots, p226_shots_fired, p226_shots_hit, p226_time_equipped, mares_leg_kills, mares_leg_headshots, mares_leg_shots_fired, mares_leg_shots_hit, mares_leg_time_equipped, mp412_kills, mp412_headshots, mp412_shots_fired, mp412_shots_hit, mp412_time_equipped, deagle_kills, deagle_headshots, deagle_shots_fired, deagle_shots_hit, deagle_time_equipped, unica_kills, unica_headshots, unica_shots_fired, unica_shots_hit, unica_time_equipped, sw40_kills, sw40_headshots, sw40_shots_fired, sw40_shots_hit, sw40_time_equipped, player_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""", (m9kills, m9headshots, m9shots, m9hits, m9time, qsz92kills, qsz92headshots, qsz92shots, qsz92hits, qsz92time, mp443kills, mp443headshots, mp443shots, mp443hits, mp443time, shortykills, shortyheadshots, shortyshots, shortyhits, shortytime, g18kills, g18headshots, g18shots, g18hits, g18time, fn57kills, fn57headshots, fn57shots, fn57hits, fn57time, m1911kills, m1911headshots, m1911shots, m1911hits, m1911time, r93kills, r93headshots, r93shots, r93hits, r93time, cz75kills, cz75headshots, cz75shots, cz75hits, cz75time, magnumkills, magnumheadshots, magnumshots, magnumhits, magnumtime, compactkills, compactheadshots, compactshots, compacthits, compacttime, p226kills, p226headshots, p226shots, p226hits, p226time, mareskills, maresheadshots, maresshots, mareshits, marestime, m412kills, m412headshots, m412shots, m412hits, m412time, deaglekills, deagleheadshots, deagleshots, deaglehits, deagletime, unicakills, unicaheadshots, unicashots, unicahits, unicatime, sw40kills, sw40headshots, sw40shots, sw40hits, sw40time, playerId))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_WEAPONS_SNIPER_STATS (m40a5_kills, m40a5_headshots, m40a5_shots_fired, m40a5_shots_hit, m40a5_time_equipped, scout_elite_kills, scout_elite_headshots, scout_elite_shots_fired, scout_elite_shots_hit, scout_elite_time_equipped, sv98_kills, sv98_headshots, sv98_shots_fired, sv98_shots_hit, sv98_time_equipped, jng90_kills, jng90_headshots, jng90_shots_fired, jng90_shots_hit, jng90_time_equipped, recon338_kills, recon338_headshots, recon338_shots_fired, recon338_shots_hit, recon338_time_equipped, m98b_kills, m98b_headshots, m98b_shots_fired, m98b_shots_hit, m98b_time_equipped, srr61_kills, srr61_headshots, srr61_shots_fired, srr61_shots_hit, srr61_time_equipped, cslr4_kills, cslr4_headshots, cslr4_shots_fired, cslr4_shots_hit, cslr4_time_equipped, l115_kills, l115_headshots, l115_shots_fired, l115_shots_hit, l115_time_equipped, gol_magnum_kills, gol_magnum_headshots, gol_magnum_shots_fired, gol_magnum_shots_hit, gol_magnum_time_equipped, fyjs_kills, fyjs_headshots, fyjs_shots_fired, fyjs_shots_hit, fyjs_time_equipped, sr338_kills, sr338_headshots, sr338_shots_fired, sr338_shots_hit, sr338_time_equipped, cs5_kills, cs5_headshots, cs5_shots_fired, cs5_shots_hit, cs5_time_equipped, m82_kills, m82_headshots, m82_shots_fired, m82_shots_hit, m82_time_equipped, amr2_kills, amr2_headshots, amr2_shots_fired, amr2_shots_hit, amr2_time_equipped, player_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""", (m40kills, m40headshots, m40shots, m40hits, m40time, scoutkills, scoutheadshots, scoutshots, scouthits, scouttime, sv98kills, sv98headshots, sv98shots, sv98hits, sv98time, jng90kills, jng90headshots, jng90shots, jng90hits, jng90time, recon338kills, recon338headshots, recon338shots, recon338hits, recon338time, m98bkills, m98bheadshots, m98bshots, m98bhits, m98btime, srr61kills, srr61headshots, srr61shots, srr61hits, srr61time, cslr4kills, cslr4headshots, cslr4shots, cslr4hits, cslr4time, l115kills, l115headshots, l115shots, l115hits, l115time, golkills, golheadshots, golshots, golhits, goltime, fyjskills, fyjsheadshots, fyjsshots, fyjshits, fyjstime, sr338kills, sr338headshots, sr338shots, sr338hits, sr338time, cs5kills, cs5headshots, cs5shots, cs5hits, cs5time, m82kills, m82headshots, m82shots, m82hits, m82time, amr2kills, amr2headshots, amr2shots, amr2hits, amr2time, playerId))
        conn.commit()
  except sqlite3.IntegrityError:
    # record already exists
    print("Record Already Exists")
  finally:
    # close cursor continue on with the code
    cursor.close()

  cursor = conn.cursor()

  """ VEHICLES STATS """

  warsawVehicleStats = 'http://battlelog.battlefield.com/bf4/warsawvehiclesPopulateStats/' + str(playerId)
  if platform == 'pc':
    warsawVehicleStats = warsawVehicleStats + '/1/stats/'
  elif platform == 'ps4':
    warsawVehicleStats = warsawVehicleStats + '/32/stats/'
  elif platform == 'xb1':
    warsawVehicleStats = warsawVehicleStats + '/64/stats/'

  startAPIrequest = time.time()
  vehicleStatsResponse = s.get(warsawVehicleStats)
  endAPIrequest = time.time()
  elapsed_seconds = (endAPIrequest - startAPIrequest)
  print(f"Vehicles API request took {elapsed_seconds:.2f} seconds.")

  vehicleStatsJson = vehicleStatsResponse.json()

  # occasionally these APIs fail but still get 200 success so need to reload the data
  while 'mainVehicleStats' not in vehicleStatsJson['data'] or not vehicleStatsJson['data']['mainVehicleStats']:
    vehicleStatsResponse = s.get(warsawVehicleStats)
    vehicleStatsJson = vehicleStatsResponse.json()

  vehicleStats = vehicleStatsJson['data']['mainVehicleStats']

  # dv15 variables
  dv15kills = 0
  dv15time = 0

  # z11w variables
  z11wkills = 0
  z11wtime = 0

  for x in vehicleStats:
    match x['slug']:
      # AA
      case '9k22-tunguska-m1':
        tunguskakills = x['kills']
        tunguskatime = x['timeIn']
      case 'lav-ad':
        lavadkills = x['kills']
        lavadtime = x['timeIn']
      case 'type-95-aa':
        type95kills = x['kills']
        type95time = x['timeIn']
      # AIR
      case 'ac-130-gunship':
        gunshipkills = x['kills']
        gunshiptime = x['timeIn']
      case 'bomber1':
        bomberkills = x['kills']
        bombertime = x['timeIn']
      # ARTILLERY
      case 'm1421':
        m142kills = x['kills']
        m142time = x['timeIn']
      # ATTACK BOATS
      case 'dv-15':
        dv15kills = dv15kills + x['kills']
        dv15time = dv15time + x['timeIn']
      case 'dv-151':
        dv15kills = dv15kills + x['kills']
        dv15time = dv15time + x['timeIn']
      case 'rcb':
        rcbkills = x['kills']
        rcbtime = x['timeIn']
      # ATTACK HELIS
      case 'mi-28-havoc':
        havockills = x['kills']
        havoctime = x['timeIn']
      case 'z-10w':
        z10wkills = x['kills']
        z10wtime = x['timeIn']
      case 'ah-1z-viper':
        viperkills = x['kills']
        vipertime = x['timeIn']
      # ATTACK JETS
      case 'a10-warthog':
        warthogkills = x['kills']
        warthogtime = x['timeIn']
      case 'su-25tm-frogfoot':
        frogfootkills = x['kills']
        frogfoottime = x['timeIn']
      case 'q-5-fantan':
        fantankills = x['kills']
        fantantime = x['timeIn']
      # EQUIPMENT
      case 'm224-mortar1':
        mortarkills = x['kills']
      case 'aa-mine1':
        aaminekills = x['kills']
      case 'ucav1':
        ucavkills = x['kills']
      case 'eod-bot1':
        eodbotkills = x['kills']
      case 'mav':
        mavkills = x['kills']
      case 'suav':
        suavkills = x['kills']
      case 'rawr':
        rawrkills = x['kills']
      case 'xd-1-accipiter':
        xd1kills = x['kills']
      # IFVS
      case 'btr-90':
        btr90kills = x['kills']
        btr90time = x['timeIn']
      case 'zbd-09':
        zbd09kills = x['kills']
        zbd09time = x['timeIn']
      case 'lav-25':
        lav25kills = x['kills']
        lav25time = x['timeIn']
      # SCOUT HELIS
      case 'z-11w':
        z11wkills = z11wkills + x['kills']
        z11wtime = z11wtime + x['timeIn']
      case 'z-11w2':
        z11wkills = z11wkills + x['kills']
        z11wtime = z11wtime + x['timeIn']
      case 'ah-6j-little-bird1':
        littlebirdkills = x['kills']
        littlebirdtime = x['timeIn']
      # STATIONARY
      case 'pantsir-s1':
        pantsirkills = x['kills']
        pantsirtime = x['timeIn']
      case 'ld-2000-aa':
        ld2000kills = x['kills']
        ld2000time = x['timeIn']
      case '50-cal':
        cal50kills = x['kills']
        cal50time = x['timeIn']
      case 'old-cannon':
        oldcannonkills = x['kills']
        oldcannontime = x['timeIn']
      case 'hj-8-launcher1':
        hj8kills = x['kills']
        hj8time = x['timeIn']
      case '9m133-kornet-launcher':
        kornetkills = x['kills']
        kornettime = x['timeIn']
      case 'm220-tow-launcher':
        towkills = x['kills']
        towtime = x['timeIn']
      case 'schipunov-42':
        schipunovkills = x['kills']
        schipunovtime = x['timeIn']
      # STEALTH JETS
      case 'su-50':
        su50kills = x['kills']
        su50time = x['timeIn']
      case 'j-20':
        j20kills = x['kills']
        j20time = x['timeIn']
      case 'f35':
        f35kills = x['kills']
        f35time = x['timeIn']
      # TANKS
      case 't-90a':
        t90akills = x['kills']
        t90atime = x['timeIn']
      case 'm1-abrams':
        m1abramskills = x['kills']
        m1abramstime = x['timeIn']
      case 'type-99-mbt':
        type99kills = x['kills']
        type99time = x['timeIn']
      case 'ht-95-levkov':
        levkovkills = x['kills']
        levkovtime = x['timeIn']
      # TRANSPORTS
      case 'uh-1y-venom1':
        uh1yvenomkills = x['kills']
        uh1yvenomtime = x['timeIn']
      case 'ka-60-kasatka1':
        ka60kasatkakills = x['kills']
        ka60kasatkatime = x['timeIn']
      case 'z-9-haitun':
        z9haitunkills = x['kills']
        z9haituntime = x['timeIn']
      case 'aav-7a1-amtrac':
        amtrackills = x['kills']
        amtractime = x['timeIn']
      case 'spm-3':
        spm3kills = x['kills']
        spm3time = x['timeIn']
      case 'mrap':
        mrapkills = x['kills']
        mraptime = x['timeIn']
      case 'zfb-05':
        zfb05kills = x['kills']
        zfb05time = x['timeIn']
      case 'm1161-itv':
        m1161itvkills = x['kills']
        m1161itvtime = x['timeIn']
      case 'lyt2021':
        lyt2021kills = x['kills']
        lyt2021time = x['timeIn']
      case 'dpv':
        dpvkills = x['kills']
        dpvtime = x['timeIn']
      case 'vdv-buggy':
        buggykills = x['kills']
        buggytime = x['timeIn']
      case 'pwc':
        pwckills = x['kills']
        pwctime = x['timeIn']
      case 'acv':
        acvkills = x['kills']
        acvtime = x['timeIn']
      case 'rhib-boat':
        rhibkills = x['kills']
        rhibtime = x['timeIn']
      case 'quad-bike':
        quadbikekills = x['kills']
        quadbiketime = x['timeIn']
      case 'dirtbike':
        dirtbikekills = x['kills']
        dirtbiketime = x['timeIn']
      case 'snowmobile':
        snowmobilekills = x['kills']
        snowmobiletime = x['timeIn']
      case 'skid-loader':
        skidloaderkills = x['kills']
        skidloadertime = x['timeIn']
      case 'launch-pod':
        launchpodkills = x['kills']
        launchpodtime = x['timeIn']

  try:
    with conn:
      if update:
        cursor.execute("""UPDATE BF4_VEHICLES_AA_STATS SET tunguska_kills=?, tunguska_time_equipped=?, lavad_kills=?, lavad_time_equipped=?, type95_kills=?, type95_time_equipped=? WHERE player_id=?""", (tunguskakills, tunguskatime, lavadkills, lavadtime, type95kills, type95time, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_VEHICLES_AIR_STATS SET ac130_kills=?, ac130_time_equipped=?, bomber_kills=?, bomber_time_equipped=? WHERE player_id=?""", (gunshipkills, gunshiptime, bomberkills, bombertime, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_VEHICLES_ARTILLERY_STATS SET m142_kills=?, m142_time_equipped=? WHERE player_id=?""", (m142kills, m142time, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_VEHICLES_ATTACK_BOAT_STATS SET dv15_kills=?, dv15_time_equipped=?, rcb_kills=?, rcb_time_equipped=? WHERE player_id=?""", (dv15kills, dv15time, rcbkills, rcbtime, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_VEHICLES_ATTACK_HELI_STATS SET mi28havoc_kills=?, mi28havoc_time_equipped=?, z10w_kills=?, z10w_time_equipped=?, ah1zviper_kills=?, ah1zviper_time_equipped=? WHERE player_id=?""", (havockills, havoctime, z10wkills, z10wtime, viperkills, vipertime, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_VEHICLES_ATTACK_JET_STATS SET a10warthog_kills=?, a10warthog_time_equipped=?, su25frogfoot_kills=?, su25frogfoot_time_equipped=?, q5fantan_kills=?, q5fantan_time_equipped=? WHERE player_id=?""", (warthogkills, warthogtime, frogfootkills, frogfoottime, fantankills, fantantime, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_VEHICLES_EQUIPMENT_STATS SET mortar_kills=?, aa_mine_kills=?, ucav_kills=?, eod_bot_kills=?, mav_kills=?, suav_kills=?, rawr_kills=?, xd1_accipiter_kills=? WHERE player_id=?""", (mortarkills, aaminekills, ucavkills, eodbotkills, mavkills, suavkills, rawrkills, xd1kills, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_VEHICLES_IFV_STATS SET btr90_kills=?, btr90_time_equipped=?, zbd09_kills=?, zbd09_time_equipped=?, lav25_kills=?, lav25_time_equipped=? WHERE player_id=?""", (btr90kills, btr90time, zbd09kills, zbd09time, lav25kills, lav25time, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_VEHICLES_SCOUT_HELI_STATS SET z11w_kills=?, z11w_time_equipped=?, little_bird_kills=?, little_bird_time_equipped=? WHERE player_id=?""", (z11wkills, z11wtime, littlebirdkills, littlebirdtime, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_VEHICLES_STATIONARY_STATS SET pantsir_kills=?, pantsir_time_equipped=?, ld2000_kills=?, ld2000_time_equipped=?, cal50_kills=?, cal50_time_equipped=?, old_cannon_kills=?, old_cannon_time_equipped=?, hj8launcher_kills=?, hj8launcher_time_equipped=?, kornet_launcher_kills=?, kornet_launcher_time_equipped=?, tow_launcher_kills=?, tow_launcher_time_equipped=?, schipunov_kills=?, schipunov_time_equipped=? WHERE player_id=?""", (pantsirkills, pantsirtime, ld2000kills, ld2000time, cal50kills, cal50time, oldcannonkills, oldcannontime, hj8kills, hj8time, kornetkills, kornettime, towkills, towtime, schipunovkills, schipunovtime, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_VEHICLES_STEALTH_JET_STATS SET su50_kills=?, su50_time_equipped=?, j20_kills=?, j20_time_equipped=?, f35_kills=?, f35_time_equipped=? WHERE player_id=?""", (su50kills, su50time, j20kills, j20time, f35kills, f35time, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_VEHICLES_TANK_STATS SET t90a_kills=?, t90a_time_equipped=?, m1abrams_kills=?, m1abrams_time_equipped=?, type99mbt_kills=?, type99mbt_time_equipped=?, levkov95_kills=?, levkov95_time_equipped=? WHERE player_id=?""", (t90akills, t90atime, m1abramskills, m1abramstime, type99kills, type99time, levkovkills, levkovtime, playerId))
        conn.commit()
        cursor.execute("""UPDATE BF4_VEHICLES_TRANSPORT_STATS SET uh1yvenom_kills=?, uh1yvenom_time_equipped=?, ka60kasatka_kills=?, ka60kasatka_time_equipped=?, z9haitun_kills=?, z9haitun_time_equipped=?, amtrac_kills=?, amtrac_time_equipped=?, spm3_kills=?, spm3_time_equipped=?, mrap_kills=?, mrap_time_equipped=?, zfb05_kills=?, zfb05_time_equipped=?, m1161itv_kills=?, m1161itv_time_equipped=?, lyt2021_kills=?, lyt2021_time_equipped=?, dpv_kills=?, dpv_time_equipped=?, buggy_kills=?, buggy_time_equipped=?, pwc_kills=?, pwc_time_equipped=?, acv_kills=?, acv_time_equipped=?, rhib_boat_kills=?, rhib_boat_time_equipped=?, quad_bike_kills=?, quad_bike_time_equipped=?, dirtbike_kills=?, dirtbike_time_equipped=?, snowmobile_kills=?, snowmobile_time_equipped=?, skid_loader_kills=?, skid_loader_time_equipped=?, launch_pod_kills=?, launch_pod_time_equipped=? WHERE player_id=?""", (uh1yvenomkills, uh1yvenomtime, ka60kasatkakills, ka60kasatkatime, z9haitunkills, z9haituntime, amtrackills, amtractime, spm3kills, spm3time, mrapkills, mraptime, zfb05kills, zfb05time, m1161itvkills, m1161itvtime, lyt2021kills, lyt2021time, dpvkills, dpvtime, buggykills, buggytime, pwckills, pwctime, acvkills, acvtime, rhibkills, rhibtime, quadbikekills, quadbiketime, dirtbikekills, dirtbiketime, snowmobilekills, snowmobiletime, skidloaderkills, skidloadertime, launchpodkills, launchpodtime, playerId))
        conn.commit()
      else:
        cursor.execute("""INSERT INTO BF4_VEHICLES_AA_STATS (player_id, tunguska_kills, tunguska_time_equipped, lavad_kills, lavad_time_equipped, type95_kills, type95_time_equipped) VALUES (?, ?, ?, ?, ?, ?, ?);""", (playerId, tunguskakills, tunguskatime, lavadkills, lavadtime, type95kills, type95time))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_VEHICLES_AIR_STATS (player_id, ac130_kills, ac130_time_equipped, bomber_kills, bomber_time_equipped) VALUES (?, ?, ?, ?, ?);""", (playerId, gunshipkills, gunshiptime, bomberkills, bombertime))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_VEHICLES_ARTILLERY_STATS (player_id, m142_kills, m142_time_equipped) VALUES (?, ?, ?);""", (playerId, m142kills, m142time))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_VEHICLES_ATTACK_BOAT_STATS (player_id, dv15_kills, dv15_time_equipped, rcb_kills, rcb_time_equipped) VALUES (?, ?, ?, ?, ?);""", (playerId, dv15kills, dv15time, rcbkills, rcbtime))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_VEHICLES_ATTACK_HELI_STATS (player_id, mi28havoc_kills, mi28havoc_time_equipped, z10w_kills, z10w_time_equipped, ah1zviper_kills, ah1zviper_time_equipped) VALUES (?, ?, ?, ?, ?, ?, ?);""", (playerId, havockills, havoctime, z10wkills, z10wtime, viperkills, vipertime))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_VEHICLES_ATTACK_JET_STATS (player_id, a10warthog_kills, a10warthog_time_equipped, su25frogfoot_kills, su25frogfoot_time_equipped, q5fantan_kills, q5fantan_time_equipped) VALUES (?, ?, ?, ?, ?, ?, ?);""", (playerId, warthogkills, warthogtime, frogfootkills, frogfoottime, fantankills, fantantime))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_VEHICLES_EQUIPMENT_STATS (player_id, mortar_kills, aa_mine_kills, ucav_kills, eod_bot_kills, mav_kills, suav_kills, rawr_kills, xd1_accipiter_kills) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);""", (playerId, mortarkills, aaminekills, ucavkills, eodbotkills, mavkills, suavkills, rawrkills, xd1kills))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_VEHICLES_IFV_STATS (player_id, btr90_kills, btr90_time_equipped, zbd09_kills, zbd09_time_equipped, lav25_kills, lav25_time_equipped) VALUES (?, ?, ?, ?, ?, ?, ?);""", (playerId, btr90kills, btr90time, zbd09kills, zbd09time, lav25kills, lav25time))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_VEHICLES_SCOUT_HELI_STATS (player_id, z11w_kills, z11w_time_equipped, little_bird_kills, little_bird_time_equipped) VALUES (?, ?, ?, ?, ?);""", (playerId, z11wkills, z11wtime, littlebirdkills, littlebirdtime))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_VEHICLES_STATIONARY_STATS (player_id, pantsir_kills, pantsir_time_equipped, ld2000_kills, ld2000_time_equipped, cal50_kills, cal50_time_equipped, old_cannon_kills, old_cannon_time_equipped, hj8launcher_kills, hj8launcher_time_equipped, kornet_launcher_kills, kornet_launcher_time_equipped, tow_launcher_kills, tow_launcher_time_equipped, schipunov_kills, schipunov_time_equipped) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""", (playerId, pantsirkills, pantsirtime, ld2000kills, ld2000time, cal50kills, cal50time, oldcannonkills, oldcannontime, hj8kills, hj8time, kornetkills, kornettime, towkills, towtime, schipunovkills, schipunovtime))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_VEHICLES_STEALTH_JET_STATS (player_id, su50_kills, su50_time_equipped, j20_kills, j20_time_equipped, f35_kills, f35_time_equipped) VALUES (?, ?, ?, ?, ?, ?, ?);""", (playerId, su50kills, su50time, j20kills, j20time, f35kills, f35time))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_VEHICLES_TANK_STATS (player_id, t90a_kills, t90a_time_equipped, m1abrams_kills, m1abrams_time_equipped, type99mbt_kills, type99mbt_time_equipped, levkov95_kills, levkov95_time_equipped) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);""", (playerId, t90akills, t90atime, m1abramskills, m1abramstime, type99kills, type99time, levkovkills, levkovtime))
        conn.commit()
        cursor.execute("""INSERT INTO BF4_VEHICLES_TRANSPORT_STATS (player_id, uh1yvenom_kills, uh1yvenom_time_equipped, ka60kasatka_kills, ka60kasatka_time_equipped, z9haitun_kills, z9haitun_time_equipped, amtrac_kills, amtrac_time_equipped, spm3_kills, spm3_time_equipped, mrap_kills, mrap_time_equipped, zfb05_kills, zfb05_time_equipped, m1161itv_kills, m1161itv_time_equipped, lyt2021_kills, lyt2021_time_equipped, dpv_kills, dpv_time_equipped, buggy_kills, buggy_time_equipped, pwc_kills, pwc_time_equipped, acv_kills, acv_time_equipped, rhib_boat_kills, rhib_boat_time_equipped, quad_bike_kills, quad_bike_time_equipped, dirtbike_kills, dirtbike_time_equipped, snowmobile_kills, snowmobile_time_equipped, skid_loader_kills, skid_loader_time_equipped, launch_pod_kills, launch_pod_time_equipped) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""", (playerId, uh1yvenomkills, uh1yvenomtime, ka60kasatkakills, ka60kasatkatime, z9haitunkills, z9haituntime, amtrackills, amtractime, spm3kills, spm3time, mrapkills, mraptime, zfb05kills, zfb05time, m1161itvkills, m1161itvtime, lyt2021kills, lyt2021time, dpvkills, dpvtime, buggykills, buggytime, pwckills, pwctime, acvkills, acvtime, rhibkills, rhibtime, quadbikekills, quadbiketime, dirtbikekills, dirtbiketime, snowmobilekills, snowmobiletime, skidloaderkills, skidloadertime, launchpodkills, launchpodtime))
        conn.commit()
  except sqlite3.IntegrityError:
    # record already exists
    print("Record Already Exists")
  finally:
    # close cursor continue on with the code
    cursor.close()

  cursor = conn.cursor()

  """ RIBBON STATS """

  warsawRibbonStats = 'http://battlelog.battlefield.com/bf4/warsawawardspopulate/' + str(playerId)
  if platform == 'pc':
    warsawRibbonStats = warsawRibbonStats + '/1/'
  elif platform == 'ps4':
    warsawRibbonStats = warsawRibbonStats + '/32/'
  elif platform == 'xb1':
    warsawRibbonStats = warsawRibbonStats + '/64/'

  startAPIrequest = time.time()
  ribbonStatsResponse = s.get(warsawRibbonStats)
  endAPIrequest = time.time()
  elapsed_seconds = (endAPIrequest - startAPIrequest)
  print(f"Ribbons API request took {elapsed_seconds:.2f} seconds.")

  ribbonStatsJson = ribbonStatsResponse.json()

  # occasionally these APIs fail but still get 200 success so need to reload the data
  while 'ribbonAwardByCode' not in ribbonStatsJson['data']:
    ribbonStatsResponse = s.get(warsawRibbonStats)
    ribbonStatsJson = ribbonStatsResponse.json()

  ribbonStats = ribbonStatsJson['data']['ribbonAwardByCode']

  killAssistRibbons = int(ribbonStats['r01']['timesTaken'])
  antiVehicleRibbons = int(ribbonStats['r02']['timesTaken'])
  squadWipeRibbons = int(ribbonStats['r03']['timesTaken'])
  headshotRibbons = int(ribbonStats['r04']['timesTaken'])
  avengerRibbons = int(ribbonStats['r05']['timesTaken'])
  saviorRibbons = int(ribbonStats['r06']['timesTaken'])
  spottingRibbons = int(ribbonStats['r07']['timesTaken'])
  aceSquadRibbons = int(ribbonStats['r08']['timesTaken'])
  mvpRibbons = int(ribbonStats['r09']['timesTaken'])
  handgunRibbons = int(ribbonStats['r10']['timesTaken'])
  assaultRifleRibbons = int(ribbonStats['r11']['timesTaken'])
  carbineRibbons = int(ribbonStats['r12']['timesTaken'])
  sniperRifleRibbons = int(ribbonStats['r13']['timesTaken'])
  lmgRibbons = int(ribbonStats['r14']['timesTaken'])
  dmrRibbons = int(ribbonStats['r15']['timesTaken'])
  pdwRibbons = int(ribbonStats['r16']['timesTaken'])
  shotgunRibbons = int(ribbonStats['r17']['timesTaken'])
  meleeRibbons = int(ribbonStats['r18']['timesTaken'])
  ifvRibbons = int(ribbonStats['r19']['timesTaken'])
  tankRibbons = int(ribbonStats['r20']['timesTaken'])
  aaRibbons = int(ribbonStats['r21']['timesTaken'])
  scoutHeliRibbons = int(ribbonStats['r22']['timesTaken'])
  attackHeliRibbons = int(ribbonStats['r23']['timesTaken'])
  jetRibbons = int(ribbonStats['r24']['timesTaken'])
  attackBoatRibbons = int(ribbonStats['r25']['timesTaken'])
  flagCaptureRibbons = int(ribbonStats['r26']['timesTaken'])
  mcomAttackerRibbons = int(ribbonStats['r27']['timesTaken'])
  bombDeliveryRibbons = int(ribbonStats['r28']['timesTaken'])
  conquestRibbons = int(ribbonStats['r29']['timesTaken'])
  rushRibbons = int(ribbonStats['r30']['timesTaken'])
  tdmRibbons = int(ribbonStats['r31']['timesTaken'])
  squadDmRibbons = int(ribbonStats['r32']['timesTaken'])
  obliterationRibbons = int(ribbonStats['r33']['timesTaken'])
  defuseRibbons = int(ribbonStats['r34']['timesTaken'])
  dominationRibbons = int(ribbonStats['r35']['timesTaken'])
  medkitRibbons = int(ribbonStats['r36']['timesTaken'])
  defibRibbons = int(ribbonStats['r37']['timesTaken'])
  repairToolRibbons = int(ribbonStats['r38']['timesTaken'])
  marksmanRibbons = int(ribbonStats['r39']['timesTaken'])
  spawnBeaconRibbons = int(ribbonStats['r40']['timesTaken'])
  ammoRibbons = int(ribbonStats['r41']['timesTaken'])
  commanderSurveillanceRibbons = int(ribbonStats['r42']['timesTaken'])
  commanderResupplyRibbons = int(ribbonStats['r43']['timesTaken'])
  commanderLeadershipRibbons = int(ribbonStats['r44']['timesTaken'])
  commanderGunshipRibbons = int(ribbonStats['r45']['timesTaken'])
  gunMasterRibbons = int(ribbonStats['rGMW']['timesTaken'])
  ctfRibbons = int(ribbonStats['xp0rFD']['timesTaken'])
  captureSpecialistRibbons = int(ribbonStats['xp0rCS']['timesTaken'])
  airSuperiorityRibbons = int(ribbonStats['xp1rAS']['timesTaken'])
  bomberDeliveryRibbons = int(ribbonStats['xp1rBD']['timesTaken'])
  carrierAssaultRibbons = int(ribbonStats['xp2rCA']['timesTaken'])
  chainLinkRibbons = int(ribbonStats['xp3rCW']['timesTaken'])
  linkBreakerRibbons = int(ribbonStats['xp3rLB']['timesTaken'])
  linkMakerRibbons = int(ribbonStats['xp3rLM']['timesTaken'])

  try:
    with conn:
      if update:
        cursor.execute("""UPDATE BF4_RIBBONS SET ANTI_VEHICLE_RIBBON=?, MEDKIT_RIBBON=?, DEFIBRILLATOR_RIBBON=?, REPAIR_TOOL_RIBBON=?, MARKSMAN_RIBBON=?, RADIO_BEACON_SPAWN_RIBBON=?, AMMO_RIBBON=?, COMMANDER_RESUPPLY_RIBBON=?, COMMANDER_LEADERSHIP_RIBBON=?, COMMANDER_GUNSHIP_RIBBON=?, CONQUEST_FLAG_CAPTURE_RIBBON=?, MCOM_ATTACKER_RIBBON=?, BOMB_DELIVERY_RIBBON=?, CONQUEST_RIBBON=?, RUSH_RIBBON=?, TEAM_DEATHMATCH_RIBBON=?, SQUAD_DEATHMATCH_RIBBON=?, OBLITERATION_RIBBON=?, DEFUSE_RIBBON=?, DOMINATION_RIBBON=?, GUN_MASTER_RIBBON=?, CAPTURE_THE_FLAG_RIBBON=?, CAPTURE_SPECIALIST_RIBBON=?, AIR_SUPERIORITY_RIBBON=?, CARRIER_ASSAULT_RIBBON=?, CHAINLINK_RIBBON=?, LINK_BREAKER_RIBBON=?, LINK_MAKER_RIBBON=?, HEADSHOT_RIBBON=?, HANDGUN_RIBBON=?, ASSAULT_RIFLE_RIBBON=?, CARBINE_RIBBON=?, SNIPER_RIFLE_RIBBON=?, LMG_RIBBON=?, DMR_RIBBON=?, PDW_RIBBON=?, SHOTGUN_RIBBON=?, MELEE_RIBBON=?, IFV_RIBBON=?, TANK_RIBBON=?, AA_RIBBON=?, SCOUT_HELI_RIBBON=?, ATTACK_HELI_RIBBON=?, JET_RIBBON=?, BOAT_RIBBON=?, BOMBER_RIBBON=?, KILL_ASSIST_RIBBON=?, SQUAD_WIPE_RIBBON=?, AVENGER_RIBBON=?, SAVIOR_RIBBON=?, SPOTTING_RIBBON=?, ACE_SQUAD_RIBBON=?, MVP_RIBBON=?, COMMANDER_SURVEILLANCE_RIBBON=? WHERE player_id=?""", (antiVehicleRibbons, medkitRibbons, defibRibbons, repairToolRibbons, marksmanRibbons, spawnBeaconRibbons, ammoRibbons, commanderResupplyRibbons, commanderLeadershipRibbons, commanderGunshipRibbons, flagCaptureRibbons, mcomAttackerRibbons, bombDeliveryRibbons, conquestRibbons, rushRibbons, tdmRibbons, squadDmRibbons, obliterationRibbons, defuseRibbons, dominationRibbons, gunMasterRibbons, ctfRibbons, captureSpecialistRibbons, airSuperiorityRibbons, carrierAssaultRibbons, chainLinkRibbons, linkBreakerRibbons, linkMakerRibbons, headshotRibbons, handgunRibbons, assaultRifleRibbons, carbineRibbons, sniperRifleRibbons, lmgRibbons, dmrRibbons, pdwRibbons, shotgunRibbons, meleeRibbons, ifvRibbons, tankRibbons, aaRibbons, scoutHeliRibbons, attackHeliRibbons, jetRibbons, attackBoatRibbons, bomberDeliveryRibbons, killAssistRibbons, squadWipeRibbons, avengerRibbons, saviorRibbons, spottingRibbons, aceSquadRibbons, mvpRibbons, commanderSurveillanceRibbons, playerId))
        conn.commit()
      else:
        cursor.execute("""INSERT INTO BF4_RIBBONS (player_id, ANTI_VEHICLE_RIBBON, MEDKIT_RIBBON, DEFIBRILLATOR_RIBBON, REPAIR_TOOL_RIBBON, MARKSMAN_RIBBON, RADIO_BEACON_SPAWN_RIBBON, AMMO_RIBBON, COMMANDER_RESUPPLY_RIBBON, COMMANDER_LEADERSHIP_RIBBON, COMMANDER_GUNSHIP_RIBBON, CONQUEST_FLAG_CAPTURE_RIBBON, MCOM_ATTACKER_RIBBON, BOMB_DELIVERY_RIBBON, CONQUEST_RIBBON, RUSH_RIBBON, TEAM_DEATHMATCH_RIBBON, SQUAD_DEATHMATCH_RIBBON, OBLITERATION_RIBBON, DEFUSE_RIBBON, DOMINATION_RIBBON, GUN_MASTER_RIBBON, CAPTURE_THE_FLAG_RIBBON, CAPTURE_SPECIALIST_RIBBON, AIR_SUPERIORITY_RIBBON, CARRIER_ASSAULT_RIBBON, CHAINLINK_RIBBON, LINK_BREAKER_RIBBON, LINK_MAKER_RIBBON, HEADSHOT_RIBBON, HANDGUN_RIBBON, ASSAULT_RIFLE_RIBBON, CARBINE_RIBBON, SNIPER_RIFLE_RIBBON, LMG_RIBBON, DMR_RIBBON, PDW_RIBBON, SHOTGUN_RIBBON, MELEE_RIBBON, IFV_RIBBON, TANK_RIBBON, AA_RIBBON, SCOUT_HELI_RIBBON, ATTACK_HELI_RIBBON, JET_RIBBON, BOAT_RIBBON, BOMBER_RIBBON, KILL_ASSIST_RIBBON, SQUAD_WIPE_RIBBON, AVENGER_RIBBON, SAVIOR_RIBBON, SPOTTING_RIBBON, ACE_SQUAD_RIBBON, MVP_RIBBON, COMMANDER_SURVEILLANCE_RIBBON) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""", (playerId, antiVehicleRibbons, medkitRibbons, defibRibbons, repairToolRibbons, marksmanRibbons, spawnBeaconRibbons, ammoRibbons, commanderResupplyRibbons, commanderLeadershipRibbons, commanderGunshipRibbons, flagCaptureRibbons, mcomAttackerRibbons, bombDeliveryRibbons, conquestRibbons, rushRibbons, tdmRibbons, squadDmRibbons, obliterationRibbons, defuseRibbons, dominationRibbons, gunMasterRibbons, ctfRibbons, captureSpecialistRibbons, airSuperiorityRibbons, carrierAssaultRibbons, chainLinkRibbons, linkBreakerRibbons, linkMakerRibbons, headshotRibbons, handgunRibbons, assaultRifleRibbons, carbineRibbons, sniperRifleRibbons, lmgRibbons, dmrRibbons, pdwRibbons, shotgunRibbons, meleeRibbons, ifvRibbons, tankRibbons, aaRibbons, scoutHeliRibbons, attackHeliRibbons, jetRibbons, attackBoatRibbons, bomberDeliveryRibbons, killAssistRibbons, squadWipeRibbons, avengerRibbons, saviorRibbons, spottingRibbons, aceSquadRibbons, mvpRibbons, commanderSurveillanceRibbons))
        conn.commit()
  except sqlite3.IntegrityError:
    # record already exists
    print("Record Already Exists")
  finally:
    # close cursor continue on with the code
    cursor.close()

  """ TIME PLAYED ADJUSTMENTS """

  # if player has reached the time limit then we'll try getting their time by adding up all "time_equipped" values
  if secondsPlayed >= 33554400:
    newTime = m40time+scouttime+sv98time+jng90time+recon338time+m98btime+srr61time+cslr4time+l115time+goltime+fyjstime+sr338time+cs5time+m82time+amr2time+scarhtime+m416time+sar21time+aek971time+famastime+auga3time+m16a4time+cz805time+ak12time+an94time+f2000time+ace23time+qbz95time+bulldogtime+ar160time+l85a2time+acwrtime+sg553time+aku12time+a91time+ace52time+g36ctime+m4time+ace21time+type95btime+groza1time+ak5ctime+mtar21time+phantomtime+mk11time+skstime+svd12time+qbu88time+m39time+ace53time+scarhsvtime+rfbtime+xm25airtime+xm25darttime+xm25smoketime+c4time+m15minetime+m2slamtime+claymoretime+repairtooltime+defibtime+lawtime+stingertime+rpgtime+iglatime+smawtime+javelintime+srawtime+hvmtime+m136time+m320hetime+m320lvgtime+m320smktime+m320darttime+m320fbtime+m3203gltime+shieldtime+railguntime+m32time+v40minitime+rgotime+m34time+m18smoketime+m84time+flaretime+m67time+type88time+lsattime+pkptime+qbb95time+m240btime+mg4time+u100time+l86a2time+awstime+m60time+rpktime+m249time+rpk12time+m60ulttime+pp2000time+ump45time+cbjmstime+pdwrtime+cz3a1time+js2time+groza4time+mx4time+asvaltime+p90time+mpxtime+ump9time+mp7time+sr2time+mcs870time+m1014time+hawk12gtime+saigatime+spas12time+uts15time+dbv12time+qbs09time+dao12time+usas12time+m26masstime+m26darttime+m26slugtime+m26fragtime+m9time+qsz92time+mp443time+shortytime+g18time+fn57time+m1911time+r93time+cz75time+magnumtime+compacttime+p226time+marestime+m412time+deagletime+unicatime+sw40time+tunguskatime+lavadtime+type95time+gunshiptime+bombertime+m142time+dv15time+rcbtime+havoctime+z10wtime+vipertime+warthogtime+frogfoottime+fantantime+btr90time+zbd09time+lav25time+z11wtime+littlebirdtime+pantsirtime+ld2000time+cal50time+oldcannontime+hj8time+kornettime+towtime+schipunovtime+su50time+j20time+f35time+t90atime+m1abramstime+type99time+levkovtime+uh1yvenomtime+ka60kasatkatime+z9haituntime+amtractime+spm3time+mraptime+zfb05time+m1161itvtime+lyt2021time+dpvtime+buggytime+pwctime+acvtime+rhibtime+quadbiketime+dirtbiketime+snowmobiletime+skidloadertime+launchpodtime
    if newTime > secondsPlayed:
      cursor = conn.cursor()
      cursor.execute("""UPDATE BF4_PLAYERS SET adjusted_playtime=? WHERE player_id=?""", (newTime, playerId))
      conn.commit()
      cursor.close()

  end = time.time()
  elapsed_seconds = (end - start)
  print(f"Function took {elapsed_seconds:.2f} seconds.\n\n")

def threaded_process(data):
  try:
    conn = sqlite3.connect('C:/Program Files/DB Browser for SQLite/Battlefield Database.db', timeout=300)
    s = requests.Session()
    retries = Retry(total=10, backoff_factor=0.1, status_forcelist=[400, 403, 404, 408, 422, 429, 500, 501, 502, 503, 504])
    s.mount('http://', HTTPAdapter(max_retries=retries))

    # iterate through json
    for x in data:
      for key, value in x.items():
        if key == '_id':
          warsawOverview = 'http://battlelog.battlefield.com/bf4/warsawoverviewpopulate/' + str(value) + '/1/'
          
          startAPIrequest = time.time()
          overviewResponse = s.get(warsawOverview)
          endAPIrequest = time.time()
          elapsed_seconds = (endAPIrequest - startAPIrequest)
          print(f"Username API request took {elapsed_seconds:.2f} seconds.\n")

          overviewResponseJson = overviewResponse.json()
          overview = overviewResponseJson['data']
          if "currentUserId" in overview:
            userId = overview['currentUserId']
            get_stats(value, userId, s, conn, 'pc')

          '''
          # call API to see if playerId exists as PC player
          payload = {'format_values': 'true', 'playerid': value, 'platform': 'pc'}
          headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 Edg/137.0.0.0", "Accept-Encoding": "*", "Connection": "keep-alive"}
          bf4api = requests.get('https://api.gametools.network/bf4/all', params=payload, headers=headers)
          # if the player exists it will return a 200 status and we'll add/update their database tables
          if bf4api.status_code == requests.codes.ok:
            response = bf4api.json()
            get_stats(response, conn, 'pc')
          else:
            # call API to see if playerId exists as PS4 player
            payload = {'format_values': 'true', 'playerid': value, 'platform': 'ps4'}
            bf4api = requests.get('https://api.gametools.network/bf4/all', params=payload)
            # if the player exists it will return a 200 status and we'll add/update their database tables
            if bf4api.status_code == requests.codes.ok:
              response = bf4api.json()
              get_stats(response, conn, 'ps4')
            else:
              # call API to see if playerId exists as XBOXONE player
              payload = {'format_values': 'true', 'playerid': value, 'platform': 'xboxone'}
              bf4api = requests.get('https://api.gametools.network/bf4/all', params=payload)
              # if the player exists it will return a 200 status and we'll add/update their database tables
              if bf4api.status_code == requests.codes.ok:
                response = bf4api.json()
                get_stats(response, conn, 'xb1')
          '''
      time.sleep(5) # to avoid API rate limit
    
    s.close()

  except sqlite3.Error as error:
    print('Error occured - ', error)

  finally:
    if conn:
      conn.close()
      print('Connection closed')

# get json with all the playerIds
json_file = 'C:/Users/bige3/OneDrive/Documents/playerIds_2.json'
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
