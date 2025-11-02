#!/usr/bin/python3

#Imports
import sys
import os
import subprocess
import logging
import sqlite3
import gc
import time

# Speicherbedingungen
FREE_CACHE_SPACE_MIN = 500*1024*1024*1024     # 500GiB
CACHE_FILE_SIZE_MAX = 5*1024*1024*1024        # 5Gib

PID_FILE="/var/run/mover.pid"

DB_PATH = "/tmp/array_access_monitor/array_access.db"

# Liste an Teilpfaden, die auf dem Cache gehalten werden sollen
CACHE_PREF_LIST = [
    "/Share1/Eric/Savegames/"
]

# Logger konfigurieren
logging.basicConfig(level=logging.INFO, format='mover_custom: [%(levelname)s] %(message)s')
logger = logging.getLogger("mover_custom")

terminate = False


# Startfunktion
def start(test_run=False):
    
    # Prüfe ob Array gestartet
    if not os.path.exists("/mnt/user0"):
        logger.info("Array nicht gestartet")
        exit(3)

    # Prüfe ob DB existiert
    if not os.path.exists(DB_PATH):
        logger.info(f"{DB_PATH} nicht gefunden")
        exit(3)

    # Prüfe ob mover bereits läuft
    if os.path.isfile(PID_FILE):
        pid = open(PID_FILE, "r").read().splitlines()[0]
        if os.path.exists(f"/proc/{pid}"):
            logger.info(f"Prozess läuft bereits mit PID: {pid}")
            exit(1)
    
    # Verschieben starten
    pid=os.getpid()
    logger.info(f"Prozess gestartet mit PID: {str(pid)}")
    with open(PID_FILE, 'w') as file:
        file.write(str(pid))

    if test_run:
        logger.info("TESTRUN")

    # Bestimme die Anzahl der disks (disk1...n)
    result = subprocess.run(['find', '/mnt/', '-maxdepth', '1', '-type', 'd', '-name', 'disk[0-9]*'], stdout=subprocess.PIPE, text=True)
    disk_count = len(result.stdout.strip().split('\n'))

    # Hole den freien Speicherplatz im Cache-Pool
    result = subprocess.run(['zfs', 'list', '-Hp', '-o', 'avail', 'ssd-pool'], stdout=subprocess.PIPE, text=True)
    free_cache_space = int(result.stdout.strip())
    
    # Hole die Zugriffsinfos aus der Datenbank
    #conn = sqlite3.connect(DB_PATH)
    #c = conn.cursor()
    #c.execute("SELECT path, access_count, last_accessed FROM files ORDER BY access_count DESC")
    #result = c.fetchall()
    #conn.close()
    #db_list = [row[0] for row in result]

    #del result
    #gc.collect()

    # Hole die Zugriffsinfos aus der Datenbank
    conn = sqlite3.connect(DB_PATH)  # Verbindung zur Datenbank herstellen
    c = conn.cursor()

    # 1. Abfrage: Diese Zeilen ziehen die Dateien mit den Teilpfaden
    like_clauses = ' OR '.join(['path LIKE ?' for _ in CACHE_PREF_LIST])
    query_pref_list = f"SELECT path, access_count, last_accessed FROM files WHERE {like_clauses} ORDER BY access_count DESC"

    # Muster für die LIKE-Abfragen erstellen
    like_patterns = [f'%{teilpfad}%' for teilpfad in CACHE_PREF_LIST]
    c.execute(query_pref_list, like_patterns)
    teilpfad_result = c.fetchall()

    # 2. Abfrage: Diese Zeilen ziehen die restlichen Dateien
    not_like_clauses = ' AND '.join(['path NOT LIKE ?' for _ in CACHE_PREF_LIST])
    query_alle = f"SELECT path, access_count, last_accessed FROM files WHERE {not_like_clauses} ORDER BY access_count DESC"

    # Muster für die NOT LIKE-Abfragen erstellen
    not_like_patterns = [f'%{teilpfad}%' for teilpfad in CACHE_PREF_LIST]
    c.execute(query_alle, not_like_patterns)
    restliche_result = c.fetchall()

    conn.close()  # Verbindung schließen

    # Kombinieren der Ergebnisse
    gesamt_result = teilpfad_result + restliche_result

    # Umwandeln in eine Liste der Pfade
    db_list = [row[0] for row in gesamt_result]

    # Aufräumen
    del teilpfad_result, restliche_result
    gc.collect()
    

    # Hole Cache-Dateiliste
    result = subprocess.run(['find', '/mnt/ssd-pool/Share1/', '-type', 'f', '-printf', '%s;;%p\n'], stdout=subprocess.PIPE, text=True)
    cache_files_dict = {}
    for line in result.stdout.splitlines():
        size, path = line.split(';;')
        cache_files_dict[path] = int(size)

    del result
    gc.collect()

    # Hole Array-Dateiliste
    find_params = [f"/mnt/disk{d+1}/Share1/" for d in range(0, disk_count)]
    result = subprocess.run(['find'] + find_params + ['-type', 'f', '-printf', '%s;;%p\n'], stdout=subprocess.PIPE, text=True)
    array_files_dict = {}
    for line in result.stdout.splitlines():
        size, path = line.split(';;')
        array_files_dict[path] = int(size)

    del result
    gc.collect()

    # Durchlaufe die DB_Liste von oben und unten und verschiebe
    i = 0                       # obere DB-Eintrag
    k = len(db_list) - 1        # untere DB-Eintrag
    global terminate 
    while (k != i and os.path.isfile(PID_FILE)):
        
        # Skippe, wenn DB-Eintrag i nicht von mover betroffen (z.B. Backup)
        if not db_list[i].startswith("/mnt/user/Share1/"): 
            i=i+1
            continue

        # Finde heraus, wo der DB-Eintrag i liegt
        db_entry_i_path = None
        db_entry_i_size = None
        if db_list[i].replace("/mnt/user/","/mnt/ssd-pool/", 1) in cache_files_dict:
            db_entry_i_path = db_list[i].replace("/mnt/user/","/mnt/ssd-pool/", 1)
            db_entry_i_size = cache_files_dict[db_entry_i_path]
        else:
            for l in range(0, disk_count):
                if db_list[i].replace("/mnt/user/",f"/mnt/disk{l+1}/", 1) in array_files_dict:
                    db_entry_i_path = db_list[i].replace("/mnt/user/",f"/mnt/disk{l+1}/", 1)
                    db_entry_i_size = array_files_dict[db_entry_i_path]
                    break

        # Falls der DB_Eintrag i nicht auf Cache oder Array gefunden, lösche in Datenbank
        if not db_entry_i_path:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("DELETE FROM files WHERE path = ?", (db_list[i],))
            conn.commit()
            conn.close()
            i=i+1
            continue
        
        # Falls Größe von DB-Eintrag i mehr als gewünscht oder DB-Eintrag i ist Videodatei und liegt in Filme, skippe
        if db_entry_i_size > CACHE_FILE_SIZE_MAX or (db_entry_i_path.lower().endswith((".mkv", ".mp4", ".avi", ".mov", ".flv")) and ("/Share1/Multimedia/Filme/" in db_entry_i_path or "/Share1/Multimedia/Serien/" in db_entry_i_path)):
            i=i+1
            continue

        # Prüfe ob der DB-Eintrag i auf Cache liegt
        if db_entry_i_path.startswith("/mnt/ssd-pool/"):
            # Lösche aus Cache-Liste und fahre mit dem nächsten Eintrag fort
            cache_files_dict.pop(db_entry_i_path, None)
            i=i+1
        
        # Prüfe ob der DB-Eintrag i auf dem Array liegt
        elif db_entry_i_path.startswith("/mnt/disk"):
            
            # Prüfe ob genügend Speicher frei
            if (free_cache_space - db_entry_i_size) > FREE_CACHE_SPACE_MIN: 
                # Verschiebe DB-Eintrag i auf Cache
                if not test_run:
                    subprocess.run(["/usr/libexec/unraid/move"], input=db_entry_i_path, text=True)
                free_cache_space = free_cache_space - db_entry_i_size
                i=i+1
                logger.debug(f"Verschiebe: {db_entry_i_path}")

            # Sonst: Prüfe DB-Eintrag k  
            else:
                
                # Skippe, wenn DB-Eintrag k nicht von mover betroffen (z.B. Backup)
                if not db_list[k].startswith("/mnt/user/Share1/"): 
                    k=k-1
                    continue
                
                # Finde heraus, wo der DB-Eintrag k liegt
                db_entry_k_path = None
                db_entry_k_size = None
                if db_list[k].replace("/mnt/user/","/mnt/ssd-pool/", 1) in cache_files_dict:
                    db_entry_k_path = db_list[k].replace("/mnt/user/","/mnt/ssd-pool/", 1)
                    db_entry_k_size = cache_files_dict[db_entry_k_path]
                else:
                    for l in range(0, disk_count):
                        if db_list[k].replace("/mnt/user/",f"/mnt/disk{l+1}/", 1) in array_files_dict:
                            db_entry_k_path = db_list[k].replace("/mnt/user/",f"/mnt/disk{l+1}/", 1)
                            db_entry_k_size = array_files_dict[db_entry_k_path]
                            break

                # Falls der DB_Eintrag k nicht auf Cache oder Array gefunden, lösche in Datenbank
                if not db_entry_k_path:
                    
                    conn = sqlite3.connect(DB_PATH)
                    c = conn.cursor()
                    c.execute("DELETE FROM files WHERE path = ?", (db_list[k],))
                    conn.commit()
                    conn.close()

                    k=k-1
                    continue

                # Prüfe ob der DB-Eintrag k auf Array liegt
                if db_entry_k_path.startswith("/mnt/disk"):
                    # Fahre mit dem nächsten Eintrag fort
                    k=k-1

                # Prüfe ob der DB-Eintrag k auf dem Cache liegt
                elif db_entry_k_path.startswith("/mnt/ssd-pool/"):
                    
                    # Verschiebe DB-Eintrag k auf Array
                    if not test_run:
                        subprocess.run(["/usr/libexec/unraid/move"], input=db_entry_k_path, text=True)
                    free_cache_space = free_cache_space + db_entry_k_size
                    cache_files_dict.pop(db_entry_k_path, None)
                    k=k-1
                    logger.debug(f"Verschiebe: {db_entry_k_path}")
        
    # Verschiebe übrig gebliebene Cache-Liste (Dateien, welche durch die DB nicht erfasst wurden und somit auf das Array gehören)
    for cache_file_path, cache_file_size in cache_files_dict.items():
        if os.path.isfile(PID_FILE):
            
            # Prüfe ob die zu verschiebende Datei auf dem Cache gehalten werden soll
            stay_in_cache = False
            for sub_path in CACHE_PREF_LIST:
                if sub_path in cache_file_path:
                    stay_in_cache = True

            if not stay_in_cache:
                logger.debug(f"Verschiebe: {cache_file_path}")
                if not test_run:
                    subprocess.run(["/usr/libexec/unraid/move"], input=cache_file_path, text=True)

    # Lösche PID Datei wieder
    if os.path.isfile(PID_FILE): 
        subprocess.run(["rm", "-f", str(PID_FILE)])
    logger.info(f"Mover beendet")
    
def stop():
    #Prüfe ob mover bereits läuft
    if not os.path.isfile(PID_FILE):
        logger.info("Läuft nicht")            
        exit(0)
    
    logger.info(f"Lösche {str(PID_FILE)}")
    subprocess.run(["rm", "-f", str(PID_FILE)])   

def status():
    #Falls PID-Datei nicht mehr vorhanden --> Gestoppt
    if not os.path.isfile(PID_FILE):
        logger.info(f"Gestoppt ({str(PID_FILE)} nicht vorhanden)")
        exit(1)
    #Falls vorhanden --> Läuft
    else:
        logger.info(f"Läuft mit PID: {open(PID_FILE, 'r').read().splitlines()[0]}")
        exit(0)

#Main Funktion: Entscheide nach mitgegebenen Parameter
if __name__ == "__main__":
    if len(sys.argv)==1:
        #Default: start
        start()
        #logger.setLevel(logging.DEBUG)
        #start(test_run=True)
    elif len(sys.argv)>1:
        arg=sys.argv[1]
        if str(arg)=="start":
            start()
        elif str(arg)=="stop":
            stop()
        elif str(arg)=="testrun":
            logger.setLevel(logging.DEBUG)
            start(test_run=True)

        elif str(arg)=="status":
            status()
        else:
            logger.info("Usage: mover (start|stop|status|testrun)")
