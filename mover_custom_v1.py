#!/usr/bin/python3

#Skript für Readcache.
#Dateien werden aus /mnt/disk[0-9]*/*Share*/**/* und /mnt/ssd-pool/*Share*/**/* gesammelt und entsprechend nach Zugriffsdatum und Größe aufgeteilt.

#Benötigt: python3

#Imports
import sys
import pathlib
import os
import subprocess
import glob

#Nutzervariablen
cacheSizeMax = 1150*1024*1024*1024 #Bytes (=1,15TiB von 1,65TiB)   Achtung: "btrfs filesystem usage /mnt/ssd-pool/" zeigt andere, aber (vermutlich) richtige Speicherauslastung als Unraid Web
cacheFileSizeMax = 400*1024*1024 #Bytes (=400MiB)

#Log (1/0 für true/false)
logLevel=1  #(1/0) für true oder false

#Dateien die vom Mover ignoriert werden sollen (werden trotzdem einbezogen, falls cacheSize noch nicht erreicht)
excludeFiles= []
excludeFiles.append(pathlib.Path('/mnt').glob('disk[0-9]*/**/Multimedia/Filme/**/*.mkv'))
excludeFiles.append(pathlib.Path('/mnt').glob('disk[0-9]*/**/Multimedia/Filme/**/*.mp4'))
excludeFiles.append(pathlib.Path('/mnt').glob('disk[0-9]*/**/Multimedia/Serien/**/*.mkv'))
excludeFiles.append(pathlib.Path('/mnt').glob('disk[0-9]*/**/Multimedia/Serien/**/*.mp4'))
excludeFiles.append(pathlib.Path('/mnt').glob('disk[0-9]*/**/Downloads/**/*'))
excludeFiles.append(pathlib.Path('/mnt').glob('disk[0-9]*/**/.Recycle.Bin/**/*'))

#Programmvariablen
pidFile="/var/run/mover.pid"

#Klasse für Dateien
class File: 
    def __init__(self, size, atime, mtime, ctime, path): 
        self.size = size
        self.atime = atime
        self.mtime = mtime
        self.ctime = ctime
        self.path = path


#Startfunktion
def start(testRun):
    #Prüfe ob Array gestartet
    if not os.path.exists("/mnt/user0"):
        print("mover_custom: Array nicht gestartet")
        exit(3)

    #Prüfe ob mover bereits läuft
    if os.path.isfile(pidFile):
        if os.path.exists("/proc/" + open(pidFile, "r").read().splitlines()[0]):
            print("mover_custom: läuft bereits mit PID: " + open(pidFile, "r").read().splitlines()[0])
            exit(1)
    
    #Verschieben starten
    pid=os.getpid()
    print("mover_custom: gestartet mit PID: " + str(pid))
    subprocess.run("echo \'" + str(pid) + "\' > " + pidFile, shell=True)

    #Erzeuge Dateiliste für ignorierte Dateien
    excludedFileList = []
    excludedFileListOnlyPaths = []
    for pathList in excludeFiles:
        for filePath in pathList:
            
            #Prüfe ob Pfad eine Datei
            if os.path.isfile(filePath):

                #Falls PID-Datei nicht mehr vorhanden --> Stoppe
                if not os.path.isfile(pidFile):
                    print("mover_custom: gestoppt (" + str(pidFile) +" nicht vorhanden)")
                    exit(0)
            
                #Hole Stats der Datei
                stat=os.stat(filePath)

                #Füge Datei der Liste hinzu
                excludedFileList.append(File(stat.st_size, stat.st_atime, stat.st_mtime, stat.st_ctime, filePath))
                excludedFileListOnlyPaths.append(str(filePath))

    #Erzeuge Dateiliste      
    fileList = [] 

    #Falls PID-Datei nicht mehr vorhanden --> Stoppe
    if not os.path.isfile(pidFile):
        print("mover_custom: gestoppt (" + str(pidFile) +" nicht vorhanden)")
        exit(0)

    #Durchlaufe alle Dateien der Disk[0-9]*
    print("mover_custom: Durchlaufe Dateien der disk[0-9]*")
    for filePath in pathlib.Path('/mnt').glob('disk[0-9]*/*Share*/**/*'):
        #Prüfe ob Pfad eine Datei & Pfad nicht in Ignorierliste
        if os.path.isfile(filePath) and not str(filePath) in excludedFileListOnlyPaths:
            
            #Falls PID-Datei nicht mehr vorhanden --> Stoppe
            if not os.path.isfile(pidFile):
                print("mover_custom: gestoppt (" + str(pidFile) +" nicht vorhanden)")
                exit(0)

            #Hole Stats der Datei
            stat=os.stat(filePath)

            #Füge Datei der Liste hinzu
            fileList.append(File(stat.st_size, stat.st_atime, stat.st_mtime, stat.st_ctime, filePath))

    #Durchlaufe alle Dateien des Caches
    print("mover_custom: Durchlaufe Dateien des ssd-pools")
    for filePath in pathlib.Path('/mnt').glob('ssd-pool/*Share*/**/*'):
        #Prüfe ob Pfad eine Datei & Pfad nicht in Ignorierliste
        if os.path.isfile(filePath) and not str(filePath) in excludedFileListOnlyPaths:
            
            #Falls PID-Datei nicht mehr vorhanden --> Stoppe
            if not os.path.isfile(pidFile):
                print("mover_custom: gestoppt (" + str(pidFile) +" nicht vorhanden)")
                exit(0)
            
            #Hole Stats der Datei
            stat=os.stat(filePath)

            #Füge Datei der Liste hinzu
            fileList.append(File(stat.st_size, stat.st_atime, stat.st_mtime, stat.st_ctime, filePath))

    #Sortiere die Liste nach aTime
    print("mover_custom: Sortiere die Dateiliste nach Zugriffszeit")
    fileList.sort(key=lambda x: x.atime, reverse=True)

    #Falls PID-Datei nicht mehr vorhanden --> Stoppe
    if not os.path.isfile(pidFile):
        print("mover_custom: gestoppt (" + str(pidFile) +" nicht vorhanden)")
        exit(0)

    #Erzeuge Liste für Cache- und Arraydateien
    cacheList = []
    arrayList = []

    #Berechne vorhandene Cache-Größe (z.B. durch appdata und system)
    cacheSize = 0
    for dir in glob.glob('/mnt/ssd-pool/*'):
        if not "Share" in str(dir):
            cacheSize = cacheSize + int(subprocess.check_output(['du','-bs', dir]).split()[0].decode('utf-8'))
    print("mover_custom: Berechne belegte Cachegröße (z.B. durch systen, appdata, etc. ohne *Share*): "+str(round(cacheSize/(1024*1024*1024),2))+"/"+ str(round(cacheSizeMax/(1024*1024*1024),2))+" GiB")

    #Durchlaufe Liste aller Dateien
    for file in fileList:
        #Falls PID-Datei nicht mehr vorhanden --> Stoppe
        if not os.path.isfile(pidFile):
            print("mover_custom: gestoppt (" + str(pidFile) +" nicht vorhanden)")
            exit(0)
        
        #Prüfe ob Datei kleiner ist als Nutzergrenze und Gesamtgröße des Cache kleiner ist als Nutzergrenze
        if (file.size < cacheFileSizeMax) and ((cacheSize + file.size) < cacheSizeMax):
            cacheList.append(file)
            cacheSize = cacheSize + file.size
        else:
            arrayList.append(file)

    #Lösche die Gesamtdateiliste (nicht mehr benötigt)
    fileList.clear()

    #Erzeuge zweite (finale) Arrayliste
    arrayListFinal = []

    #Merke aktuelle Größe des Caches (zu Informationszwecken später)
    cacheListBasicLength=len(cacheList)
    cacheListBasicSize=cacheSize

    #Falls Gessamtgröße des Caches die Maximalgröße noch nicht erreicht hat, füge weitere Dateien von der Arrayliste und Ignorierliste hinzu
    arrayAndExcluededCombinedList = []
    arrayAndExcluededCombinedList.extend(arrayList)
    arrayAndExcluededCombinedList.extend(excludedFileList)
    arrayAndExcluededCombinedList.sort(key=lambda x: x.atime, reverse=True)
    if cacheSize < cacheSizeMax:
        for file in arrayAndExcluededCombinedList:
            if (cacheSize + file.size) < cacheSizeMax:
                cacheList.append(file)
                cacheSize = cacheSize + file.size
            else:
                arrayListFinal.append(file)

    #Lösche die nicht finale ArrayList
    arrayList.clear()

    #Falls PID-Datei nicht mehr vorhanden --> Stoppe
    if not os.path.isfile(pidFile):
        print("mover_custom: gestoppt (" + str(pidFile) +" nicht vorhanden)")
        exit(0)

    #Gebe Listen aus
    if logLevel > 0:
        print("mover_custom: -----------------------IGNORIER-LISTE-----------------------------")
        for filePath in excludedFileListOnlyPaths:
            print ("mover_custom: Dateipfad: " + str(filePath))
        print("mover_custom: -----------------------ARRAY-LISTE-----------------------------")
        for file in arrayListFinal:
            print("mover_custom: Dateipfad: " + str(file.path) + " | Zugriffszeit: " + str(file.atime) + " | Größe: " + str(round(file.size/(1024*1024),2)) + " MiB")
        print("mover_custom: -----------------------CACHE-LISTE-----------------------------")
        i=0
        for file in cacheList:
            if i==cacheListBasicLength:
                print("mover_custom: --------------------CACHE-GRÖßE: "+ str(round(cacheListBasicSize/(1024*1024*1024),2))+"/"+ str(round(cacheSizeMax/(1024*1024*1024),2))+" GiB--------------------")  
                print("mover_custom: --------------------CACHE-LISTE-ERWEITERT---------------------")
            print("mover_custom: Dateipfad: " + str(file.path) + " | Zugriffszeit: " + str(file.atime) + " | Größe: " + str(round(file.size/(1024*1024),2)) + " MiB")
            i=i+1
        print("mover_custom: --------------------CACHE-GRÖßE-ERWEITERT: "+ str(round(cacheSize/(1024*1024*1024),2))+"/"+ str(round(cacheSizeMax/(1024*1024*1024),2))+" GiB--------------------")    

    #MOVE: Cache --> Array
    print ("mover_custom: -----------------------MOVE-----------------------------")
    for file in arrayListFinal:
        #Falls PID-Datei nicht mehr vorhanden --> Stoppe
        if not os.path.isfile(pidFile):
            print("mover_custom: gestoppt (" + str(pidFile) +" nicht vorhanden)")
            exit(0)
        
        #Falls Datei auf Cache liegt --> Verschiebe
        if not "/mnt/disk" in str(file.path):
            if logLevel > 0:
                print("mover_custom: Move: " + str(file.path) + " | Zugriffszeit: " + str(file.atime) + " | Größe: " + str(round(file.size/(1024*1024),2)) + " MiB")
            if testRun > 0:
                subprocess.run("echo \"" + str.replace(str(file.path),"$","\$") + "\" | /usr/local/sbin/move -d " + str(logLevel), shell=True)

    #MOVE: Array --> Cache
    for file in cacheList:
        #Falls PID-Datei nicht mehr vorhanden --> Stoppe
        if not os.path.isfile(pidFile):
            print("mover_custom: gestoppt (" + str(pidFile) +" nicht vorhanden)")
            exit(0)

        #Falls Datei auf Array liegt --> Verschiebe
        if "/mnt/disk" in str(file.path):
            #Erzeuge zunächst Shareordner im ssd-pool (falls noch nicht existiert)
            shareName=str(file.path).split(os.path.sep)[3]
            subprocess.run("mkdir -p /mnt/ssd-pool/"+shareName, shell=True)
            #Verschiebe
            if logLevel > 0:
                print("mover_custom: Move: " + str(file.path) + " | Zugriffszeit: " + str(file.atime) + " | Größe: " + str(round(file.size/(1024*1024),2)) + " MiB")
            if testRun > 0:
                subprocess.run("echo \"" + str.replace(str(file.path),"$","\$") + "\" | /usr/local/sbin/move -d " + str(logLevel), shell=True)


    print("mover_custom: beendet")
    subprocess.run("rm -f " + pidFile, shell=True)

    #Lösche Listen
    excludedFileList.clear()
    excludedFileListOnlyPaths.clear()
    excludeFiles.clear()
    arrayListFinal.clear()
    cacheList.clear()

def stop():
    #Prüfe ob mover bereits läuft
    if not os.path.isfile(pidFile):
        print("mover_custom: läuft nicht")            
        exit(0)
    print("mover_custom: lösche " + str(pidFile))
    subprocess.run("rm -f " + str(pidFile), shell=True)

def status():
    #Falls PID-Datei nicht mehr vorhanden --> Gestoppt
    if not os.path.isfile(pidFile):
        print("mover_custom: gestoppt (" + str(pidFile) +" nicht vorhanden)")
        exit(1)
    #Falls vorhanden --> Läuft
    else:
        print("mover_custom: läuft mit PID: " + open(pidFile, "r").read().splitlines()[0])
        exit(0)

#Main Funktion: Entscheide nach mitgegebenen Parameter
if len(sys.argv)==1:
    #Default=start
    start(1)
elif len(sys.argv)>1:
    arg=sys.argv[1]
    if str(arg)=="start":
        start(1)
    elif str(arg)=="stop":
        stop()
    elif str(arg)=="testrun":
        logLevel=1
        start(0)
    elif str(arg)=="status":
        status()
    else:
        print("Usage: mover (start|stop|status|testrun)")
