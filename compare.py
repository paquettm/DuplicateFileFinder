#Unless you're using a really complicated and/or slow hash, loading the data from the disk is going to take much longer than computing the hash (unless you use RAM disks or top-end SSDs).
#So to compare two files, use this algorithm:
#    Compare sizes
#    Compare dates (be careful here: this can give you the wrong answer; you must test whether this is the case for you or not)
#    Compare the hashes
#This allows for a fast fail (if the sizes are different, you know that the files are different).
#To make things even faster, you can compute the hash once and save it along with the file. Also save the file date and size into this extra file, so you know quickly when you have to recompute the hash or delete the hash file when the main file changes.

#Make a DB with SQLite
#file table
#path, size, date, md5_hash

#scan sizes first
#query by size and count hash for sizes with more than 1
#use date to update scan when requested anew

# Importing Libraries
import os
import sys
from pathlib import Path
import hashlib
import argparse
import json
import time
import magic

import sqlite3
from sqlite3 import Error

conn = None #simulate a static variable with a global-to-module var
db_file = 'compare.db' #define which file to connect as a database

class Database:

    def __init__(self):
        """ create a database connection to a SQLite database """
        global conn
        global db_file
        if conn == None:
            try:
                #conn = sqlite3.connect(app.config['SQLITE_DATABASE'])
                source = sqlite3.connect(db_file)
                conn = sqlite3.connect(':memory:')
                source.backup(conn)
                #conn = sqlite3.connect(':memory:')
                conn.row_factory = self.dict_factory
            except Error as e:
                print(e)

    def toDISK():
        global conn
        dest = sqlite3.connect(db_file)
        conn.backup(dest)

    def __call__(self):
        global conn
        return conn

    def dict_factory(self,cursor, row):
        fields = [column[0] for column in cursor.description]
        return {key: value for key, value in zip(fields, row)}

    def query(self, sql, params):
        try:
            global conn
            c = conn.cursor()
            c.execute(sql,params)
            return c.fetchall()
        except Error as e:
            print(e)

    def queryOne(self, sql, params) -> any:
        try:
            global conn
            c = conn.cursor()
            c.execute(sql,params)
            result = c.fetchone()
            return result
        except Error as e:
            print(e)

    def create(self, sql, params) -> int:
        try:
            global conn
            c = conn.cursor()
            c.execute(sql,params)
            conn.commit()
            return c.lastrowid
        except Error as e:
            print(e)

    def execute(self, sql, params):
        try:
            global conn
            c = conn.cursor()
            c.execute(sql,params)
            conn.commit()
        except Error as e:
            print(e)

    def create_table(self, sql):
        try:
            global conn
            c = conn.cursor()
            c.execute(sql)
        except Error as e:
            print(e)

class File(Database):

    def __init__(self,path='',date='',size=0,md5_hash=''):
        super(File,self).__init__()
        self.create_table()
        self.setAll(path,date,size,md5_hash)

    def setAll(self,path='',date='',size=0,md5_hash=''):
        try:
            self.path=path #does not work .encode('utf-16','surrogatepass').decode('utf-16')
            self.size=size
            self.date=date
            self.md5_hash=md5_hash
        except Error as e:
            print(e)

    def makeAndPush(self, path,date,size):
        try:
            self.setAll(path,date,size)
            self.insert() #automatically clears md5_hash on size/date difference
            self.update()
        except Error as e:
            print(e)

    def create_table(self):
        sql = """CREATE TABLE IF NOT EXISTS file (
                   file_id integer PRIMARY KEY,
                   path text UNIQUE NOT NULL,
                   date text NOT NULL,
                   size int NOT NULL,
                   mime text,
                   md5_hash text
                 );"""
        return super(File,self).create_table(sql)

    def insert(self):
        try:
            sql = "INSERT OR IGNORE INTO file(path,size,date,md5_hash) VALUES (:path,:size,:date,:md5_hash)"
            return self.create(sql,{'path':self.path,'date':self.date,'size':self.size,'md5_hash':self.md5_hash})
        except Error as e:
            print(e)
            return false

    def update(self):
        sql = "UPDATE file SET md5_hash = CASE WHEN size != :size OR date != :date THEN '' ELSE md5_hash END, date = :date, size = :size WHERE path=:path"
        return self.execute(sql,{'path':self.path,'date':self.date,'size':self.size})

    def updateHash(self):
        sql = "UPDATE file SET md5_hash = :md5_hash WHERE path=:path"
        return self.execute(sql,{'path':self.path,'md5_hash':self.md5_hash})

    def delete(self,id):
        sql = "DELETE FROM file WHERE id=?"
        return self.execute(sql,(id,))

    def findByPath(self,path):
        sql = "SELECT * FROM file WHERE path = :path"
        return self.queryOne(sql,{"path":path})

    def listBySizeMoreThanOne(self):
        sql = "SELECT group_concat('{\"path\":\"' || path || '\", \"md5_hash\":\"' || md5_hash || '\"}',';') AS 'allPaths', size FROM file GROUP BY size HAVING COUNT(*)>1"
        return self.query(sql,{})

    def listBySizeAndHashMoreThanOne(self):
        sql = "SELECT group_concat('{\"path\":\"' || path || '\"}',';') AS 'allPaths', size, md5_hash FROM file WHERE md5_hash != '' GROUP BY size, md5_hash HAVING COUNT(*)>1"
        return self.query(sql,{})

def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        usage="%(prog)s [PATH]",
        description="SEARCH PATH SUBTREE FOR DUPLICATES BASED ON MD5 CHECKSUM."
    )
    parser.add_argument(
        "-v", "--version", action="version",
        version = f"{parser.prog} version 1.0.0"
    )
    parser.add_argument('path')
    parser.add_argument('-m', '--mode', default='normal')
    return parser

def ScanTree(folder):
    #TODO: is there a way to check the folder date to skip if no change?
    _file = File()
    # hashes is in format {hash:[names]}
    if os.path.isfile(folder): return
    files = sorted(os.listdir(folder))
    for file_name in files:
        if file_name == "." or file_name == "..": continue
        # Path to the file
        path = os.path.join(folder, file_name)
        if os.path.isfile(path):
            date = time.ctime(os.path.getmtime(path))
            size = os.path.getsize(path)
            #insert the file
            _file.makeAndPush(path,date,size) #automatically clears md5_hash on size/date difference
            # Calculate hash - later - only on size match
        else: #add files to scan if it is a directory
            ScanTree(path)
# Calculates md5_hash hash of file
# Returns HEX digest of file
def Hash_File(path):
    # Opening file in afile
    afile = open(path, 'rb')
    hasher = hashlib.md5()
    blocksize=65536
    buf = afile.read(blocksize)

    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    afile.close()
    return hasher.hexdigest()

def HashItAll():
    #hash files with same size
    _file = File()
    _list = _file.listBySizeMoreThanOne()
    _list = list(map(lambda x: {'size':x['size'],'allPaths':list(map(lambda x: json.loads(x), x['allPaths'].split(';')))}, _list))
    #start getting the hashes
    for _group_ in _list:
        for _file_ in _group_['allPaths']:
            _file.path = _file_['path']
            if _file_['md5_hash'] == '':
                _md5 = Hash_File(_file.path)
                _file.md5_hash = _md5
                _file.updateHash()

def EndItAll():
    #compare files with same hash
    _file = File()
    _list = _file.listBySizeAndHashMoreThanOne()
    print(_list)
    #content comparison

parser = init_argparse()
args = parser.parse_args()

print(args)

match args.mode:
    case 'normal':
        if args.path:
            #scantree
            path = Path(args.path)
            ScanTree(path)
            HashItAll()
            EndItAll()

    case 'hash':
        HashItAll()
        EndItAll()

    case 'end':
        EndItAll()

Database.toDISK()
