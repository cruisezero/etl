import os
import stat
import time
import sys
import logging
import datetime
from datetime import date, datetime, time
import psycopg2
import psycopg2.extras
from ftplib import FTP


def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i+1

def which_file(wfNetId, wftype):
    files = []
    print wfNetId
    ls = []
    ret_list = []
    ftp.retrlines('MLSD', ls.append)
    log_cur = db_connect()
    #qstatement = 'select * from file_log where net_id = %d and filename = %s', (wfNetId,)
    #print qstatement
    #log_rows = log_cur.conn(qstatement)

    
    for line in ls:
        #print line
        x_file = (line.split(";")[7]).strip()
        x_type = (line.split(";")[0]).strip()
        #print x_file
        #print x_type
        if x_type.find("dir") == -1 and x_file.find("pgp") > 0 :
            qstatement = "select * from file_log where net_id = %d and filename = '%s'"% (wfNetId,x_file)
            #print qstatement
            log_rows = log_cur.conn(qstatement)
            #print log_rows
            if len(log_rows) < 1:
                ret_list.append(x_file)
            
    #print ret_list
    return ret_list

class db_connect():
    
    def conn(self, connectQuery):
        conn = psycopg2.connect("dbname='netdata' user='postgres' password='' host='localhost'")
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(connectQuery)
        return cur.fetchall()
        


#pick_file = r'TalisPointProviderSafewayPPPExport_20150302.pgp'
pick_file = []


net_cur = db_connect()
rows = net_cur.conn('select * from net_cfg;')
#net_cur.cur('select * from net_cfg;')
#rows = cur.fetchall()
print rows
pick_file = []

for row in rows:
    print row['network']
    print row['username']
    print row['password']
    print row['id']
    if row['transfer_method'].upper() == 'FTP':
        print "Im here"
        ftp_loc = row['location'].split("/")
        print ftp_loc
        if len(ftp_loc) > 1:
            print ftp_loc[0]
            print ftp_loc[1]
        real_ftp = ftp_loc[0]
        ftp = FTP(real_ftp)
        ftp.login(row['username'],row['password'])
        #ftp.retrlines('LIST')
        network_id = row['id']
        ftype = row['file_type']
        pick_file = which_file(network_id, ftype)
        nofile = len(pick_file)
        #print pick_file
        try:
            #ftp.retrbinary('RETR %s' % pick_file,open(pick_file, 'wb').write)
            for num in range(0,nofile):
                fname =  pick_file[num].strip()
                print fname
                ftp.retrbinary('RETR %s' % fname,open(r'C:\xx\%s' % fname, 'wb').write)
                row_count = file_len(pick_file)
                pk = (row['id'])
                print "Inserting"
                break
                #cur.execute("insert into file_log (net_id,filename,filesize,record_count,received_dt, processed_dt) values (%s, %s, %s, %s, %s, %s)", [pk,pick_file,0,0,"03/13/2015","03/13/2015"])

        except Exception, err:
            sys.stderr.write('Error: %s\n' % str(err))
            print "Nooooo"
            
        ftp.quit()
    if row['transfer_method'].upper() == 'SFTP':
        print "Do this"
    if row['transfer_method'].upper() == 'DIR':
        for fileName in os.listdir ( r'/xx' ):
            print fileName
        print "Check if we ran these files and run the ones that haven't been ran"

    conn.commit()
    cur.close()
    conn.close()

#insert into file_log (net_id,filename,filesize,record_count) values (%d,%s,%d,%d)









'''


network = "ABC"
log_filename = network +"_"+str(datetime.date.today()).replace('-','')
logging.basicConfig(filename=log_filename,level=logging.DEBUG,format='%(asctime)s %(message)s')

try:
    fileStats = os.stat(r'c:\script.scp')
    fileInfo = {
        'size':fileStats[stat.ST_SIZE],
        'lastmodified':time.ctime(fileStats[stat.ST_MTIME]),
        'lastaccessed':time.ctime(fileStats[stat.ST_ATIME]),
        'creationtime':time.ctime(fileStats[stat.ST_CTIME]),
        'mode':fileStats[stat.ST_MODE],
        'dir':stat.S_ISDIR(fileStats[stat.ST_MODE])
    }
    print fileInfo
except Exception, err:
    sys.stderr.write('Error: %s\n' % str(err))
    logging.info(str(err))


'''
