# -*- coding: utf-8 -*-
import cx_Oracle
import time

class BKOrc:
    def __init__(self,host='10.44.247.227',user='snapall', passwd='bkxtjc',sid='snapall'):
        self.host   = host
        self.user   = user
        self.passwd = passwd
        self.port   = 1521
        self.sid    = sid
        self.cur    = None
        self.conn   = None
        
    def __del__(self):
        if self.cur != None:
            self.cur.close()
        if self.conn != None:
            self.conn.close()
        
    def login(self):
        try:
            self.conn = cx_Oracle.connect(self.user,self.passwd,self.host+':'+str(self.port)+'/'+self.sid)
            self.cur = self.conn.cursor()
            #print self.passwd
        except Exception,e:
            raise

    def setupOrc(self):
        try:
            self.login()
        except Exception,e:
            #print now,e
            #logging.exception(e)
            #print now,'Reconn after 15 seconds'
            time.sleep(15)
            self.setupOrc()
        else:
            pass

    def gettest(self):
        try:
            self.cur.execute("select * from B_BK_JGCL where JLBH = '100000000405201411261010122'")
        except Exception,e:
            print e
            raise
        else:
            return self.rowsToDictList()

    def getJGCLByRowID(self,rowid):
        try:
            self.cur.execute("select * from B_BK_JGCL where rowid = '%s'"%rowid)
        except Exception,e:
            print e
            raise
        else:
            return self.rowsToDictList()

    def getJGCLByJLBH(self, jlbh):
        try:
            self.cur.execute("select * from B_BK_JGCL where jlbh = '%s'"%jlbh)
        except Exception,e:
            print e
            raise
        else:
            return self.rowsToDictList()

    #获取触发器产生的JLBH
    def getJLBH(self,step=10):
        try:
            self.conn.commit()
            self.cur.execute("SELECT a.ROWID,a.*from( SELECT t.*,t.ROWID FROM b_bk_jlbh t ORDER BY t.JGSJ DESC )a where ROWNUM <=%s"%step)
            #self.conn.commit()
        except Exception,e:
            print e
            raise
        else:
            return self.rowsToDictList()

    def delJLBHByRowid(self,rowid):
        try:
            self.cur.execute("delete from b_bk_jlbh where rowid = '%s'"%rowid)
            self.conn.commit()
        except Exception,e:
            print e
            raise

    def delJLBHByJLBH(self,jlbh_list):
        try:
            for i in jlbh_list:
                self.cur.execute("delete from b_bk_jlbh where jlbh = '%s'"%i)
            self.conn.commit()
        except Exception,e:
            print e
            raise

    def getJGCLByTime(self,t1,t2):
        try:
            self.cur.execute("select * from B_BK_JGCL where jgsj > to_date('%s','yyyy-mm-dd hh24:mi:ss') and jgsj<= to_date('%s','yyyy-mm-dd hh24:mi:ss') "%(t1,t2))
        except Exception,e:
            print e
            raise
        else:
            return self.rowsToDictList()
        
    def rowsToDictList(self):
        columns = [i[0] for i in self.cur.description]
        return [dict(zip(columns, row)) for row in self.cur]

    def orcCommit(self):
        self.conn.commit()

if __name__ == "__main__":
##    def getUrl(ip,jlbh):
##        url = "http://"+'%s:8088/'%ip+jlbh[10:12]+'/'+'vhipict'+'/'+jlbh[12:18]+'/'+jlbh[18:20]+'/'+jlbh[20:22]+'/'+jlbh[0:12]+'-'+jlbh[12:26]+'-'+jlbh[26:27]+'-1.jpg'
##        return url
##    
##    import datetime
    orc = BKOrc()
    #orc = KakouOrc(host='localhost',user='kakou', passwd='kakou',sid='kakou')
    #orc = KakouOrc(host='10.44.192.69',user='snapall', passwd='bkxtjc',sid='snapall')
    #values = []
    orc.login()
    #print orc.getJLBH()
    s = orc.getJGCLByJLBH('000760001402201606061518143')
    print s
    print (s[0]['HPHM'].decode('gbk'),)
    #s= orc.getJGCLByTime('2014-11-29 11:55:00','2014-11-29 11:56:00')
    #print s
##    dict1 = {}
##    for i in s:
##        dict1[i['TYPE_EXTVAL11']] = i['TYPE_VALUE']
##    print dict1
    del orc
