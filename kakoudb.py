# -*- coding: cp936 -*-
import cx_Oracle
import time

class KakouOrc:
    def __init__(self,host='localhost',user='kakou', passwd='kakou',sid='kakou'):
        self.host    = host
        self.user    = user
        self.passwd  = passwd
        self.port    = 1521
        self.sid     = sid
        self.cur = None
        self.conn = None
        
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

    def getTest(self):
        try:
            self.cur.execute("select TYPE_EXTVAL11,TYPE_VALUE from config_info where id>= 200")
        except Exception,e:
            print e
            raise
        else:
            return self.rowsToDictList()

    def getConfigInfo(self):
        try:
            self.cur.execute("select ID,TYPE_VALUE,TYPE_ID,TYPE_ALIAS from config_info  where type_name='卡口名称'")
        except Exception,e:
            raise
        else:
            return self.rowsToDictList()
    
    def getWlcp(self):
        try:
            self.cur.execute("select HPHM from wlcp where clbj='T'")
        except Exception,e:
            raise
        else:
            return self.cur.fetchall()

    def getBkcp(self):
        try:
            self.cur.execute("select HPHM from bkcp where clbj='T'")
        except Exception,e:
            raise
        else:
            return self.cur.fetchall()
        
    def addCltx(self,values):
        try:
            self.cur.executemany('insert into cltx(FXBH,HPHM,HPZL,HPYS,JGSJ,CLSD,TJTP,QMTP,JLLX,CLBJ,HDGG,QBGG,CFGG,WZDD,CLXS,CDBH,KKBH,FJDM,CSYS,CSYSSQ,CSCD) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21)',values)
        except Exception,e:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
    
    def addWcltx(self,values,w_values):
        s=False
        try:
            self.cur.executemany('insert into cltx(FXBH,HPHM,HPZL,HPYS,JGSJ,CLSD,TJTP,QMTP,JLLX,CLBJ,HDGG,QBGG,CFGG,WZDD,CLXS,CDBH,KKBH,FJDM,CSYS,CSYSSQ,CSCD) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21)',values)
            if len(w_values)>0:
                self.cur.executemany('insert into wcltx(FXBH,HPHM,HPZL,HPYS,JGSJ,CLSD,TJTP,QMTP,JLLX,CLBJ,HDGG,QBGG,CFGG,WZDD,CLXS,CDBH,KKBH,FJDM,CSYS,CSYSSQ,CSCD) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21)',w_values)
        except Exception,e:
            self.conn.rollback()
            raise
        else:
            s=True
            self.conn.commit()
        finally:
            return s

    def addWcltx2(self,values):
        try:
            #print '123'
            self.cur.executemany('insert into wcltx(FXBH,HPHM,HPZL,HPYS,JGSJ,CLSD,TJTP,QMTP,JLLX,CLBJ,HDGG,QBGG,CFGG,WZDD,CLXS,CDBH,KKBH,FJDM,CSYS,CSYSSQ,CSCD) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21)',values)
        except Exception,e:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()

    def get_bkcp_by_hphm(self, hphm):
        try:
            self.cur.execute("select hphm, clbj, lk, mobiles, memo from bkcp WHERE hphm='%s' AND clbj='T'" % hphm)
        except Exception,e:
            raise
        else:
            return self.rowsToDictList()

        
    def rowsToDictList(self):
        columns = [i[0] for i in self.cur.description]
        return [dict(zip(columns, row)) for row in self.cur]

    def orcCommit(self):
        self.conn.commit()

if __name__ == "__main__":
    def getUrl(ip,jlbh):
        url = "http://"+'%s:8088/'%ip+jlbh[10:12]+'/'+'vhipict'+'/'+jlbh[12:18]+'/'+jlbh[18:20]+'/'+jlbh[20:22]+'/'+jlbh[0:12]+'-'+jlbh[12:26]+'-'+jlbh[26:27]+'-1.jpg'
        return url
    
    import datetime
    orc = KakouOrc(host='localhost',user='kakou', passwd='kakou',sid='kakou')
    #orc = KakouOrc(host='10.44.192.69',user='snapall', passwd='bkxtjc',sid='snapall')
    #values = []
    orc.setupOrc()
    print orc.get_bkcp_by_hphm('粤LXX266')
    #s= orc.getTest()
    #print s
    #print s
##    dict1 = {}
##    for i in s:
##        dict1[i['TYPE_EXTVAL11']] = i['TYPE_VALUE']
##    print dict1
    del orc
