# -*- coding: cp936 -*-
from kakoudb import KakouOrc
from baokangdb import BKOrc
from mysqldb import HMysql
import time,datetime

bk_hpzl = {0:'白牌',1:'黄牌',2:'蓝牌',3:'黑牌',4:'其他'}
bk_fxbh = {'SN':'由南往北','NS':'由北往南','EW':'由东往南','WE':'由西往东','OT':'出城','IN':'进城'}
bk_sbwz = {'10000003': '\xce\xf7A\xc2\xb7\xbf\xda', '10000002': '\xcc\xc1\xba\xe1\xc9\xee\xdb\xda\xbd\xbb\xbd\xe7\xc2\xb7\xbf\xda', '10000001': '\xbb\xdd\xd1\xf4\xba\xa3\xb9\xd8', '10000007': '\xd0\xc2\xd0\xb1\xc9\xee\xdb\xda\xbd\xbb\xbd\xe7\xb4\xa6', '10000006': '\xcf\xbc\xd3\xbf\xbb\xc6\xc4\xe0\xdb\xea', '10000005': '\xcf\xbc\xd3\xbf\xcf\xfe\xc1\xaa\xc2\xb7\xbf\xda', '10000004': '\xce\xf7\xc7\xf8\xd4\xb2\xc5\xcc', '10000018': '\xbf\xaa\xb3\xc7\xb4\xf3\xb5\xc0', '10000019': '\xc1\xfa\xba\xa3\xb6\xfe\xc2\xb7', '10000009': '\xce\xf7\xc7\xf8\xb6\xab\xd6\xb1\xbd\xd6\xd3\xeb\xb5\xad\xcb\xae\xbd\xbb\xbd\xe7\xb4\xa6', '10000008': '\xb7\xe7\xcc\xef\xcb\xae\xbf\xe2\xc7\xc5', '10000016': '\xb5\xad\xb0\xc4\xb4\xf3\xb5\xc0\xd3\xeb\xb5\xad\xcb\xae\xbd\xbb\xbd\xe7\xb4\xa6', '10000021': '\xb1\xb1\xbb\xb7\xc2\xb7', '10000020': '\xca\xaf\xbb\xaf\xb4\xf3\xb5\xc0', '10000023': '\xd0\xa1\xb9\xf0\xb8\xdf\xcb\xd9\xbf\xda', '10000022': '\xce\xf7\xc4\xcf\xb4\xf3\xb5\xc0', '10000025': '\xd0\xa1\xbe\xb6\xcd\xe5\xb8\xdf\xcb\xd9\xbf\xda', '10000024': '\xcf\xbc\xd3\xbf\xb8\xdf\xcb\xd9\xbf\xda', '10000017': '\xd1\xd8\xba\xa3\xb8\xdf\xcb\xd9\xb0\xc4\xcd\xb7\xca\xd5\xb7\xd1\xd5\xbe', '10000026': '\xca\xaf\xbb\xaf\xb8\xdf\xcb\xd9\xbf\xda'}
bk_hphm = {'00000000': '-'}

class FetchData:
    def __init__(self):
        self.bkorc = BKOrc(host='10.44.192.69',user='snapall', passwd='bkxtjc',sid='snapall')
        self.kkorc = KakouOrc(host='localhost',user='kakou', passwd='kakou',sid='kakou')
        self.mysql   = HMysql('localhost','root','','127.0.0.1')
        self.bkorc.login()
        self.kkorc.login()
        self.mysql.login()
        self.t1  = datetime.datetime(2014,11,30,11,30,0)
        self.end = datetime.datetime(2014,1,1,0,0,0)
        
    def __del__(self):
        del self.bkorc
        del self.kkorc
        
    def loopGetData(self):
        while 1:
            t2 = self.t1 - datetime.timedelta(minutes = 5)
            data = self.bkorc.getJGCLByTime(t2.strftime('%Y-%m-%d %H:%M:%S'),self.t1.strftime('%Y-%m-%d %H:%M:%S'))
            if len(data) == 0:
                pass
            else:
                values = []
                mysql_values = []
                for i in data:
                    #g = self.mysql.getJGCLByJLBH(i['JLBH'])
                    #if g == None:
                    values.append((bk_fxbh.get(i['FXBH'],'其他'),bk_hphm.get(i['HPHM'],i['HPHM']),'0',
                                   bk_hpzl.get(i['HPZL'],'其他'),i['JGSJ'],i['CLSD'],
                                   self.getUrl(i['SJLY'],i['JLBH']),'BK','0','F','F','F','F',
                                   bk_sbwz.get(i['BZWZDM'],'其他'),i['ZGXS'],i['CDBH'],
                                   '',i['CDBH'],'0',0,'0'))
                        #mysql_values.append((i['JLBH'],i['JGSJ']))
                    
                self.kkorc.addCltx(values)
                #self.mysql.addJGCL(mysql_values)
            print '%s - %s'%(self.t1,t2)
            self.t1 = t2
            time.sleep(0.1)
            
            if self.t1 < self.end:
                break
        
    def getUrl(self,ip,jlbh):
        url = "http://"+'%s:8088/'%ip+jlbh[10:12]+'/'+'vhipict'+'/'+jlbh[12:18]+'/'+jlbh[18:20]+'/'+jlbh[20:22]+'/'+jlbh[0:12]+'-'+jlbh[12:26]+'-'+jlbh[26:27]+'-1.jpg'
        return url

if __name__ == "__main__":
    #bkorc = BKOrc(host='10.44.192.69',user='snapall', passwd='bkxtjc',sid='snapall')
    #bkorc.login()
    #t1  = datetime.datetime(2014,12,1,0,0,0)
    #end = datetime.datetime(2014,1,1,0,0,0)
    #print t1.strftime('%Y-%m-%d %H:%M:%S')
##    while 1:
##        2 = t1 - datetime.timedelta(minutes = 1)
##        s = bkorc.getJGCLByTime(t1.strftime('%Y-%m-%d %H:%M:%S'),t2.strftime('%Y-%m-%d %H:%M:%S'))
##        print 'len',len(s)
##        print '%s - %s'%(t1,t2)
##        t1 = t2
##        
##        if t1 < end:
##            break
##        time.sleep(2)
    
    fd = FetchData()
    fd.loopGetData()
    del fd
    
##    ##    bkorc = BKOrc(host='10.44.192.69',user='snapall', passwd='bkxtjc',sid='snapall')
##    ##    kkorc = KakouOrc(host='localhost',user='kakou', passwd='kakou',sid='kakou')
##    ##    bkorc.login()
##    ##    kkorc.login()
##        s = bkorc.getJGCLByRowID('AAANEEAAoAABafvAAI')
##        #print s
##        values = []
##        values.append((bk_fxbh.get(s[0]['FXBH'],'其他'),s[0]['HPHM'],'0',
##                       bk_hpzl.get(s[0]['HPZL'],'其他'),s[0]['JGSJ'],s[0]['CLSD'],
##                       getUrl(s[0]['SJLY'],s[0]['JLBH']),'BK','0','F','F','F','F',
##                       bk_sbwz.get(s[0]['BZWZDM'],'其他'),s[0]['ZGXS'],s[0]['CDBH'],
##                       '',s[0]['CDBH'],'0',0,'0'))
##        kkorc.addWcltx2(values)
##        #s['SBBH'] = bk_hpzl.get(s['SBBH'],'其他')
##        #print s
##        del bkorc
##        del kkorc
