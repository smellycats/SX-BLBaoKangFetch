# -*- coding: utf-8 -*-
import time
import datetime
import json

import requests

from baokangdb import BKOrc
from union_kakou import UnionKakou


bk_hpzl = {0: u'白牌',1: u'黄牌',2: u'蓝牌',3: u'黑牌',4: u'其他'}
bk_fxbh = {
    'SN': u'由南往北',
    'NS': u'由北往南',
    'EW': u'由东往南',
    'WE': u'由西往东',
    'OT': u'出城',
    'IN': u'进城'
}
bk_sbwz = {
    '00000011': {'id': '441322001', 'name': u'飞鹅岭卡口'},
    '00000010': {'id': '441322002', 'name': u'石坝卡口'},
    '00000014': {'id': '441322003', 'name': u'生鸡头卡口'},
    '00000206': {'id': '441322004', 'name': u'石洲卡口'},
    '00000009': {'id': '441322005', 'name': u'永石卡口辅道'},
    '00000202': {'id': '441322006', 'name': u'龙桥卡口'},
    '00000203': {'id': '441322007', 'name': u'罗浮山卡口'},
    '00000013': {'id': '441322008', 'name': u'永石卡口'},
    '00000201': {'id': '441322009', 'name': u'白马围卡口'}
}
bk_sbwz2 = {
    '00000011': '441322001',
    '00000010': '441322002',
    '00000014': '441322003',
    '00000206': '441322004',
    '00000009': '441322005',
    '00000202': '441322006',
    '00000203': '441322007',
    '00000013': '441322008',
    '00000201': '441322009'
}
bk_hphm = {'00000000': '-'}


class FetchData:
    def __init__(self):
        self.bkorc = BKOrc(host='10.44.247.227',user='snapall', passwd='bkxtjc',sid='snapall')
        self.uk = UnionKakou(**{'host': '10.47.223.145', 'port':8080})
        #self.kkorc = KakouOrc(host='localhost',user='kakou', passwd='kakou',sid='kakou')
        #self.mysql = HMysql('localhost','root','','127.0.0.1')

        self.bkorc.login()
        #self.kkorc.login()
        #self.mysql.login()
        
    def __del__(self):
        del self.bkorc
        #del self.kkorc
            
    def bk_login(self):
        self.bkorc.login()
    
    def getData(self):
        time.sleep(0.1)
        JLBH_list = self.bkorc.getJLBH(100)

        if len(JLBH_list) == 0:
            time.sleep(0.25)
            return
        else:
            data = []
            del_jlbh_list = []
            for i in JLBH_list:
                s = self.bkorc.getJGCLByJLBH(i['JLBH'])
                #print s
                del_jlbh_list.append(i['JLBH'])
                data.append({'jgsj': str(s[0]['JGSJ']),
                             'hphm': bk_hphm.get(s[0]['HPHM'], s[0]['HPHM'].decode('utf-8')),
                             'kkdd_id': bk_sbwz2.get(s[0]['BZWZDM'], '441322000'),
                             'hpys_id': s[0]['HPZL'],
                             'fxbh': s[0]['FXBH'],
                             'cdbh': s[0]['CDBH'],
                             'hpzl': '00',
                             'clsd': s[0]['CLSD'],
                             'img_path': self.getUrl(s[0]['JLBH'])
                             })
	#print data
        r = self.uk.post_kakou(data)
        #print r
        self.bkorc.delJLBHByJLBH(del_jlbh_list)
        
    def getUrl(self, jlbh):
        url = "http://10.44.247.228:8088/"+jlbh[10:12]+'/vhipict/'+jlbh[12:18]+'/'+jlbh[18:20]+'/'+jlbh[20:22]+'/'+jlbh[0:12]+'-'+jlbh[12:26]+'-'+jlbh[26:27]+'-1.jpg'
        return url

if __name__ == "__main__":
    print 'main'
    fd = FetchData()
    bk_state = True
    while 1:
        try:
            if not bk_state:
                fd.bk_login()
            fd.getData()
            time.sleep(1)
        except Exception as e:
            print e
            bk_state = False
            time.sleep(15)
    del fd

