import threading
import requests
import time
import uuid
import sys

from PyQt5.QtCore import pyqtSlot, QThread, QTimer
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *

import pymysql

PUSHSERVER_URL = "http://localhost:8000/condition_push/"

class pushRequest():
    def __init__(self, requestURI):
        self.request_uri = requestURI

    def send(self, arg):
        res = requests.get(self.request_uri, params=arg)
    

class APIDatabase():
    def __init__(self, host='localhost', user='', password='', db=''):
        self.connection = pymysql.connect(host=host,
                                          user=user,
                                          password=password,
                                          db=db,
                                          charset='utf8',
                                          cursorclass=pymysql.cursors.DictCursor)

        self.cursor = self.connection.cursor()


    def save_conditionlist(self, index, condition_name):
        sql = """ insert into api_conditionexpresslist 
                  (express_index, express_name, express_content)
                  values (%s, %s, %s)
                  ON DUPLICATE KEY UPDATE express_name=%s """

        self.cursor.execute(sql, (index, condition_name, '', condition_name))
        return self.connection.commit()


    def truncate_investmentitem(self):
        sql="truncate api_investmentitems"
        self.cursor.execute(sql)


    def save_investmentitem(self, item_code, item_name, condition_id):
        sql = """ insert into api_investmentitems 
                  (item_code, item_name, item_condition_id, 
                  item_marketcap, item_transactions, item_current_price,
                  item_high_price, item_low_price, item_price, item_yester_price,
                  item_percentage) 
                  values(%s, %s, %s, 0, 0, 0, 0, 0, 0, 0, 0) """

        try:
            self.cursor.execute(sql, (item_code, item_name, condition_id))
        except:
            print(self.cursor._last_executed)

        return self.connection.commit()

    def delete_investmentitem(self, item_code):
        sql = """ delete from api_investmentitems
                  where item_code=%s """

        try:
            self.cursor.execute(sql, item_code)
        except:
            print(self.cursor._last_executed)

        return self.connection.commit()

    
    def update_investmentitem(self, item_code, item_marketcap, 
                              item_transactions, item_current_price, 
                              item_high_price, item_low_price, 
                              item_price, item_percentage,
                              item_yester_price):

        sql = """ update api_investmentitems
                  set item_marketcap=%s, item_transactions=%s, item_current_price=%s,
                  item_high_price=%s, item_low_price=%s,
                  item_price=%s, item_yester_price=%s, item_percentage=%s 
                  where item_code=%s """
        
        try:
            self.cursor.execute(sql, (item_marketcap, item_transactions, 
                                      item_current_price, item_high_price, 
                                      item_low_price, item_price, item_yester_price,
                                      item_percentage, item_code))
        except:
            print(self.cursor._last_executed)            

        return self.connection.commit()



class KiWoomApi(QMainWindow):
    ConditionNameList = dict()
    CodeList = dict()

    scrNum = 5000

    def __init__(self, Database):
        super().__init__()

        self.db = Database

        self.btn1 = QPushButton("automatic", self)
        self.btn1.clicked.connect(self.automaticbtn_event)

        self.btn2 = QPushButton("real_automatic", self)
        self.btn2.clicked.connect(self.real_automatic_event)

        self.kiwoom_ocx = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")

        self.kiwoom_ocx.OnEventConnect.connect(self.OnEventConnect)
        self.kiwoom_ocx.OnReceiveMsg.connect(self.OnReceiveMsg)

        self.kiwoom_ocx.OnReceiveTrData.connect(self.OnReceiveTrData)
        self.kiwoom_ocx.OnReceiveRealData.connect(self.OnReceiveRealData)
        self.kiwoom_ocx.OnReceiveMsg.connect(self.OnReceiveMsg)

        self.kiwoom_ocx.OnReceiveConditionVer.connect(self.OnReceiveConditionVer)
        self.kiwoom_ocx.OnReceiveTrCondition.connect(self.OnReceiveTrCondition)
        self.kiwoom_ocx.OnReceiveRealCondition.connect(self.OnReceiveRealCondition)

        self.db.truncate_investmentitem()

        self.CommConnect()


    def do_automatic(self):
        #QTimer.singleShot(1000 * 60 * 2, lambda: self.do_real_automatic())
        self.btn1.animateClick() 

        
    def automaticbtn_event(self):
        self.getConditionLoad()


    def do_real_automatic(self):
        self.btn2.animateClick()


    def real_automatic_event(self):
        for index, name in self.ConditionNameList.items():
            self.SendCondition(str(self.getScrNum()), name, index, 1)


    def getScrNum(self):
        if self.scrNum < 9999:
            self.scrNum += 1
        else:
            self.scrNum = 5000

        return self.scrNum


    def CommConnect(self):
        return self.kiwoom_ocx.dynamicCall("CommConnect()")


    @pyqtSlot(str, str)
    def GetRepeatCnt(self, TrCode, RecordName):
        return self.kiwoom_ocx.dynamicCall("GetRepeatCnt(QString, QString)", TrCode, RecordName)


    @pyqtSlot(str, int, int, int, str, str)
    def CommKwRqData(self, ArrCode, Next, CodeCount, TypeFlag, RQName, ScreenNo):
        return self.kiwoom_ocx.dynamicCall("CommKwRqData(QString, int, int, int, QString, QString)", ArrCode, Next, CodeCount, TypeFlag, RQName, ScreenNo)


    @pyqtSlot(str, str, int, str)
    def GetCommData(self, TrCode, RecordName, Index, ItemName):
        return self.kiwoom_ocx.dynamicCall("GetCommData(QString, QString, int, QString)", TrCode, RecordName, Index, ItemName)

    @pyqtSlot(str, int)
    def GetCommRealData(self, strCode, nFid):
        return self.kiwoom_ocx.dynamicCall("GetCommRealData(QString, int)", strCode, nFid)


    def getConditionLoad(self):
        return self.kiwoom_ocx.dynamicCall("GetConditionLoad()")


    @pyqtSlot(result=str)
    def getConditionNameList(self):
        return self.kiwoom_ocx.dynamicCall("GetConditionNameList()")


    @pyqtSlot(str, str, int, int)
    def SendCondition(self, strScrNo, strConditionName, nIndex, nSearch):
        return self.kiwoom_ocx.dynamicCall("SendCondition(QString, QString, int, int)", strScrNo, strConditionName, nIndex, nSearch)


    @pyqtSlot(str, str, int)
    def SendConditionStop(self, ScrNo, strConditionName, nIndex):
        return self.kiwoom_ocx.dynamicCall("SendConditionStop(QString, QString, int)", ScrNo, strConditionName, nIndex)


    def GetMasterCodeName(self, strCode):
        return self.kiwoom_ocx.dynamicCall("GetMasterCodeName(QString)", strCode)

    def GetMasterLastPrice(self, strCode):
        return self.kiwoom_ocx.dynamicCall("GetMasterLastPrice(QString)", strCode)


    def OnEventConnect(self, err_code):
        if err_code == 0:
            self.do_automatic()

    def OnReceiveMsg(self, ScrNo, RQName, TrCode, Msg):
        print(OnRecevieMsg.__name__, RQName, Msg)

    @pyqtSlot(str, str, str, str, str, int, str, str, str)
    def OnReceiveTrData(self, ScrNo, RQName, TrCode, RecordName, PrevNext, DataLength, ErrorCode, Message, SplmMsg):
        if RQName == "주식기본정보":
            cnt = self.GetRepeatCnt(TrCode, RecordName)

            for i in range(cnt):
                item_code = self.GetCommData(TrCode, RQName, i, "종목코드").strip()

                item_marketcap = self.GetCommData(TrCode, RQName, i, "시가총액")

                item_transactions = self.GetCommData(TrCode, RQName, i, "거래량").strip()

                item_current_price = self.GetCommData(TrCode, RQName, i, "시가").strip()
                item_high_price = self.GetCommData(TrCode, RQName, i, "고가").strip()
                item_low_price = self.GetCommData(TrCode, RQName, i, "저가").strip()
                item_price = self.GetCommData(TrCode, RQName, i, "현재가").strip()
                item_yester_price = self.GetMasterLastPrice(item_code)

                item_percentage = self.GetCommData(TrCode, RQName, i, "등락율").strip()

                d = dict(item_code=item_code, 
                        item_marketcap=item_marketcap,
                        item_transactions=item_transactions,
                        item_current_price=item_current_price,
                        item_high_price=item_high_price,
                        item_low_price=item_low_price,
                        item_price=item_price,
                        item_yester_price=item_yester_price,
                        item_percentage=item_percentage)


                self.db.update_investmentitem(**d)
        else:
            return


    def OnReceiveRealData(self, Code, RealType, RealData):
        print(Code, RealType)
        if RealType == "주식시세":
            print(self.GetMasterCodeName(Code), "시세 변경")
            item_code = Code

            item_marketcap = self.GetCommRealData(RealType, 311).strip()

            item_transactions = self.GetCommRealData(RealType, 13).strip()

            item_current_price = self.GetCommRealData(RealType, 16).strip()
            item_high_price = self.GetCommRealData(RealType, 17).strip()
            item_low_price = self.GetCommRealData(RealType, 18).strip()
            item_price = self.GetCommRealData(RealType, 10).strip()
            item_yester_price = self.GetMasterLastPrice(item_code)

            item_percentage = self.GetCommRealData(RealType, 12).strip()

            
            d = dict(item_code=item_code, 
                    item_marketcap=item_marketcap,
                    item_transactions=item_transactions,
                    item_current_price=item_current_price,
                    item_high_price=item_high_price,
                    item_low_price=item_low_price,
                    item_price=item_price,
                    item_yester_price=item_yester_price,
                    item_percentage=item_percentage)

            self.db.update_investmentitem(**d)
        else:
            return


    def OnReceiveConditionVer(self, lRet, sMsg):
        if lRet != 1: 
            pass
        else:
            raw_list = self.getConditionNameList().split(';')
            raw_list.pop()

            for element in raw_list:
                index, name = element.split('^')
                index = int(index)

                self.ConditionNameList[index] = name
                self.db.save_conditionlist(index, name)

        return self.do_real_automatic()


    def OnReceiveTrCondition(self, ScrNo, CodeList, ConditionName, nIndex, nNext):
        if self.CodeList.get(nIndex):
            return

        self.CodeList[nIndex] = CodeList.split(';')[: -1]
        for code in self.CodeList[nIndex]:
            item_name = self.GetMasterCodeName(code)
            condition_name = self.ConditionNameList[nIndex]

            self.db.save_investmentitem(code, item_name, condition_name)

        codelen = len(self.CodeList[nIndex])
        if not codelen:
            return    

        codelist = CodeList[: -1]
        randomString = str(uuid.uuid4())
        time.sleep(1)
        while self.CommKwRqData(codelist, 0, codelen, 0, "주식기본정보", self.getScrNum()) == -200:
            pass


    @pyqtSlot(str, str, str, str)
    def OnReceiveRealCondition(self, sCode, sType, strConditionName, strConditionIndex):
        push_request = pushRequest(PUSHSERVER_URL)

        arg = dict()    
        item_name = str()
        arg['condition_index'] = strConditionIndex

        if sType == "I":
            item_name = self.GetMasterCodeName(sCode)
            print(item_name, "편입")

            self.db.save_investmentitem(sCode, item_name, strConditionName) 
            while self.CommKwRqData(sCode, 0, 1, 0, "주식기본정보", self.getScrNum()) == -200:
                pass

            arg['status'] = '1'

        elif sType == "D":
            item_name = self.GetMasterCodeName(sCode)
            print(item_name, "이탈")

            self.db.delete_investmentitem(sCode)

            arg['status'] = '0'


        arg['item_name'] = item_name
        push_request.send(arg)    

if __name__ == "__main__":
    app = QApplication(sys.argv)

    db = APIDatabase(host='localhost', 
                    user='',
                    password='',
                    db ='')  # CREATE DATABASE (DATABASE_NAME) DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

    api = KiWoomApi(db)
    api.show()
    app.exec_()
