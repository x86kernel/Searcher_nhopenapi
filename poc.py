import threading
import time
import uuid
import sys

from PyQt5.QtCore import pyqtSlot, QThread, QTimer
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *

import pymysql


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
                  (express_index, express_name) 
                  values (%s, %s) 
                  ON DUPLICATE KEY UPDATE express_name=%s """

        self.cursor.execute(sql, (index, condition_name, condition_name))
        return self.connection.commit()


    def truncate_investmentitem(self):
        sql="truncate api_investmentitems"
        self.cursor.execute(sql)


    def save_investmentitem(self, item_code, item_name, condition_id):
        sql = """ insert into api_investmentitems 
                  (item_code, item_name, item_condition_id, 
                  item_transactions, item_current_price,
                  item_high_price, item_low_price, item_price,
                  item_percentage) 
                  values(%s, %s, %s, 0, 0, 0, 0, 0, 0) """

        try:
            self.cursor.execute(sql, (item_code, item_name, condition_id))
        except MySQLError as e:
            print(cursor._last_executed)

        return self.connection.commit()

    
    def update_investmentitem(self, item_code, item_transactions,
                              item_current_price, item_high_price,
                              item_low_price, item_price, item_percentage):

        sql = """ update api_investmentitems
                  set item_transactions=%s, item_current_price=%s,
                  item_high_price=%s, item_low_price=%s,
                  item_price=%s, item_percentage=%s 
                  where item_code=%s """
        
        try:
            self.cursor.execute(sql, (item_transactions, item_current_price,
                                        item_high_price, item_low_price,
                                        item_price, item_percentage, item_code))
        except:
            print(cursor._last_executed)            

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


    def getConditionLoad(self):
        print("GetConditionLoad()")
        return self.kiwoom_ocx.dynamicCall("GetConditionLoad()")


    @pyqtSlot(result=str)
    def getConditionNameList(self):
        return self.kiwoom_ocx.dynamicCall("GetConditionNameList()")


    @pyqtSlot(str, str, int, int)
    def SendCondition(self, strScrNo, strConditionName, nIndex, nSearch):
        print("SendCondition(", strScrNo, ",", strConditionName, ",", nIndex, ",", nSearch, ")")
        return self.kiwoom_ocx.dynamicCall("SendCondition(QString, QString, int, int)", strScrNo, strConditionName, nIndex, nSearch)


    @pyqtSlot(str, str, int)
    def SendConditionStop(self, ScrNo, strConditionName, nIndex):
        return self.kiwoom_ocx.dynamicCall("SendConditionStop(QString, QString, int)", ScrNo, strConditionName, nIndex)


    def GetMasterCodeName(self, strCode):
        return self.kiwoom_ocx.dynamicCall("GetMasterCodeName(QString)", strCode)


    def OnEventConnect(self, err_code):
        print(err_code)
        if err_code == 0:
            pass
            self.do_automatic()

    def OnReceiveMsg(self, ScrNo, RQName, TrCode, Msg):
        print(OnRecevieMsg.__name__, RQName, Msg)

    @pyqtSlot(str, str, str, str, str, int, str, str, str)
    def OnReceiveTrData(self, ScrNo, RQName, TrCode, RecordName, PrevNext, DataLength, ErrorCode, Message, SplmMsg):
        print(RQName)
        if RQName == "주식기본정보":
            cnt = self.GetRepeatCnt(TrCode, RecordName)

            for i in range(cnt):
                item_code = self.GetCommData(TrCode, RQName, i, "종목코드").strip()

                item_transactions = self.GetCommData(TrCode, RQName, i, "거래량").strip()
                item_current_price = self.GetCommData(TrCode, RQName, i, "시가").strip()
                item_high_price = self.GetCommData(TrCode, RQName, i, "고가").strip()
                item_low_price = self.GetCommData(TrCode, RQName, i, "저가").strip()
                item_price = self.GetCommData(TrCode, RQName, i, "현재가").strip()

                item_percentage = self.GetCommData(TrCode, RQName, i, "등락율").strip()

                d = dict(item_code=item_code, 
                        item_transactions=item_transactions,
                        item_current_price=item_current_price,
                        item_high_price=item_high_price,
                        item_low_price=item_low_price,
                        item_price=item_price,
                        item_percentage=item_percentage)


                self.db.update_investmentitem(**d)
        else:
            return


    def OnReceiveRealData(self, Code, RealType, RealData):
        print("test")
        print(Code, RealType)


    def OnReceiveMsg(self, ScrNo, RQName, TrCode, Msg):
        print(self.OnReceiveMsg.__name__)
        print(ScrNo, RQName)

    
    def OnReceiveConditionVer(self, lRet, sMsg):
        if lRet != 1: 
            return

        raw_list = self.getConditionNameList().split(';')
        raw_list.pop()

        for element in raw_list:
            index, name = element.split('^')
            index = int(index)

            self.ConditionNameList[index] = name
            self.db.save_conditionlist(index, name)


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

        test = CodeList[: -1]
        randomString = str(uuid.uuid4())
        while self.CommKwRqData(test, 0, codelen, 0, "주식기본정보", self.getScrNum()) == -200:
            pass


    @pyqtSlot(str, str, str, str)
    def OnReceiveRealCondition(self, sCode, sType, strConditionName, strConditionIndex):
        print(self.OnReceiveRealCondition.__name__)
        print(self.GetMasterCodeName(sCode), ": ", sType)


if __name__ == "__main__":
    app = QApplication(sys.argv)

    db = APIDatabase(host='localhost', 
                    user='',
                    password='',
                    db ='')  # CREATE DATABASE (DATABASE_NAME) DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

    api = KiWoomApi(db)
    api.show()
    app.exec_()