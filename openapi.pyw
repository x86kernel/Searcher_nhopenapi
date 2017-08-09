import threading
import requests
import logging
import time
import uuid
import sys

from PyQt5.QtCore import pyqtSlot, QThread, QTimer, QCoreApplication
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

    def select_investmentitem(self, field_name, where_name, where_value):
        sql = """ select %s
                  from api_investmentitems
                  where %s=%s """ % (field_name, where_name, where_value)
        
        try:
            self.cursor.execute(sql)
        except:
            logging.error('%s %s', self.select_investmentitem.__name__,
                                   self.cursor._last_executed)

        return self.cursor.fetchone()[field_name]
        

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
            logging.error('%s %s', self.save_investmentitem.__name__, 
                                   self.cursor._last_executed)

        return self.connection.commit()

    def delete_investmentitem(self, item_code):
        sql = """ delete from api_investmentitems
                  where item_code=%s """

        try:
            self.cursor.execute(sql, item_code)
        except:
            logging.error('%s %s', self.delete_investmentitem.__name__,
                                   self.cursor._last_executed)

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
            logging.error('%s %s', self.update_investmentitem.__name__, 
                                  self.cursor._last_executed)

        return self.connection.commit()



class KiWoomApi(QMainWindow):
    ConditionNameList = dict()
    CodeList = dict()

    scrNum = 5000

    def __init__(self, Database):
        super().__init__()
        
        self.setWindowTitle('Searcher')
        self.setGeometry(300, 300, 220, 220)

        self.db = Database

        self.btn_after_login = QPushButton("automatic", self)
        self.btn_after_login.setVisible(False)
        self.btn_after_login.clicked.connect(self.automaticbtn_event)

        self.btn_after_init = QPushButton("real_automatic", self)
        self.btn_after_init.setVisible(False)
        self.btn_after_init.clicked.connect(self.real_automatic_event)

        self.status_list = QListWidget(self)
        self.status_list.setGeometry(10, 10, 200, 150)

        self.logout_button = QPushButton('종료', self)
        self.logout_button.setGeometry(10, 180, 200, 30)
        self.logout_button.setEnabled(False)
        self.logout_button.clicked.connect(self.logout_button_handler)

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


    def add_status_message(self, msg):
        self.status_list.addItem(msg)
        self.status_list.scrollToBottom()

        return


    def do_automatic(self):  # automatic progress trigger when after login
        self.btn_after_login.animateClick() 

        
    def automaticbtn_event(self):
        self.getConditionLoad()


    def do_real_automatic(self):  # automatic process trigger when after init
        self.btn_after_init.animateClick()


    def real_automatic_event(self):
        for index, name in self.ConditionNameList.items():
            self.SendCondition(str(self.getScrNum()), name, index, 1)


    def logout_button_handler(self):
        if self.GetConnectState():
            quit_msg = '정말 종료하시겠습니까?'
            reply = QMessageBox.question(self, 'Message',
            quit_msg, QMessageBox.Yes, QMessageBox.No)

            if reply == QMessageBox.Yes:
                QCoreApplication.exit(0)
            else:
                pass

        return


    def getScrNum(self):
        if self.scrNum < 9999:
            self.scrNum += 1
        else:
            self.scrNum = 5000

        return self.scrNum


    def CommConnect(self):
        return self.kiwoom_ocx.dynamicCall("CommConnect()")

    
    def GetConnectState(self):
        return self.kiwoom_ocx.dynamicCall("GetConnectState()")


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
            logging.info('%s %d', self.OnEventConnect.__name__, err_code)

            self.logout_button.setEnabled(True)
            
            self.add_status_message('로그인 성공')
            self.do_automatic()

    def OnReceiveMsg(self, ScrNo, RQName, TrCode, Msg):
        logging.info('%s %s %s', self.OnReceiveMsg.__name__, RQName, Msg)

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

            self.add_status_message('{} 개의 종목이 추가 됨'.format(cnt))

        return


    def OnReceiveRealData(self, Code, RealType, RealData):
        logging.info('%s %s %s', self.OnReceiveRealData.__name__, Code, RealType)
        if RealType == "주식시세":
            item_name = self.GetMasterCodeName(Code)
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
            self.add_status_message('시세 변경 {}'.format(item_name))

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

                self.add_status_message('조건식 {} 가져옴'.format(name))

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

        arg['condiiton_name'] = strConditionName
        arg['condition_index'] = strConditionIndex

        if sType == "I":
            item_name = self.GetMasterCodeName(sCode)
            self.add_status_message('{} 종목 편입'.format(item_name))

            self.db.save_investmentitem(sCode, item_name, strConditionName) 
            while self.CommKwRqData(sCode, 0, 1, 0, "주식기본정보", self.getScrNum()) == -200:
                pass

            time.sleep(1)
            arg['item_price'] = self.db.select_investmentitem('item_price', 'item_code', sCode)
            arg['status'] = '1'

            logging.info('조건식 %s, %s 편입 %s', strConditionName, item_name, arg['item_price'])

        elif sType == "D":
            item_name = self.GetMasterCodeName(sCode)
            arg['item_price'] = self.db.select_investmentitem('item_price', 'item_code', sCode)

            logging.info('조건식 %s, %s 이탈 %s', strConditionName, item_name, arg['item_price'])

            self.add_status_message('{} 종목 이탈'.format(item_name))

            self.db.delete_investmentitem(sCode)

            arg['status'] = '0'


        arg['item_name'] = item_name
        push_request.send(arg)    

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(message)s', 
                        filename='openapi.log', 
                        filemode='w', 
                        level=logging.DEBUG,
                        datefmt='%Y/%m/%d %I:%M:%S %p')

    db = APIDatabase(host='localhost', 
                    user='',
                    password='',
                    db ='')  # CREATE DATABASE (DATABASE_NAME) DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

    app = QApplication(sys.argv)
    api = KiWoomApi(db)
    api.show()
    app.exec_()