import paho.mqtt.client as mqtt
import mysql.connector as sql
import time
import threading 
import multitasking
from PyCRC.CRC16 import CRC16

#mqtt broker address
broker_url = input("Enter IP : ")
group_input = input("Enter Group :")
#broker_url = "10.113.91.111" 
broker_port = 1883

mydb = sql.connect(host="localhost",
                   user="root",
                   passwd="tuan123",
                   database="pj_hbi")

listMachines = []

def getListMachine(group_number):
    try:
        global listMachines
        mydb = sql.connect(host="localhost",
                   user="root",
                   passwd="tuan123",
                   database="pj_hbi")
        sql_select_Query = "SELECT * FROM pj_hbi.layoutmay WHERE GROUPNo='%s'" % group_number
        cursor = mydb.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        print("Total number of rows is: ", cursor.rowcount)
        for row in records:
            listMachines.append(Machine(int(row[2]),int(row[0]),int(row[1]),int(row[3])))
        #del row
        #del records
    except sql.Error as error:
        print("Failed to get record from MySQL table: {}".format(error))


class Machine:
    def __init__(self, idmachine, group, line, operation):
        self.idMachine = idmachine
        self.group = group
        self.line = line
        self.operation = operation
        self.onConnect = False
        self.amoutOfProducts = None
        self.countTimeDown = 0

    def joinInMqtt(self):
        self.topicMainData = 'phubai2/realtimeproduction/topicMainData/%d' % (self.idMachine)
        self.topicCheckIDHR = 'phubai2/realtimeproduction/topicCheckIDHR/%d' % (self.idMachine)
        self.client = mqtt.Client(str(self.idMachine))
        self.client.connect(broker_url, broker_port)
        self.client.loop_start()

    def checkValidData(self,client, userdata, message):
        try:
            array_message = str(message.payload.decode())
            if len(array_message) != 41:
                raise Exception
            self.__crcChecksum = int(array_message[-5:])
            self.__crcChecksum_new = CRC16().calculate(str(array_message[:-5]))
            if (self.__crcChecksum_new != self.__crcChecksum):
                raise Exception
            self.__idMachine_mgs = int(array_message[1:11])
            if (self.__idMachine_mgs != self.idMachine):
                raise Exception
            self.__amoutOfProducts_mgs = int(array_message[31:36])
            if (self.amoutOfProducts == self.__amoutOfProducts_mgs):
                raise Exception
            self.__idhr_mgs = int(array_message[11:21])
            self.__wls_mgs = int(array_message[21:31])
            #print(" CheckValidData: OK")
            #print("ID: %d\nIDHR: %d\nWls: %d" %
            #(self.__idMachine_mgs,self.__idhr_mgs, self.__wls_mgs))
            self.amoutOfProducts = self.__amoutOfProducts_mgs
            self.countTimeDown = 0
            self.onConnect = True
            self.insertIntoMySQL()
        except Exception:
            pass
            #print(" CheckValidData: FAILED ID:%d GROUP:%d LINE:%s"
            #      %(self.idMachine, self.group, self.line))
        except:
            pass

    def insertIntoMySQL(self):
        try:
            cursor = mydb.cursor()
            insql = "insert into realtime (IDMay, IDHR, LOT, SLSP, OP) values (%s, %s, %s, %s, %s)"
            val = (self.idMachine, self.__idhr_mgs, self.__wls_mgs, self.__amoutOfProducts_mgs, self.operation)
            cursor.execute(insql, val)
            mydb.commit()
            print("      Machine: {}    AoP: {}".format(self.idMachine,self.__amoutOfProducts_mgs))

        except:
            pass
            #print(" Insert Into MySQL: Failed
            #Machine:{}".format(self.idMachine))

    def checkIdhr(self,client, userdata, message):
        pass

    def checkOnConnect(self):
        if self.countTimeDown > 300:
            self.onConnect = False

def threadTaskData(machine):
    machine.client.subscribe(machine.topicMainData)
    machine.client.message_callback_add(machine.topicMainData,machine.checkValidData)
    machine.countTimeDown +=1


@multitasking.task
def getData():
    while True:
        tasks = []
        for machine in listMachines:
            tasks.append(threading.Thread(target=threadTaskData, args=(machine,)))
            del machine
        for task in tasks:
            task.start()
            del task
        for task in tasks:
            task.join()
            del task
        del tasks

        
@multitasking.task
def checkOnConnect():
    while True:
        print('-----------------------------')
        for machine in listMachines:
            machine.checkOnConnect()
            if machine.onConnect == True:
                print('id: {}     stt: {}'.format(machine.idMachine, machine.onConnect))
        time.sleep(5)


@multitasking.task
def checkIDHR():
    pass


if __name__ == '__main__':
    getListMachine(group_input)
    for machine in listMachines:
        machine.joinInMqtt()
    getData()
    #checkOnConnect()
    #haha