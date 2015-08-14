#!python
#coding=utf8
from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
import string
import thread
import time

class Master:
    def __init__(self):
        self.port = 8859
        self.status = 0
        self.server = SimpleXMLRPCServer(("localhost", self.port))
        self.server.register_multicall_functions()
        self.server.register_function(self.add, 'add')
        self.server.register_function(self.register, 'register')
        self.server.register_function(self.heart_beat, 'heart_beat')
        self.slave_list = []
    
    # 测试函数
    def add(self, x, y):
        print "X : " + str(x) + " ; Y : " + str(y)
        return x + y
    
    # 心跳报文
    def heart_beat(self, ip, status):
        #print "hello world"
        print "HeartBeat: " + str(ip) + " is " + str(status)
        return 0
    
    # 注册新slave节点
    def register(self, url):
        proxy = xmlrpclib.ServerProxy(url)
        self.slave_list.append(proxy)
        return len(self.slave_list)
    
    # 启动master服务
    def run(self):
        print "Master is running, listening port 8859..."
        thread.start_new_thread(self.check_task_list, ())
        self.server.serve_forever()
    
    # 检查任务队列
    # todo：如队列不空，取出任务执行
    def check_task_list(self):
        while self.status != 99:
            if len(self.slave_list) > 0:
                task_id = self.slave_list[0].work("test_test_test")
                print "task_id is " + str(task_id)
                time.sleep(15)

def main():
    master = Master()
    master.run()
    
main()