#!python
#coding=utf8
from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
import thread
import time
import pdb

class Slave:
    def __init__(self):
        self.port   = 8860
        self.status = 0
        self.master_url = "http://localhost:8859/"
        self.heart_beat_interval = 5
        self.server = SimpleXMLRPCServer(("localhost", self.port))
        self.server.register_multicall_functions()
        self.server.register_function(self.work, "work")
        self.server.register_function(self.stop, "stop")
        self.master_proxy = xmlrpclib.ServerProxy(self.master_url)
            
    def getProxy(self):
        return self.master_proxy
        
    def run(self):
        #pdb.set_trace()
        my_worker_id = self.master_proxy.register("http://localhost:8860/")
        print "My work_id is " + str(my_worker_id)
        thread.start_new_thread(self.send_heart_beat, ())
        return self.server.serve_forever()
    
    def stop(self):
        self.server.server_close()
        self.status = 99
        return 0
        
    def work(self, parameter):
        task_id = 99999
        print "run task_" + str(task_id) + ":" + parameter
        thread.start_new_thread(self.run_a_task, (task_id, parameter))
        return task_id
    
    def run_a_task(self, task_id, parameter):
        print "start, task_" + str(task_id) + ":" + parameter
        time.sleep(10)
        print "finish, task_" + str(task_id) + ":" + parameter
    
    def send_heart_beat(self):
        while self.status != 99:
            self.master_proxy.heart_beat('192.168.1.1', 'busy')
            #print "send heart_beat"
            time.sleep(self.heart_beat_interval)
        
    def add(self, x, y):
        print "X : " + str(x) + " ; Y : " + str(y)
        return x + y

def Main():
    slave = Slave()    
    slave.run()

Main()