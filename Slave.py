#!python
#coding=utf8
from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
import thread
import time
import ConfigParser
import pdb

class Slave:
    def __init__(self, ip, port, master_url):
        self.port   = port
        self.ip     = ip
        self.status = 0     #0是空闲，1是忙碌，2是异常，99是关闭
        self.master_url = master_url
        self.heart_beat_interval = 0
        self.server = SimpleXMLRPCServer((ip, self.port))
        self.server.register_multicall_functions()
        self.server.register_function(self.work, "work")
        self.server.register_function(self.stop, "stop")
        self.master_proxy = xmlrpclib.ServerProxy(self.master_url)
            
    def getProxy(self):
        return self.master_proxy
        
    def run(self):
        #pdb.set_trace()
        (my_worker_id, self.heart_beat_interval) = self.master_proxy.register(self.ip, self.port, self.status)
        print "My work_id is " + str(my_worker_id)
        thread.start_new_thread(self.send_heart_beat, ())
        return self.server.serve_forever()
    
    def stop(self):
        self.server.server_close()
        self.status = 99
        return 0
        
    def work(self, job_id, job_cmd, para_list):
        print "run job_" + str(job_id) + ":" + str(para_list)
        try:
            thread.start_new_thread(self.run_a_job, (job_id, job_cmd, para_list))
            self.status = 1
            self.master_proxy.update_status(self.ip, self.status)
        except:
            return -1
        return job_id
    
    def run_a_job(self, job_id, job_cmd, parameter):
        print "start, " + str(job_cmd) + ":" + str(parameter)
        
        time.sleep(30)
        self.status = 0
        print "finish, " + str(job_cmd) + ":" + str(parameter)
    
    def send_heart_beat(self):
        while self.status != 99:
            self.master_proxy.heart_beat(self.ip, self.status)
            #print "send heart_beat"
            time.sleep(self.heart_beat_interval)
        else:
            self.master_proxy.heart_beat(self.ip, self.status)
            #print "send heart_beat"
            time.sleep(self.heart_beat_interval)

def Main():
    config = ConfigParser.ConfigParser()
    config.read("conf.ini")
    ip = config.get("slave configuration", "ip address")
    port = int(config.get("slave configuration", "listening port"))
    master_url = config.get("slave configuration", "master url")
    print master_url
    slave = Slave(ip, port, master_url)
    slave.run()

Main()