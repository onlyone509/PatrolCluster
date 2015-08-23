#!python
#coding=utf8
from SimpleXMLRPCServer import SimpleXMLRPCServer
import ConfigParser
import xmlrpclib
import string
import thread
import time
import Queue
from ClusterJob import ClusterJob
from DataDic import dic_slave_status
import pdb

class Master:
    # 构造函数
    def __init__(self, port, job_queue_size, heart_beat_interval):
        self.port = port
        self.status = 0
        self.server = SimpleXMLRPCServer(("localhost", self.port))
        self.server.register_multicall_functions()
        self.server.register_function(self.register, 'register')
        self.server.register_function(self.heart_beat, 'heart_beat')
        self.server.register_function(self.update_status, 'update_status')
        self.heart_beat_interval = heart_beat_interval
        self.slave_list = {}
        self.to_do_job_queue = Queue.Queue(job_queue_size)
        self.doing_job_list = []
    
    # 心跳报文
    def heart_beat(self, ip, status):
        #print "hello world"
        print "HeartBeat: " + str(ip) + " is " + dic_slave_status[status]
        self.slave_list[ip]["status"] = status        
        self.slave_list[ip]["last_live_time"] = time.time()
        return 0
    
    # 更新slave状态
    def update_status(self, ip, status):
        print "update status: " + str(ip) + " is " + dic_slave_status[status]
        self.slave_list[ip]["status"] = status
        self.slave_list[ip]["last_live_time"] = time.time()
        return 0
    
    # 注册新slave节点
    def register(self, ip, port, status):
        url = "http://%s:%i/" % (ip, port)
        proxy = xmlrpclib.ServerProxy(url)
        self.slave_list[ip] = {"proxy":proxy, "status":status}
        return (len(self.slave_list), self.heart_beat_interval)
    
    # 启动master服务
    def run(self):
        print "Master is running, listening port 8859..."
        thread.start_new_thread(self.check_job_list, ())
        self.server.serve_forever()
    
    # 停止master服务
    def stop(self):
        self.server.server_close()
        self.status = 99
        return 0
    
    def add_job(self, job):
        if not self.to_do_job_queue.full():
            # to-do: 写入数据库，持久存储
            create_time = time.time()
            job.create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(create_time))
            job.job_id = int(create_time)
            self.to_do_job_queue.put(job)
            return job.job_id
        else:
            return -1
    
    # 检查任务队列
    # todo：如队列不空，取出任务执行
    def check_job_list(self): 
        print "check_job_list is called: " + str(self.status)
        while self.status != 99:
            if not self.to_do_job_queue.empty():    # 如果待执行队列不为空
                print "queue is not empty"
                # 找到一个空闲的slave
                idle_slave = None
                for ip in self.slave_list.keys():
                    if self.slave_list[ip]["status"] == 0 and (time.time() - self.slave_list[ip]["last_live_time"]) < 3 * self.heart_beat_interval :
                        idle_slave = self.slave_list[ip]["proxy"]
                        break
                else:
                    continue
                print "working"
                # 从队列中取出一个作业，由空闲的slave运行
                job = self.to_do_job_queue.get()
                status = idle_slave.work(job.job_id, job.job_cmd, job.job_para_list)
                if status != -1:
                    # to-do: 更新数据库
                    job.job_status = 1
                    self.doing_job_list.append(job)
                    print "job_" + str(job.job_id) + "is running"
            else:           # 如果任务队列为空
                print "queue is empty"
                time.sleep(15)

def main():
    config = ConfigParser.ConfigParser()
    config.read("conf.ini")
    port = int(config.get("master configuration", "listening port"))
    job_queue_size = int(config.get("master configuration", "job queue size"))
    heart_beat_interval = int(config.get("master configuration", "heart beat interval"))
    master = Master(port, job_queue_size, heart_beat_interval)
    
    # 测试用
    job = ClusterJob('test', "test1", [])
    master.add_job(job)
    master.run()
    
main()