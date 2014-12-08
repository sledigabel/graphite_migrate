#!/bin/env python

# graphite_migrate.py

"""
Migration tool from a graphite cluster to another.
Replays all valid metrics and migrates them to one or several cluster.
"""

# by Seb Le Digabel <sledigabel@gmail.com>

# ______ IMPORT _______

import logging
import re
import os
import socket
import string
import sys
import subprocess
import threading
import time
from Queue import Queue
from Queue import Empty
from Queue import Full
from optparse import OptionParser


# ______ DEFAULT ______
ALIVE = True
##DEFAULT_LOG = '/var/log/graphite_migrate.log'
DEFAULT_LOG = '/Users/sledigab/lab/graphite_migrate.log'
LOG = logging.getLogger('graphite_migrate')


class GraphiteQueue(Queue):
    """ dedicated queue for designated graphite servers. """

    def nput(self, value):
        """A nonblocking put, that simply logs and discards the value when the
           queue is full, and returns false if we dropped."""
        try:
            self.put(value, False)
        except Full:
            LOG.error("DROPPED LINE: %s", value)
            return False
        return True 

class GraphiteSenderThread(threading.Thread):
    """  """

    def __init__(self,hostname='localhost',port=2001,batchsize=100,reconnect_interval=10000):
        super(GraphiteSenderThread,self).__init__()
        #self.daemon = True
        self.queue = GraphiteQueue()                # queue mechanisms to send the stuffs.
        self.hostname = hostname                    # hostname to connect to
        self.port = port                            # port on which to send the data to.
        self.connection = ''                        # will be used to connect to the server
        self.counter = 0
        self.can_kill = False
        self.batchsize = batchsize
        self.reconnect_interval = reconnect_interval
    
    def connect(self):
        """ connects to the host """
        TRIES = 0
        result = False
        while TRIES < 100 and result is not True:
            TRIES += 1
            LOG.info('%s - Attempting connection to %s try#%d'%(self.name,self.hostname,TRIES))
            try:
                addresses = socket.getaddrinfo(self.hostname, self.port, socket.AF_UNSPEC, socket.SOCK_STREAM, 0)
            except socket.gaierror, e:
                # Don't croak on transient DNS resolution issues.
                if e[0] in (socket.EAI_AGAIN, socket.EAI_NONAME, socket.EAI_NODATA):
                    LOG.debug('DNS resolution failure: %s: %s', self.hostname, e)
                raise
            for family, socktype, proto, canonname, sockaddr in addresses:
                try:
                    self.connection = socket.socket(family, socktype, proto)
                    # pretty resilient timeout.
                    self.connection.settimeout(15)
                    self.connection.connect(sockaddr)
                    # WHOOOOO connected :-)
                    LOG.debug('%s - Connection to %s was successful'%(self.name,str(sockaddr)))
                    result = True
                    break
                except socket.error, msg:
                    LOG.debug('%s - Connection attempt failed to %s:%d: %s', self.name, self.hostname, self.port, msg)
            time.sleep(.1*TRIES)
        if not result:
            LOG.error('%s - giving up on connection to %s; too many tries'%(self.name,self.hostname))   
        return result

    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
        except:
            pass

    def enqueue(self, line):
        """ """
        try:
            #LOG.debug("checking line: %s"%(line))
            the_line = string.split(string.strip(line),' ')[0:3]
            check_epoch_timestamp(the_line[2])
        except:
            LOG.error("%s(%s) - Could not enqueue metric line: %s"%(self.name,self.hostname,line))
            return False
        return self.queue.nput("%s\n"%(line))

    def kill(self):
        LOG.debug("ordering %s to terminate"%(self.getName()))
        self.disconnect()
        self.can_kill = True

    def qsize(self):
        return self.queue.qsize()

    def report(self):
        return "Thread %s - %s - sent: %d - queued: %d"%(self.getName(),self.hostname,self.counter,self.queue.qsize())
    
    def send(self):
        content = ''
        for i in range(0,self.batchsize):
            if not self.queue.empty():
                content += self.queue.get()
                self.counter += 1
        self.connection.sendall(content)
    
    def run(self):
        """
        Main loop to send data over.
        Dequeues and sends over by batches.
        """
        while not self.can_kill:
            if self.counter%(self.reconnect_interval) < self.batchsize:
                self.disconnect()
                if not self.connect():
                    LOG.error("%s - could not initiate connection to %s. Killing..."%(self.name,self.hostname))
                    self.kill()
                    break
            if not self.queue.empty():
                self.send()
            else:
                LOG.info('%s - Nothing to do. waiting'%(self.name))
                time.sleep(1)

            
        

        


# 
# class WhisperReaderThread(threading.Thread):
#     """ Will feed a whisper file content into  """
# 
#     def __init__(self,i__whisperfile_list,i__graphite_threads=[]):
#         """ """
#         super(WhisperReaderThread, self).__init__()
#         self.whisperfile = i__whisperfile
#         self.list_threads = i__graphite_threads
# 
#     def readWhisperFile(self,):
#         """ returns a list of metrics (filtered) """
#         pass
# 
#     def run(self):
#         """ runs the whisper file and feeds its content in the graphte threads queues """
#         pass  
# 




# def ControllerThread(threading.Thread):
#    """ Controls the data coming in, the graphite destinations and the """
    


def check_epoch_timestamp(i__timestamp):
    time.gmtime(int(i__timestamp))
    

def setup_logging(logfile=DEFAULT_LOG, max_bytes=None, backup_count=None):
    """Sets up logging and associated handlers."""

#    LOG.setLevel(logging.INFO)
    LOG.setLevel(logging.DEBUG)
    if backup_count is not None and max_bytes is not None:
        assert backup_count > 0
        assert max_bytes > 0
        ch = RotatingFileHandler(logfile, 'a', max_bytes, backup_count)
    else:  # Setup stream handler.
        ch = logging.StreamHandler(sys.stdout)

    ch.setFormatter(logging.Formatter('%(asctime)s %(name)s[%(process)d] %(levelname)s: %(message)s'))
    LOG.addHandler(ch)


def parse_cmdline(argv):
    """Parses the command-line."""

    # get arguments
    parser = OptionParser(description='Manages graphite migration')
    parser.add_option('-b', '--batch-size', dest='BATCHSIZE', metavar='BATCHSIZE',
                      default=1000,
                      help='Batch size when sending data to Graphite')
    parser.add_option('-r', '--reconnect-interval', dest='RECONNECT_INTERVAL', metavar='RECONNECT_INTERVAL',
                      default=100000,
                      help='Batch size when sending data to Graphite')
    
    (options, args) = parser.parse_args(args=argv[1:])
    
    return (options, args)

def simulate_data(i__metric_name):
    """ sends some rubbish in graphite """
    import random
    last_value = 0
    ret_data = []
    i__timestamp2 = int(time.time())
    i__timestamp1 = i__timestamp2-(2*60*60*24)
    for i in range(i__timestamp1,i__timestamp2,60):
        last_value += random.randint(-5,8)
        ret_data.append("%s %d %d"%(i__metric_name,last_value,i))
    return ret_data


def main(i__argv):
    """ the main entry point"""

    options, graphite_destinations = parse_cmdline(i__argv)
    BATCHSIZE = options.BATCHSIZE

    setup_logging()
    
    list_graphite_threads = []
    # spawning all the graphite sender threads
    for i in graphite_destinations:
        
        server = string.split(i,':')
        if len(server) == 1:
            list_graphite_threads.append(GraphiteSenderThread(hostname=i,batchsize=int(options.BATCHSIZE),reconnect_interval=int(options.RECONNECT_INTERVAL)))
        if len(server) == 2:
            list_graphite_threads.append(GraphiteSenderThread(hostname=server[0],port=int(server[1]),batchsize=int(options.BATCHSIZE),reconnect_interval=int(options.RECONNECT_INTERVAL)))

    # simulate stuffs
    rubbish = simulate_data('blah.test2')

    for i in list_graphite_threads:
        for j in rubbish:
            i.enqueue(j)

    for i in list_graphite_threads:
        print i.report()
    for i in list_graphite_threads:
        i.start()
    STATUS=True
    while STATUS:
        STATUS=False
        for i in list_graphite_threads:
            if i.qsize() == 0:
                i.kill()
            STATUS = STATUS or i.is_alive()
            time.sleep(1)
    for i in list_graphite_threads:
        i.join()
    
    

# ______ MAIN _______

if __name__ == '__main__':
    sys.exit(main(sys.argv))



