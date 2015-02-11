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
WHISPER_BIN = 'whisper-fetch'


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
    """ Sender threads that connects to graphite servers and will send data to it. """

    def __init__(self,hostname='localhost',port=2001,batchsize=100,reconnect_interval=10000,max_enqueued=10000000):
        super(GraphiteSenderThread,self).__init__()
        #self.daemon = True
        self.queue = GraphiteQueue()                # queue mechanisms to send the stuffs.
        self.hostname = hostname                    # hostname to connect to
        self.port = port                            # port on which to send the data to.
        self.connection = ''                        # will be used to connect to the server
        self.counter = 0
        self.max_enqueued = max_enqueued
        self.can_kill = False
        self.batchsize = batchsize
        self.reconnect_interval = reconnect_interval

    def connect(self):
        """ connects to the host """
        TRIES = 0
        result = False
        while TRIES < 100 and result is not True:
            TRIES += 1
            LOG.info('[ %s ] Attempting connection to %s try#%d'%(self.name,self.hostname,TRIES))
            try:
                addresses = socket.getaddrinfo(self.hostname, self.port, socket.AF_UNSPEC, socket.SOCK_STREAM, 0)
            except socket.gaierror, e:
                # Don't croak on transient DNS resolution issues.
                if e[0] in (socket.EAI_AGAIN, socket.EAI_NONAME, socket.EAI_NODATA):
                    LOG.debug('[ %s ] DNS resolution failure: %s: %s', self.name, self.hostname, e)
                raise
            for family, socktype, proto, canonname, sockaddr in addresses:
                try:
                    self.connection = socket.socket(family, socktype, proto)
                    # pretty resilient timeout.
                    self.connection.settimeout(30)
                    self.connection.connect(sockaddr)
                    # WHOOOOO connected :-)
                    LOG.info('%s - Connection to %s was successful'%(self.name,str(sockaddr)))
                    result = True
                    break
                except socket.error, msg:
                    LOG.warning('[ %s ] Connection attempt failed to %s:%d: %s', self.name, self.hostname, self.port, msg)
            time.sleep(.2*TRIES)
        if not result:
            LOG.error('[ %s ] giving up on connection to %s; too many tries'%(self.name,self.hostname))
        return result

    def disconnect(self):
        """ I'm sure you can figure out what it does """
        try:
            if self.connection:
                self.connection.close()
        except:
            pass

    def can_enqueue(self):
        return (self.queue.qsize() < self.max_enqueued or self.max_enqueued < 0 )

    def enqueue(self, line):
        """ Adds metric line into the thread queue to be sent. """
        try:
            #LOG.debug("checking line: %s"%(line))
            the_line = string.split(string.strip(line),' ')[0:3]
            check_epoch_timestamp(the_line[2])
        except:
            LOG.error("[ %s ] %s - Could not enqueue metric line: %s"%(self.name,self.hostname,line))
            return False
        return self.queue.nput("%s\n"%(line))

    def kill(self):
        """ instructs the thread to terminate when it gets the chance. """
        LOG.debug("ordering %s to terminate"%(self.name))
        self.disconnect()
        self.can_kill = True

    def qsize(self):
        """ returns the queue size """
        return self.queue.qsize()

    def report(self):
        """ reports on the current thread state. """
        status = "ALIVE" if self.is_alive() else "DEAD"
        LOG.info("[ %s ] REPORT - %s - %s - sent: %d - queued: %d"%(self.name,status,self.hostname,self.counter,self.queue.qsize()))

    def send(self):
        """
        sends data across to the graphite server.
        respects the batch size for sending data.
        """
        content = ''
        for i in range(0,self.batchsize):
            if not self.queue.empty():
                content += self.queue.get()
                self.counter += 1
	retries = 0
        success = False
	while not success and retries < 10:
            try:
	        self.connection.sendall(content)
                success = True
            except:
                LOG.warning("[ %s ] Connection timed out, retrying"%self.name)
                retries += 1
                time.sleep(5)


    def run(self):
        """
        Main loop to send data over.
        Dequeues and sends over by batches.
        """
        while not self.can_kill:
            if self.counter%(self.reconnect_interval) < self.batchsize:
                LOG.debug("[ %s ] Reconnecting..."%self.name)
                self.disconnect()
                if not self.connect():
                    LOG.error("[ %s ] could not initiate connection to %s. Killing..."%(self.name,self.hostname))
                    self.kill()
                    break
            if not self.queue.empty():
                self.send()
            else:
                LOG.info('[ %s ] Nothing to do. waiting'%(self.name))
                time.sleep(1)
        LOG.info("[ %s ] terminating..."%self.name)


class WhisperReader:
    """ Reads whisper files (graphite content) and processes it. """
    def __init__(self,file='',root='',graphite_threads=[],before_timestamp=0,after_timestamp=0):
        """ constructor """
        self.file = file
        self.root = root
        self.metric_name = whisperToMetric(self.file)
        self.metrics = []
        self.graphite_threads = graphite_threads
        self.before_timestamp = before_timestamp
        self.after_timestamp = after_timestamp
        LOG.debug('metric file: %s\nmetric root: %s\nmetric name: %s'%(self.file,self.root,self.metric_name))

    def read(self):
        """ reads from the source file and populates the metric array """
        #LOG.info("Whisper Reading ource file: %s"%(os.path.join(self.root,self.file)))
        try:
            before_param = "" if self.before_timestamp == 0 else "--until=%s"%str(self.before_timestamp)
            after_param = "" if self.after_timestamp == 0 else "--from=%s"%str(self.after_timestamp)
	    LOG.debug(str([ WHISPER_BIN, after_param, before_param, os.path.join(self.root,self.file) ]))
            result = subprocess.Popen([ WHISPER_BIN, after_param, before_param, os.path.join(self.root,self.file) ], stdout=subprocess.PIPE)
            for line in result.stdout:
		linestrip = string.strip(line)
                temp_timestamp,temp_value = linestrip.split('\t')
                if temp_value != "None":
                    self.metrics.append("%s %s %s"%(self.metric_name,temp_value,temp_timestamp))
            LOG.debug("Whisper Reading completed for file %s"%(os.path.join(self.root,self.file)))
            return True
        except:
            LOG.error("Whisper Reading failed for file %s"%(os.path.join(self.root,self.file)))
            return False

    def feed(self):
        """ sends all metrics into the graphite destinations. """
        for metric in self.metrics:
            for graphite_instance in self.graphite_threads:
                res = False
                tries = 10
                # we're trying until we're successful
                while not res and tries > 0:
                    try:
                        tries -= 1
                        res = graphite_instance.enqueue(metric)
                    except:
                        LOG.warning("Could not load metric -- %s -- in %s"%(metric,graphite_instance.name()))
                # we could not load the metric. Something is wrong. We need to stop.
                if not res:
                    LOG.error("Metric not loaded. STOPPING.")
                    return False
        #LOG.info("Successfully loaded metric %s"%(self.metric_name))
        return True




class TextReader:
    """ Reads classic test files (graphite content) and processes it. """
    def __init__(self,file='',root='',graphite_threads=[]):
        """ contructor """
        self.file = file
        self.root = root
        self.metric_name = whisperToMetric(self.file)
        self.metrics = []
        self.graphite_threads = graphite_threads
        LOG.debug('metric file: %s\nmetric root: %s\nmetric name: %s'%(self.file,self.root,self.metric_name))

    def read(self):
        """ reads from the source file and populates the metric array """
        #LOG.info("Reading source file: %s"%(os.path.join(self.root,self.file)))
        try:
            fd = open(os.path.join(self.root,self.file),'r')
            line = string.strip(fd.readline())
            while line:
                temp_timestamp,temp_value = line.split('\t')
                self.metrics.append("%s %s %s"%(self.metric_name,temp_value,temp_timestamp))
                line = string.strip(fd.readline())
            fd.close()
            LOG.debug("Reading completed for file %s"%(os.path.join(self.root,self.file)))
            return True
        except:
            LOG.error("Reading failed for file %s"%(os.path.join(self.root,self.file)))
            return False

    def feed(self):
        """ sends all metrics into the graphite destinations. """
        for metric in self.metrics:
            for graphite_instance in self.graphite_threads:
                res = False
                tries = 10
                # we're trying until we're successful
                while not res and tries > 0:
                    try:
                        tries -= 1
                        res = graphite_instance.enqueue(metric)
                    except:
                        LOG.warning("Could not load metric -- %s -- in %s"%(metric,graphite_instance.name()))
                # we could not load the metric. Something is wrong. We need to stop.
                if not res:
                    LOG.error("Metric not loaded. STOPPING.")
                    return False
        #LOG.info("Successfully loaded metric %s"%(self.metric_name))
        return True


class ControllerThread(threading.Thread):
    """ Prints regular reports on the graphite queues. """
    def __init__(self,graphite_threads=[]):
        super(ControllerThread,self).__init__()
        self.graphite_threads = graphite_threads
        self.can_kill = False
        self.txtfiles = []
        self.wspfiles = []

    def kill(self):
        self.can_kill = True

    def add_txtfile(self,txtfile):
        self.txtfiles.append(txtfile)

    def add_wspfile(self,wspfile):
        self.wspfiles.append(wspfile)

    def feed(self):
        if len(self.txtfiles) != 0:
            txtfile = self.txtfiles.pop()

    def run(self):
        if len(self.graphite_threads) == 0:
            return
        while not self.can_kill:
            can_enqueue = True
            for gthread in self.graphite_threads:
                gthread.report()
            time.sleep(2)
        LOG.info("[ %s ] terminating..."%self.name)


class FeederThread(threading.Thread):

    def __init__(self,path='',graphite_threads=[],before_timestamp=time.time(),after_timestamp=0):
        super(FeederThread,self).__init__()
        self.txtfiles = []
        self.wspfiles = []
        self.path = path
        self.can_kill = False
        self.graphite_threads = graphite_threads
        self.before_timestamp = before_timestamp
        self.after_timestamp = after_timestamp

    def add_txtfile(self,txtfile):
        self.txtfiles.append(txtfile)

    def add_wspfile(self,wspfile):
        self.wspfiles.append(wspfile)

    def kill(self):
        self.can_kill = True

    def status(self):
        return ((len(self.txtfiles) == 0) and (len(self.wspfiles) == 0))

    def run(self):
        # is there any work?
        LOG.info("[ %s ] starting feeder thread"%self.name)
        if len(self.graphite_threads) == 0:
            return

        while (not self.can_kill) or not self.status():

            # can we push stuffs through?
            for server in self.graphite_threads:
                timer = 1
                while not server.can_enqueue():
                    time.sleep(timer)
                    timer = max(timer*2,60)

            if len(self.txtfiles):
                txtfile = self.txtfiles.pop()
                txtObj = TextReader(file=txtfile,root=self.path,graphite_threads=self.graphite_threads)
                # if the feed failed, we stop and don't do anything
                if not txtObj.read() or not txtObj.feed():
                    LOG.error("[ %s ] %s NOT loaded #SHOULDRETRY"%(self.name,txtfile))
                else:
                    LOG.info("[ %s ] %s loaded up"%(self.name,txtfile))

            elif len(self.wspfiles):
                wspfile = self.wspfiles.pop()
                wspObj = WhisperReader(file=wspfile,root=self.path,graphite_threads=self.graphite_threads,before_timestamp=self.before_timestamp,after_timestamp=self.after_timestamp)
                # if the feed failed, we stop and don't do anything
                if not wspObj.read() or not wspObj.feed():
                    LOG.error("[ %s ] %s NOT loaded #SHOULDRETRY"%(self.name,wspfile))
                else:
                    LOG.info("[ %s ] %s loaded up"%(self.name,wspfile))
            time.sleep(1)

        if self.can_kill:
            LOG.info("[ %s ] terminating (Killed)..."%self.name)
        else:
            LOG.info("[ %s ] terminating..."%self.name)


def check_epoch_timestamp(i__timestamp):
    """
    validates that the timestamp is correctly interpreted.
    """
    time.gmtime(int(i__timestamp))


def setup_logging(level=logging.INFO,logfile=DEFAULT_LOG, max_bytes=None, backup_count=None):
    """Sets up logging and associated handlers."""
    LOG.setLevel(level)
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
    parser.usage = '%prog [options] [ -i FILE | -t ] graphite_destination1[:port1] [ graphite_destination2[:port2] [ ... ] ]'
    parser.add_option('-b', '--batch-size', dest='BATCHSIZE', metavar='BATCHSIZE',
                        type="int",
                        default=1000,
                        help='Batch size when sending data to Graphite')
    parser.add_option('-r', '--reconnect-interval', dest='RECONNECT_INTERVAL', metavar='RECONNECT_INTERVAL',
                        type="int",
                        default=100000,
                        help='Batch size when sending data to Graphite')
    parser.add_option('-m','--max-enqueued',dest='MAX_ENQUEUES',metavar='MAX_ENQUEUES',
                        type="int",
                        default=100000000,
                        help='Maximum queue size for the graphite threads')

    parser.add_option('-p','--path',dest='ABSOLUTE_PATH',metavar='ABSOLUTE_PATH',
                        default='/var/lib/carbon/whisper/',
                        help='absolute path where the whisper file relate from')
    parser.add_option('-f','--file',dest='INPUT_FILE',metavar='INPUT_FILE',
                        default='',
                        help='The input file that contains the list of all the whisper files to browse')

    parser.add_option('-s','--simulate',dest='TEST_GRAPHITE',action='store_true',
                        default=False,
                        help='Testing the remote graphite by sending dummy data')

    parser.add_option('-d','--debug',dest='DEBUG',action='store_true',
                        default=False,
                        help='Toggles DEBUG logging')

    parser.add_option('-E', '--before', dest='BEFORE_TIMESTAMP', metavar='BEFORE_TIMESTAMP',
                        type="int",
                        default=int(time.time()),
                        help='upper timestamp limit')
    parser.add_option('-B', '--after', dest='AFTER_TIMESTAMP', metavar='AFTER_TIMESTAMP',
                        type="int",
                        default=int(time.time()-(60*60*24*7)),           # defaults to 7 days ago
                        help='lower timestamp limit')

    (options, args) = parser.parse_args(args=argv[1:])

    if len(args) == 0:
        parser.error('No graphite destination server found')
    if not options.TEST_GRAPHITE and not options.INPUT_FILE:
        parser.error('No input file specified')

    return (options, args)

def simulate_data(i__metric_name,i__before,i__after):
    """ sends some rubbish in graphite """
    import random
    last_value = 0
    ret_data = []
    for i in range(i__after,i__before,60):
        last_value += random.randint(-5,8)
        ret_data.append("%s %d %d"%(i__metric_name,last_value,i))
    return ret_data

def whisperToMetric(i__whisper_filepath):
    """ converts files into metric names. """
    if i__whisper_filepath[-4:] in [ '.wsp', '.txt' ]:
        i__whisper_filepath = i__whisper_filepath[:-4]

    return '.'.join(string.split(i__whisper_filepath,'/'))

def main(i__argv):
    """ the main entry point"""

    options, graphite_destinations = parse_cmdline(i__argv)

    if options.DEBUG:
        setup_logging(level=logging.DEBUG)
    else:
        setup_logging()

    list_graphite_threads = []

    # spawning all the graphite sender threads
    for i in graphite_destinations:

        server = string.split(i,':')
        if len(server) == 1:
            list_graphite_threads.append(GraphiteSenderThread(hostname=i,batchsize=options.BATCHSIZE,reconnect_interval=options.RECONNECT_INTERVAL))
        if len(server) == 2:
            list_graphite_threads.append(GraphiteSenderThread(hostname=server[0],port=int(server[1]),batchsize=options.BATCHSIZE,reconnect_interval=options.RECONNECT_INTERVAL))

    # starting the threads!
    for thread in list_graphite_threads:
        thread.start()

    # starting the controller thread.
    controller = ControllerThread(list_graphite_threads)
    controller.start()


    # Are we in simulation mode?
    if options.TEST_GRAPHITE:
        LOG.info('[Simulation mode]')
        rubbish = simulate_data('graphite.migrate.data',options.BEFORE_TIMESTAMP,options.AFTER_TIMESTAMP)
        for i in list_graphite_threads:
            for j in rubbish:
                i.enqueue(j)
    else:
        LOG.info('[normal mode]')
        # starting the feeder thread
        feeder = FeederThread(graphite_threads=list_graphite_threads,path=options.ABSOLUTE_PATH,before_timestamp=options.BEFORE_TIMESTAMP,after_timestamp=options.AFTER_TIMESTAMP)
        feeder.start()
        list_files = { 'txt':[], 'wsp':[] }
        try:
            fd = open(options.INPUT_FILE,'r')
        except:
            LOG.error('Unable to read from input file %s'%(options.INPUT_FILE))
            sys.exit(5)
        for i in fd.readlines():
            file = string.strip(i)
            if os.path.isfile(os.path.join(options.ABSOLUTE_PATH,file)):
                if file[-4:] == '.wsp':
                    LOG.info("Adding file %s to the list"%(file))
                    feeder.add_wspfile(file)
                elif file[-4:] == '.txt':
                    LOG.info("Adding file %s to the list"%(file))
                    feeder.add_txtfile(file)
                else:
                    LOG.info("Discarding file %s"%(file))
            else:
                LOG.error('Unable to find file %s (from %s)'%(os.path.join(options.ABSOLUTE_PATH,file),options.INPUT_FILE))
        fd.close()

    while (not options.TEST_GRAPHITE and not feeder.status()):
	        time.sleep(5)

    STATUS=True
    while STATUS:
        STATUS=False
        for i in list_graphite_threads:
            if i.qsize() == 0:
                i.kill()
            STATUS = (STATUS or i.is_alive())
            time.sleep(1)
    if not options.TEST_GRAPHITE:
        feeder.kill()
    controller.kill()


# ______ MAIN _______

if __name__ == '__main__':
    sys.exit(main(sys.argv))


