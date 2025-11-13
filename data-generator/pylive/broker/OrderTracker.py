import os, sys, csv, setproctitle, time
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

import multiprocessing
import config as cfg
from helpers import *

def init():
    ''' This runs in the context of the main process.
    '''
    PYPInfo('OrderTracker: init() start')
    PYPInfo('OrderTracker: init() end')

def start():
    ''' This runs in the context of the main process.
    '''
    PYPInfo('OrderTracker: start() start')
    PYPInfo('OrderTracker: start() end')

def stop():
    ''' This runs in the context of the main process.
    '''
    PYPInfo('OrderTracker: stop() start')
    PYPInfo('OrderTracker: stop() end')

def join():
    ''' This runs in the context of the main process.
    '''
    PYPInfo('OrderTracker: join() start')
    PYPInfo('OrderTracker: join() end')
