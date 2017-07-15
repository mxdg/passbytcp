#!/usr/bin/python3
# -*- coding: utf-8 -*-

import threading
import time
import logging

import db_transfer

class MainThread(threading.Thread):
    def __init__(self, obj):
        threading.Thread.__init__(self)
        self.obj = obj

    def run(self):
        self.obj.thread_db(self.obj)

    def stop(self):
        self.obj.thread_db_stop()

def main():
    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(levelname)s %(asctime)s] %(message)s',
    )


    thread = MainThread(db_transfer.Dbv3Transfer)
    thread.start()

    try:
        while thread.is_alive():
            time.sleep(10)
    except (KeyboardInterrupt, IOError, OSError) as e:
        import traceback
        traceback.print_exc()
        thread.stop()

if __name__ == '__main__':
    main()
