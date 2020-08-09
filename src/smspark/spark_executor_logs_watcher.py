import os.path
import time
from subprocess import Popen, PIPE
from threading import Thread
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class SparkExecutorLogsWatcher(Thread):

    def __init__(self, log_dir="/var/log/yarn"):
        Thread.__init__(self)
        self.log_dir = log_dir

    def run(self):
        if not os.path.isdir(self.log_dir):
            os.makedirs(self.log_dir)

        print(f"Starting executor logs watcher on log_dir: {self.log_dir}")
        observer = Observer()
        event_handler = SparkExecutorLogsHandler()
        observer.schedule(event_handler, self.log_dir, recursive=True)
        observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            observer.stop()

        observer.join()


class SparkExecutorLogsHandler(FileSystemEventHandler):
    @staticmethod
    def on_created(event):
        if event.is_directory:
            return None

        print(f"Handling create event for file: {event.src_path}")
        p1 = Popen(["tail", "-f", event.src_path], stdout=PIPE)
        p2 = Popen(["sed", f"s~^~[{event.src_path}] ~"], stdin=p1.stdout)
        p1.stdout.close()

