import os.path
import time
from subprocess import PIPE, Popen
from threading import Thread

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer


class SparkExecutorLogsWatcher(Thread):
    """A thread that tails executor logs and writes them to stdout."""

    def __init__(self, log_dir: str = "/var/log/yarn") -> None:
        Thread.__init__(self)
        self.log_dir = log_dir

    def run(self) -> None:
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


class SparkExecutorLogsHandler(FileSystemEventHandler):  # type: ignore
    @staticmethod
    def on_created(event: FileSystemEvent) -> None:
        if event.is_directory:
            return None

        print(f"Handling create event for file: {event.src_path}")
        p1 = Popen(["tail", "-f", event.src_path], stdout=PIPE)
        Popen(["sed", f"s~^~[{event.src_path}] ~"], stdin=p1.stdout)
        if p1.stdout is not None:
            p1.stdout.close()
