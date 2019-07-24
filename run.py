import os
import threading
import time
import logging.config
import schedule

from utils import main


def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()


def run():
    schedule.every().day.at("02:00").do(run_threaded, main)

    while True:
        logger.info(schedule.jobs)
        schedule.run_pending()
        time.sleep(300)


logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "simple": {
            "format": "[%(levelname)1.1s %(asctime)s|%(module)s|%(funcName)s|%(lineno)d] %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        }
    },
    "handlers": {
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": 'simple',
            "stream": "ext://sys.stdout"
        },
        "main_file_log": {
            "level": "DEBUG",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filename": os.path.join(os.getcwd(), "logs/main.log"),
            "formatter": "simple",
            "when": "D",
            "backupCount": 5
        },
    },
    "loggers": {
        "main_log": {
            "level": "DEBUG",
            "handlers": ["console", "main_file_log"]
        },
    }
})


logger = logging.getLogger("main_log")