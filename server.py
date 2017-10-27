from os import listdir, remove
from os.path import isdir, join
from datetime import datetime, timedelta
from time import sleep
from paramiko import SSHClient, AutoAddPolicy
from scp import SCPClient
from schedule import Scheduler
from logging.handlers import RotatingFileHandler
import logging

from timing import YEAR, MONTH, DAY
from settings import DISTANCE_HOST, DISTANCE_HOST_PORT,\
                     DISTANCE_HOST_USERNAME, DISTANCE_HOST_PASSWORD,\
                     DISTANCE_HOST_PATH, ASTERISK_MONITORING_PATH,\
                     SAVE_LAST, SAVE_LAST_TIME, LOG_PATH, UPDATE_EVERY_DAYS


def build_logging():
    handler = RotatingFileHandler(LOG_PATH, mode='a', maxBytes=5*1024*1024,
                                  backupCount=2, encoding=None, delay=0)
    logging.basicConfig(format='%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s] %(message)s',
                        level=logging.DEBUG, handlers=[handler, ])

build_logging()

def get_embedded_folders(path):
    return list(filter(lambda dir_path: isdir(join(path, dir_path)),\
                       listdir(path)))

def _get_dir_structure(path):
    """
        mintoring dir structure:
            monitoring
            |
            |-year
            |   |-month
            |   |   |-day
            .............
        return {year: {month: [day, day, ...]}}
    """
    return {year: {
                month: get_embedded_folders(join(path, year, month))\
                    for month in get_embedded_folders(join(path, year))}\
            for year in get_embedded_folders(path)}

def _get_save_date(timing, accuracy):
    now_date = datetime.now()
    delta = timedelta(days=accuracy*timing)
    return now_date - delta

def _get_files_paths_less_data(path, structure, save_date):
    backup_files_paths = []
    dates = [(year, month, day) for year, month_structure in structure.items()\
                                 for month, days in month_structure.items()
                                 for day in days]
    for date in dates:
        if save_date > datetime(*[int(time) for time in date]):
            dir_path = join(path, *date)
            backup_files_paths.extend([join(dir_path, file_path)\
                                      for file_path in listdir(dir_path)])
    return backup_files_paths

def get_backup_files_paths(path, timing, accuracy):
    structure = _get_dir_structure(path)
    save_date = _get_save_date(timing, accuracy)
    return _get_files_paths_less_data(path, structure, save_date)

def connect(**settings):
    client = SSHClient()
    client.set_missing_host_key_policy(AutoAddPolicy)
    client.connect(**settings)
    return client

def _remove_files(files):
    for _file in files:
        remove(_file)

def run():
    ssh_client = connect(hostname=DISTANCE_HOST, port=DISTANCE_HOST_PORT,
                         username=DISTANCE_HOST_USERNAME,
                         password=DISTANCE_HOST_PASSWORD)
    with SCPClient(ssh_client.get_transport()) as scp_client:
        files = get_backup_files_paths(ASTERISK_MONITORING_PATH, SAVE_LAST,
                                       SAVE_LAST_TIME)
        logging.info('Start put files: {}.'.format(files))
        scp_client.put(files, remote_path=DISTANCE_HOST_PATH)
        logging.info('Finish put.')
        logging.info('Start remove files: {}.'.format(files))
        _remove_files(files)
        logging.info('Finish remove files.')

def _get_schedule_from_settings(days):
     schedule = Scheduler()
     schedule.every(days).days.do(run)
     return schedule

if __name__ == '__main__':
    schedule = _get_schedule_from_settings(UPDATE_EVERY_DAYS)
    logging.info('Service up with {}.'.format(schedule.jobs))
    while True:
        schedule.run_pending()
        sleep(1)
