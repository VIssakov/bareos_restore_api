#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Скрипт для автоматического запуска восстановления данных из бэкапа.
Для работы скрипта нужен модуль python3-bareos и sslpsk
'''

import sys
import re
import json
import os
import argparse
import logging
import time
import bareos.bsock

class Config(object):
    def __init__(self):
        self.bareos_host = 'localhost'
        self.bareos_port = 9101
        self.bareos_pass = 'ABC'


def set_logging():
    log_path = '/var/log/bareos_restore'
    log_filename = 'bareos_restore_api.log'
    console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # initialization logging
    try:
        os.makedirs(log_path)
    except OSError:
        pass

    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger(__name__)

    # Create handlers
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(os.path.join(log_path, log_filename))
    console_handler.setLevel(logging.INFO)
    file_handler.setLevel(logging.WARN)

    # Create formatters and add it to handlers
    console_handler.setFormatter(console_format)
    file_handler.setFormatter(file_format)

    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


def bareos_connect(bareos_host, bareos_port, bareos_pass):
    logger.info("ESTABLISH CONNECT TO BAREOS")
    try:
        password=bareos.bsock.Password(dir_pass)
        directorconsole=bareos.bsock.DirectorConsoleJson(address=bareos_host, 
                                                         port=bareos_port, 
                                                         password=bareos_pass)
    except:
        logger.error(f"WE CAN NOT CONNECT TO BAREOS DIRECTOR: {directorconsole}"))

    return directorconsole


def check_client_exist(src, dest):
    result = {}
    # check src client exist
    status_src_client_json = directorconsole.call(f"status client={src}")
    if 'error' in status_src_client_json:
        result[src] = 'ERR'
    else:
        result[src] = 'OK'
    # check dest client exist
    status_dest_client_json = directorconsole.call(f"status client={dest}")
    if 'error' in status_dest_client_json:
        result[dest] = 'ERR'
    else:
        result[dest] = 'OK'
    return result


def get_client_jobs(src):
    try:
        client_all_jobs = directorconsole.call(f"list jobs client={src}")
        logger.info(f"client {src}, has next jobs {client_all_jobs["jobs"]}:")
    except KeyError:
        logger.error(f"client {src} has not backups, check it manualy")
        client_all_jobs = 'ERR'
    return client_all_jobs


def get_all_client_related_jobs(job_id):
    print(job_id)
    all_client_related_jobs = []
    all_client_related_jobs_json = directorconsole.call(f".bvfs_get_jobids jobid={job_id} [all]")
    logger.info("all jobs id:", all_client_related_jobs_json)

    for key in all_client_related_jobs_json["jobids"]:
        all_client_related_jobs.append(key["id"])
    return all_client_related_jobs


def get_data_id(all_client_related_jobs, sdata_element_type, sdata_element):
    data_id = ''
    try:
        if sdata_element_type == 'dir':
            get_dir_id_json = directorconsole.call(f".bvfs_lsdirs jobid={all_client_related_jobs} path={sdata_element}")
            data_id = get_dir_id_json["directories"][0]["pathid"]

        if sdata_element_type == 'file':
            sdata_element_list = sdata_element.split('/')
            dir_name = ("/".join(sdata_element_list[:-1]) + '/')
            logger.info("dirname for file %s is %s:", sdata_element, dir_name)
            file_name = sdata_element_list[-1]
            logger.info("filename for file %s is %s:", sdata_element, file_name)
            get_file_id_json = directorconsole.call(f".bvfs_lsfiles jobid={all_client_related_jobs} path={dir_name}")
            logger.info(f"file id is {get_file_id_json}:")
            for job_file in get_file_id_json["files"]:
                if file_name == job_file["name"]:
                    data_id = job_file["fileid"]
                else:
                    logger.error(f"file {sdata_element} is not exist in backup:")
    except KeyError:
        logger.error(f"data {sdata_element} is not exist in backup")
    return data_id


def restore( sclient, sdata, dclient, dpath ):

    sql_table_name = "b200001"
    result = []
    sdata_id_list = {}

    #проверяем есть ли клиенты в bareos
    check_client = check_client_exist(sclient, dclient)
    if (check_client[sclient] == 'ERROR'):
        logger.error(f"client {sclient} does not exist or director can not connect to client:")
        exit(1)
    if (check_client[dclient] == 'ERROR'):
        logger.error(f"client {dclient} does not exist or director can not connect to client:")
        exit(1)

    #начинаем восстановление файлов и папок рекурсивно.
    for sdata_element in sdata.split(','):
        #определяем что восстанавливаем
        last_char = sdata_element[-1]
        if last_char is "/":
            sdata_element_type = 'dir'
        else:
            sdata_element_type = 'file'

        logger.info(f"restore {sdata_element_type} from {sclient}:{sdata_element} to {dclient}:{dpath}")

        #получаем последний бэкап клиента
        last_client_job_id = get_client_jobs(sclient)
        last_client_job_id = last_client_job_id["jobs"][-1]["jobid"]
        logger.info(f"last jobid: {last_client_job_id}")

        #Получаем все связанные бэкапы с последним.
        all_client_related_jobs = get_all_client_related_jobs(last_client_job_id)
        all_client_related_jobs = ','.join(all_client_related_jobs)
        logger.info(f"all client jobs id: {all_client_related_jobs}")

        #Обновляем кеш
        update_client_related_jobs_cache = directorconsole.call(f".bvfs_update jobid={all_client_related_jobs}")

        # Получаем ID элемента которого хотим восстановить
        sdata_id = get_data_id(all_client_related_jobs, sdata_element_type, sdata_element)

        # собираем небольшой json со всеми типами бэкапов
        if (sdata_id and sdata_element_type == 'dir'):
            if "dirid" not in sdata_id_list:
                sdata_id_list["dirid"] = []
            sdata_id_list["dirid"].append(sdata_id)
        if (sdata_id and sdata_element_type == 'file'):
            if "fileid" not in sdata_id_list:
                sdata_id_list["fileid"] = []
            sdata_id_list["fileid"].append(sdata_id)

    # если ничего не нашли, выходим
    if( len(sdata_id_list) == 0 ):
        logger.error("nothing to restore, exit")
        exit(1)

    # запускаем восстановление, для файлов и папок будут разные джобы
    logger.info(f"work with json: {sdata_id_list}")
    for key, value in sdata_id_list.items():
        sdata_ids = (key + '=' + ','.join(str(element) for element in value))
        logger.info(f"restore command ids is: {sdata_ids}")

        # Очищаем табличку с файлами
        delete_sql_with_files = directorconsole.call(f".bvfs_cleanup path={sql_table_name}")
        logger.info(f"delete old sql table {sql_table_name}: {delete_sql_with_files}")

        # Создаем табличку с файлами
        create_sql_with_files = directorconsole.call(f".bvfs_restore jobid={all_client_related_jobs} {sdata_ids} path={sql_table_name}")
        logger.info(f"create sql table status: {create_sql_with_files}")

        # Запускаем восстановление
        run_restore = directorconsole.call(f"restore client={sclient} jobid={all_client_related_jobs} file=?{sql_table_name} restoreclient={dclient} restorejob=BackupRestore where={dpath} select current done yes")
        logger.info(f"job restore id: {run_restore}", )

        # получаем ID джобы
        job_id = run_restore["run"]["jobid"]

        # Проверяем статус бэкапа, пока идет спим.
        restore_status = job_status(job_id)
        while restore_status in ["R", "C"]:
            logger.info(f"job {job_id} is running: {restore_status}")
            time.sleep(300)
            restore_status = job_status(job_id)
        result.append(f"{sclient}_{sdata_element}_{dclient}_{dpath}_{restore_status}")

    return result


def job_status(job_id):
    job_status = ''
    get_job_status = directorconsole.call(f"list jobid={job_id}")

    try:
        job_status = get_job_status["jobs"][0]["jobstatus"]
    except KeyError:
        logger.error(f"job {job_id} is not found")
        job_status = 'NULL'
    return job_status


if __name__ == '__main__':

    config = Config() 
    logger = set_logging()

    directorconsole = bareos_connect(config.bareos_host, config.bareos_port, config.bareos_pass)

    parser = argparse.ArgumentParser()
    parser.add_argument("--restore", help="restore data", action="store_true")
    parser.add_argument("--sclient", help="src client", metavar="src client name")
    parser.add_argument("--sdata", help="what restore", metavar="what restore")
    parser.add_argument("--dclient", help="dest client", metavar="where restore")
    parser.add_argument("--dpath", help="dest path", metavar="dest dir")

    args = parser.parse_args()

    if args.restore and args.sclient and args.sdata and args.dclient and args.dpath:
        print( restore( args.sclient, args.sdata, args.dclient, args.dpath ) )

    exit(0)