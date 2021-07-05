import schedule
import time
import functools
import threading
import datetime
import logging
import queue
import configparser
import csv
import os
import stat
import constants
from execute_input import main



def schmain():

    os.chdir(constants.MAIN_FOLDER)
    
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # create a file handler
    handler = logging.FileHandler(os.path.join(constants.MAIN_FOLDER, constants.LOGS_FOLDER, constants.FILE_LOG_OUTPUT))
    handler.setLevel(logging.INFO)

    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)


    #LEVELS = {'debug': logging.DEBUG,
    #          'info': logging.INFO,
    #          'warning': logging.WARNING,
    #          'error': logging.ERROR,
    #          'critical': logging.CRITICAL}

    logger.info('Hello DBMaster')
    logger.info('Current working directory: ' + os.getcwd())

    # This decorator can be applied to
    def with_logging(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            print('here')
            print(*args)
            print(**kwargs)
            logger.info('Job "%s" completed' % func.__name__)
            result = func(*args, **kwargs)
            print(('LOG: Job "%s" completed' % func.__name__))
            print("\n\n")
            return result
        return wrapper

    def worker_main():
        while 1:
            job_func = jobqueue.get()
            job_func()
            jobqueue.task_done()

    jobqueue = queue.Queue()
    print(jobqueue)

    def run_threaded(func, parm):
        job_name = parm[0]

        if not os.path.exists(os.path.join(constants.MAIN_FOLDER, constants.INPUT_STATUS_FOLDER)):
            os.makedirs(os.path.join(constants.MAIN_FOLDER, constants.INPUT_STATUS_FOLDER))

        if os.path.exists(os.path.join(constants.MAIN_FOLDER, constants.INPUT_STATUS_FOLDER)+'/'+job_name+'.txt'):
            print('\nOne of the instance is currently active for this input. Skiping the execution of this thread.\n')
            logger.warning('%s : One of the instance is currently active for this input. Skiping the execution of this thread',job_name)
            return

        # with open("input_execution_status.csv", "r") as csv_file:
        #     reader = csv.reader(csv_file, delimiter=',')
        #     if os.stat("input_execution_status.csv").st_size == 0:
        #         threading.Thread(target=func, args=parm).start()
        #         logger.info('%s started',job_name)
        #     else:
        #         for row in reader:
        #             if(job_name == row[0] and row[1] == "1"):
        #                 print '\nOne of the instance is currently active for this input. Skiping the execution of this thread.\n'
        #                 logger.warning('%s : One of the instance is currently active for this input. Skiping the execution of this thread',job_name)
        #                 return
        #             if(job_name != row[0]):
        #                 threading.Thread(target=func, args=parm).start()
        #                 logger.info('%s : started',job_name)
        #     csv_file.close()
        threading.Thread(target=func, args=parm).start()


    @with_logging
    def job(message):
        print(("\n\n#########"  + message + " is running on thread %s" % threading.current_thread()))
        logger.info('%s is running on thread %s',message,threading.current_thread())
        print("\nTime Started: "+str(datetime.datetime.now()))
        main(message)

    ## args can be used as args = ("123", 6)  https://github.com/dbader/schedule/issues/65 ### Keep the "," at the end of args list
    #args = ("sample_input",)


    config = configparser.ConfigParser()
    logger.info('before reading inputs.ini')
    config.read(os.path.join(constants.MAIN_FOLDER, constants.CONFIG_FOLDER, constants.FILE_INPUTS_INI))
    logger.info('after reading inputs.ini')
    for each_section in config.sections():
        logger.info('in for loop of inputs.ini')
        print("############  Pulling Details of Input: "+each_section)

        cron = config.get(each_section,'cron')
        print("###########  Schedule Run:  "+cron)
        print("\n\n")
        #args = 'args = ("' + each_section + '",)'
        loc = {}
        exec('args = ("' + each_section + '",)', globals(), loc)
        print(loc['args'])
        #exec(args,globals())
        kw = {"func": job,"parm": loc['args'],}
        print(kw)
        #var = "schedule.every(2).seconds.do(run_threaded, **kw)"
        exec(cron)


    #schedule.every(10).minutes.do(job)
    #schedule.every(5).seconds.do(job)
    #schedule.every().hour.do(job)
    #schedule.every().day.at("10:30").do(job)
    #schedule.every().monday.do(job)
    #schedule.every().wednesday.at("13:15").do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    schmain()
