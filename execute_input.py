#import cx_Oracle
import csv
import pymssql
from os import getenv
import configparser
import re
import os
import logging
from datetime import datetime
import stat
import constants
import time

from crypto import decrypt

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



def main(input_name):
        
    config = configparser.ConfigParser()

    ## Reading the Inputs Configuration File
    ##input_name = 'sample_input'

    config.read(os.path.join(constants.MAIN_FOLDER, constants.CONFIG_FOLDER, constants.FILE_INPUTS_INI))

    connection = config.get(input_name, 'connection')
    interval = config.get(input_name, 'interval')
    timestamp_column = config.get(input_name, 'output.timestamp.column')
    query = config.get(input_name, 'query')
    input_type = config.get(input_name, 'input_type')
    timezone = config.get(input_name, 'TZ')
    time_based_rising_column = config.get(input_name, 'time_based_rising_column')

    ## Reading the Connections Config File

    config.read(os.path.join(constants.MAIN_FOLDER, constants.CONFIG_FOLDER, constants.FILE_CONNECTIONS_INI))

    server = config.get(connection, 'server')
    user = config.get(connection, 'user')
    password = decrypt(config.get(connection, 'password'))
    database = config.get(connection, 'database')


    if(input_type == 'tail'):
        
        ## Reading the rising column and related data
        config.read(os.path.join(constants.MAIN_FOLDER, constants.CONFIG_FOLDER, constants.FILE_INPUTS_INI))
        rising_column = config.get(input_name,'tail.rising.column')
        #config.read('metadata.cfg')
        #current_rising_value = config.get(input_name, 'rising_value')
        
        if(os.stat(os.path.join(constants.MAIN_FOLDER, constants.CHECKPOINT_FOLDER, constants.FILE_TAIL)).st_size) != '0':
            with open(os.path.join(constants.MAIN_FOLDER, constants.CHECKPOINT_FOLDER, constants.FILE_TAIL), 'r') as f:
                current_rising_value = '0'
                reader = csv.reader(f)               
                for rec in reader:
                    print(rec)
                    if rec:
                        if rec[0] == input_name:
                            current_rising_value = rec[2] 
        else:
           current_rising_value = '0'
                    
        print(current_rising_value)
        print('This')
        print(time_based_rising_column)
        if(time_based_rising_column == '1'):            
            current_rising_value = time.strftime('%Y-%m-%d %H:%M:%S.000', time.localtime(current_rising_value))
        else:
            current_rising_value = int(current_rising_value)
        print('raising column type')
        print(current_rising_value)
        print('printing details...')
        print(current_rising_value)
        print(type(current_rising_value))
        additional_string = rising_column + " > '" + current_rising_value + "'"
        m = re.search('{{(.+?)> \?}}', query)
        additional_string = m.group(1).replace("$rising_column$",additional_string)
        line = re.sub(r"{{.*}}",additional_string, query)
        query = line
        print(query)
    conn = pymssql.connect(server, user, password, database)  
    cursor = conn.cursor()

     #############   Creating the Status Folder and creating a file everytime input starts its execution

    if not os.path.exists(os.path.join(constants.MAIN_FOLDER, constants.INPUT_STATUS_FOLDER)):
        os.makedirs(os.path.join(constants.MAIN_FOLDER, constants.INPUT_STATUS_FOLDER))

    if not os.path.exists(os.path.join(constants.MAIN_FOLDER, constants.INPUT_STATUS_FOLDER)+'/'+input_name+'.txt'):
        open(os.path.join(constants.MAIN_FOLDER, constants.INPUT_STATUS_FOLDER)+'/'+input_name+'.txt', 'w').close()
    cursor.execute(query)
    rows_affected=cursor.rowcount
    
    logger.info('%s started execution with SQL: %s',input_name,query)
    logger.info('%s fetch records: %s',input_name,rows_affected)


    
    if not os.path.exists(os.path.join(constants.MAIN_FOLDER, constants.OUTPUT_STG_FOLDER)):
        os.makedirs(os.path.join(constants.MAIN_FOLDER, constants.OUTPUT_STG_FOLDER))

    if not os.path.exists(os.path.join(constants.MAIN_FOLDER, constants.OUTPUT_STG_FOLDER+"/"+input_name)):
        os.makedirs(os.path.join(constants.MAIN_FOLDER, constants.OUTPUT_STG_FOLDER+"/"+input_name))

    csv_file_dest = input_name + ".csv"

    #output = csv.writer(open(csv_file_dest,'wb'))

    with open(os.path.join(constants.MAIN_FOLDER, constants.OUTPUT_STG_FOLDER)+"/"+input_name+"/"+csv_file_dest, "w") as outfile:
        writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
        print(cursor.description)
        writer.writerow([i[0] for i in cursor.description])
        for row in cursor:
            row_list = []
            for i in row:
                if isinstance(i, datetime):
                    i_str = str(i.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                    row_list.append(i_str)
                elif isinstance(i, str):
                    i_str = i.replace("\r", "\\r").replace("\n", "\\n")
                    row_list.append(i_str)
                else:
                    row_list.append(i)
            row = tuple(row_list)
            #print(row)
            writer.writerow(row)
    cursor.close()
    conn.close()

    input = open(os.path.join(constants.MAIN_FOLDER, constants.OUTPUT_STG_FOLDER)+"/"+input_name+"/"+csv_file_dest, 'r')
    if not os.path.exists(os.path.join(constants.MAIN_FOLDER, constants.OUTPUT_PROD_FOLDER)+"/"+input_name):
        os.makedirs(os.path.join(constants.MAIN_FOLDER, constants.OUTPUT_PROD_FOLDER)+"/"+input_name)
    output = open(os.path.join(constants.MAIN_FOLDER, constants.OUTPUT_PROD_FOLDER)+"/"+input_name+"/"+input_name+"_out.csv", 'a',newline='')
    writer = csv.writer(output)
    reader = csv.reader(input)
    row_count = sum(1 for row in reader)
    print('Printing Row Counts')
    print(row_count)
    print(type(row_count))
    input.close()
    if(row_count > 1):   # Checking if the first csv file is having any data inside it or not
        print('writing....')
        input = open(os.path.join(constants.MAIN_FOLDER, constants.OUTPUT_STG_FOLDER)+"/"+input_name+"/"+csv_file_dest, 'r')
        reader = csv.reader(input)
        #next(reader)     # Skip header row
        for i,row in enumerate(reader):
            if row:
                #print('data printing..')
                #print(row)
                #print(i)
                if ( i == 0 ):
                    row.insert(0, 'DBM_TIMESTAMP')
                    row.insert(1, 'DBM_TIMEZONE')
                    #print row
                    timestamp_column_index = row.index(timestamp_column)
                    print(timestamp_column_index)
                    if (os.stat(os.path.join(constants.MAIN_FOLDER, constants.OUTPUT_PROD_FOLDER)+"/"+input_name+"/"+input_name+"_out.csv").st_size) == 0:
                        writer.writerow(row)
                else:
                    timestamp = row[timestamp_column_index - 2]
                    row.insert(0, timestamp)
                    row.insert(1, timezone)
                    #print(' new data printing..')
                    #print(row)
                    #print(i)
                    writer.writerow(row)

    input.close()
    output.close()
    
    if(input_type == 'tail'):        
        with open(os.path.join(constants.MAIN_FOLDER, constants.OUTPUT_PROD_FOLDER)+"/"+input_name+"/"+input_name+"_out.csv", 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                rising_column_index = row.index(rising_column)
                print(rising_column_index)
                break
        f.close()
        input = open(os.path.join(constants.MAIN_FOLDER, constants.OUTPUT_PROD_FOLDER)+"/"+input_name+"/"+input_name+"_out.csv", 'r')
        reader = csv.reader(input)
        #next(reader)     # Skip header row
        values = [0]
        for i,rec in enumerate(reader):
            if(i==0):
                continue
            #print("rec printing..")
            #print(rec)
            #print(rising_column_index)
            #print('This,,,')
            #print(rec[rising_column_index])
            if not rec[rising_column_index].isdigit():
                rising_value = time.mktime(datetime.strptime(rec[rising_column_index], "%Y-%m-%d %H:%M:%S.%f").timetuple())
                values.append(rising_value)
                #print(rising_value)
            if rec[rising_column_index].isdigit():
                values.append(rec[rising_column_index])
                #print values
        print('Values..')
        #print(values)
        answer = max(values) # Need to take the column index/column name from the inputs config file
        print(answer)
        input.close()

        ############  Creating Metadata config file to store the latest raising column value of inputs

        with open(os.path.join(constants.MAIN_FOLDER, constants.CHECKPOINT_FOLDER, constants.FILE_TAIL), "a",newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            data = [input_name,rising_column,answer]
            writer.writerow(data)
        csv_file.close()

    if os.path.exists(os.path.join(constants.MAIN_FOLDER, constants.INPUT_STATUS_FOLDER)+"/" + input_name + '.txt'):
        os.remove(os.path.join(constants.MAIN_FOLDER, constants.INPUT_STATUS_FOLDER)+"/" + input_name + '.txt')

    ###################### Finish the Execution of the Input and marking the status 0 in status file #############


if __name__ == '__main__':
    main()
