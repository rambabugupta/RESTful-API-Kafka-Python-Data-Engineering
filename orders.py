import psycopg2
import requests
import json
import time
import datetime;
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError

zip_url = 'https://www.zipcodeapi.com/rest/lx7PkvTYlRycpK0XQu17HaPlwUw2xDQTbXIvsCtOt20TN1QEwSLd2Byzv4C3IsWT'; 

#Definition of producer in Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

#Connection with postgres
try:
    conn = psycopg2.connect(database = "postgres", user = "postgres", host = "localhost", password = "postgres")
    conn.autocommit = True
except Exception as e:
    print ("Error in db connection", e)

#Extracting zipcode from table
def extract_table_info_using_psql():
    cur = conn.cursor()
    query = "select * from DWH.orders";
    cur.execute(query);
    row = cur.fetchone();
    while row is not None:
        zip_code = row[3]
        zip_response = get_city_name_using_zip_code(row, zip_code);
        if zip_response is None:
            row = cur.fetchone()
            continue;
        update_dwh_orders(row, zip_response);
        row = cur.fetchone()
    conn.commit();

#Fetching city name from zipcoode API using zipcode values from table 
#Assumed that if ZIP CODE is invalid then city would be "others".
#if API fails to respond or it return city as "Classified" then pushed to Kafka.
def get_city_name_using_zip_code(row, zip_code):
    units = "degrees";
    res_format = 'json';
    URL  = zip_url + '/info.' + res_format + '/' + zip_code + '/'+units;
    PARAMS = {};
    try:
        r = requests.get(url = URL, params = PARAMS);
        response = r.json();
        if 'city' in response and response['city'] == 'Classified':
            print ('Api not disclosing the city for order_id', row[0], 'pushing into failed_job topic');
            push_to_kafka(row, zip_code);
            return None;
        elif  'error_code' in response and response['error_msg'] == 'Invalid request.':
            print ('Invalid PIN, Setting city to others for order_id', row[0]);
            return response;
        elif 'error_code' in response:
            print ('Error in Api', response, 'for order_id', row[0], 'pushing into failed_job topic');
            push_to_kafka(row, zip_code);
            return None;
        else:
            return response;

    except Exception as e:
        print ('Error in Request for order_id', row[0],'pushing into failed_job topic');
        push_to_kafka(row, zip_code);
        return None;

#Pushed in topic using producer in Kafka
def push_to_kafka(row, zip_code):
    failed_message = {"row_info": row, "zip_code": zip_code }
    failed_message = str(failed_message);
    time.sleep(2); # 1 second lap
    producer.send('failed_job', failed_message)


#Updating table with city name
def update_dwh_orders(row, zip_response):
    if 'city' not in zip_response:
        city = 'others'
    else:
        city =  zip_response['city']
    
    query = """INSERT INTO DWH.dwh_orders (order_id, delivery_date, user_id, zipcode, city, total, item_count) 
        VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (order_id) DO UPDATE SET city = %s """

    cursor = conn.cursor();
    try:
    	cursor.execute(query, (row[0], row[1], row[2], row[3], city, row[4], row[5], city));
    except Exception as e:
    	print ('Error in SQL execution', e);
    	return;

    print ("sql executed successfully for order_id", row[0]);


#function defition for executing failed job
def execute_failed_job(row, zip_code):
    zip_response = get_city_name_using_zip_code(row, zip_code);
    if zip_response is None:
    	return 0;
    update_dwh_orders(row, zip_response);


extract_table_info_using_psql();

#Creating consumer in Kafka
failed_job_consumer = KafkaConsumer('failed_job', group_id='my-group', bootstrap_servers=['localhost:9092'])

#Taking message from topic in Kafka and calling the function execute_failed_job
for message in failed_job_consumer:
    message_value = message.value;
    try:
    	message_value = eval(message_value);
    except Exception as e:
    	print ('message format wrong', e, message_value);
    	continue;
    
    if 'row_info' not in message_value:
    	print ('row_info missing from the message', message_value);
    	continue;
    if 'zip_code' not in message_value:
    	print ('zip_code missing from the message', message_value);
    	continue;
    
    row_info = message_value['row_info'];
    zip_code = message_value['zip_code'];

    if type(row_info) is not tuple:
    	print ('Error in row_info type, it must be tuple', row_info)
    	continue;

    if len(row_info) < 6:
    	print ('values missing from row_info', row_info);
    	continue;

    if type(zip_code) is not str:
    	print ('Error in zip_code type, it must be string', zip_code);
    	continue;

    print ('Executing failed job for order_id', row_info[0]);
    execute_failed_job(row_info, zip_code);





