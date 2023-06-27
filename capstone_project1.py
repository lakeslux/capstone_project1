#install dependencies and libraries

pip install psycopg2

import psycopg2

import csv



#develop a function for connection to postgres

def get_db_connection(host, database, user, password) :
    conn = psycopg2.connect(host=host, database=database, user=user, password=password)
    
    return conn

host = "localhost"
database = "covid"
user = "postgres"
password = "imperial33"

conn = get_db_connection(host, database, user, password)




#create a cursor

cur = conn.cursor()



#test your connection

cur.execute ("SELECT * FROM covid_19_data")
header = cur.fetchall()
print(header)



#download the csv file

with open("C:/Users/HP/Downloads/covid_19_data.csv", "r") as file:
    next(file)
    data=[line.split(',') for line in file]
    
print(data)



#loading data into postgres

batch_size = 10

for i in range(0, len(data), batch_size):
    batch = data[i:i+batch_size]
    for instance in batch:
        cur.execute(f"INSERT INTO covid_19_data(serial_number, observation_date, state, country, last_updated, confirmed, deaths, recovered) VALUES ('{instance[0]}', '{instance[1]}', '{instance[2]}', '{instance[3]}', '{instance[4]}', '{instance[5]}', '{instance[6]}', '{instance[7]}')")
                    
conn.commit()