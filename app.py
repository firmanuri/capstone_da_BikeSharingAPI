from flask import Flask, request
import sqlite3
import requests
from tqdm import tqdm

import json 
import numpy as np
import pandas as pd

app = Flask(__name__) 

@app.route('/')
@app.route('/homepage')
def home():
    return 'Hello World'

#######################################################################################################################

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

#######################################################################################################################

@app.route('/stations/<stations_id>')
def route_stations_id(stations_id):
    conn = make_connection()
    stations = get_stations_id(stations_id, conn)
    return stations.to_json()

def get_stations_id(stations_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {stations_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

#######################################################################################################################

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

#######################################################################################################################

@app.route('/trips/<trips_id>')
def route_trips_id(trips_id):
    conn = make_connection()
    trips = get_trips_id(trips_id, conn)
    return trips.to_json()

def get_trips_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

#######################################################################################################################

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

#######################################################################################################################

@app.route('/trips/add', methods=['POST']) 
def route_add_trips():
     
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

#######################################################################################################################

@app.route('/json') 
def json_example():
    
    req = request.get_json(force=True) # Parse the incoming json data as Dictionary
    
    name = req['name']
    age = req['age']
    address = req['address']
    
    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

#######################################################################################################################

@app.route('/trips/sum_depatures/')
def route_sum():
    conn = make_connection()
    sum = get_sum(conn)
    return sum.to_json()

def get_sum(conn):
    query = f"""
            select 
            count(start_station_id) as jumlah_keberangkatan
            from trips
            """
    result = pd.read_sql_query(query, conn)
    return result 

#######################################################################################################################

@app.route('/trips/sum_depatures/<start_station_id>')
def route_start_station_id(start_station_id):
    conn = make_connection()
    sum = get_start_station_id(start_station_id, conn)
    return sum.to_json()

def get_start_station_id(start_station_id, conn):
    query = f"""
            select 
            start_station_id as id_stasiun,
            start_station_name as nama_stasiun,
            count(start_station_id) as jumlah_keberangkatan,
            (CASE
            when count(start_station_id) <= 693 then 'Stasiun lengang'
            when count(start_station_id) <= 8227 then 'Stasiun normal'
            else 'Stasiun ramai'
	        end) as Kepadatan_pengunjung
            from trips
            where id_stasiun = {start_station_id}
            """
    result = pd.read_sql_query(query, conn)
    return result 

#######################################################################################################################

@app.route('/trips/find/', methods=['POST']) 
def route_find():

    input_data = request.get_json(force=True) # Get the input as dictionary
    specified_date = input_data['period']
    print(specified_date)
     
    # Subset the data with query 
    conn = make_connection()
    query = f"""SELECT * FROM trips WHERE start_time LIKE ('{specified_date}%')"""
    selected_data = pd.read_sql_query(query, conn)
    
    result = selected_data.groupby('start_station_id').agg({
    'bikeid' : 'count', 
    'duration_minutes' : 'mean'
    })
    return result.to_json()

#######################################################################################################################

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

if __name__ == '__main__':
    app.run(debug=True, port=5000)