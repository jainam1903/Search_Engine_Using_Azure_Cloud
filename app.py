from flask import Flask, render_template, request
import pyodbc
import time
import random
import pandas as pd
import csv
import string
from datetime import datetime, timedelta
import redis
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

# Azure SQL connection string
connection_string = os.getenv('connection_string')
conn = pyodbc.connect(connection_string)


cache = redis.StrictRedis(
    host=os.getenv('REDIS_HOST'),
    port=int(os.getenv('REDIS_PORT')),
    password=os.getenv('REDIS_PASSWORD'),
    ssl=os.getenv('REDIS_SSL'),
    abort_connect=os.getenv('REDIS_ABORT_CONNECT')
)

# Create the Earthquakes table
def create_earthquake_table():
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE EQ (
            time VARCHAR(100) NOT NULL,
            latitude FLOAT NOT NULL,
            longitude FLOAT NOT NULL,
            depth FLOAT NOT NULL,
            mag FLOAT NOT NULL,
            magType VARCHAR(100) NOT NULL,
            net VARCHAR(100) NOT NULL,
            id VARCHAR(100) NOT NULL,
            updated VARCHAR(100) NOT NULL,
            place VARCHAR(100) NOT NULL,
            type VARCHAR(100) NOT NULL,
        )
    ''')
    # calculate time taken to create the table
    start_time = time.time()
    cursor.commit()
    end_time = time.time()
    creation_time = end_time - start_time
    return creation_time

@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')

# A number of random queries (up to 1000 queries of random tuples in the dataset)
@app.route('/ass1', methods = ['GET','POST'])
def ass1():
    if request.method == "POST":
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        total_time = []
        n = int(request.form['n'])
        mag = float(request.form['mag'])
        start = time.time()
        for i in range(int(n)):
            begin_time = time.time()
            # cursor.execute("SELECT time, latitude, longitude, mag, place FROM all_month WHERE all_month.mag>"+str(mag)+";")
            cursor.execute("select time, latitude, longitude,mag, place from dbo.all_month where mag < "+str(mag)+";")
            x = cursor.fetchall()
            exit_time = time.time()
            total_time.append(exit_time - begin_time)
        end_time = time.time()
        execute_time = end_time - start 
        print("SQL", execute_time)
        return render_template("ass1.html", begin_time = begin_time, exit_time = exit_time,total_time = total_time, x=x, execute_time = execute_time)
    return render_template("ass1.html")

@app.route('/ass2', methods=['POST','GET'])
def ass2():
    if request.method == "POST":
        conn = pyodbc.connect(connection_string)
        cache.flushall()
        cursor = conn.cursor()
        number = request.form['n']
        mag = float(request.form['mag'])
        total_time = []
        start_time = time.time()
        s = "select time, latitude, longitude,mag, place from all_month where mag < "+str(mag)+";"
        for i in range(0,int(number)):
            begin_time = time.time() 
            if cache.exists(s):
                cache.get(s)
            else:
                cursor.execute(s)
                cache.set(s, str(cursor))
                result = cursor.fetchall()
            exit_time = time.time()
            total_time.append(exit_time-begin_time)        
        end_time = time.time()
        execute_time = end_time - start_time
        print("redis", execute_time)
        return render_template("ass2.html",  execute_time = execute_time, x = result, total_time = total_time)
    return render_template("ass2.html")

# write the above function using redis object caching
# @app.route('/ass2', methods = ['GET','POST'])
# def ass2():
#     # conn = pyodbc.connect(connection_string)
#     # cursor = conn.cursor()
#     if request.method == "POST":
#         # r.flushall()
#         total_time = []
#         n = int(request.form['n'])
#         mag = float(request.form['mag'])
#         start = time.time()
#         for i in range(int(n)):
#             begin_time = time.time()

#             cache_key = f"key_{mag}"
#             cached_data = cache.get(cache_key)
#             if cached_data:
#                 x = cached_data.decode('utf-8')
#             else:
#                 conn = pyodbc.connect(connection_string)
#                 cursor = conn.cursor()
#                 cursor.execute(f"SELECT time, latitude, longitude, mag, place FROM dbo.all_month WHERE mag < {mag};")
#                 x = cursor.fetchall()
#                 cursor.close()
#                 conn.close()

#                 cache.set(cache_key, str(x))
#             # s = "select time, latitude, longitude,mag, place from dbo.all_month where mag < "+str(mag)+";"
#             # if r.exists(s):
#             #     r.get(s)
#             # else:
#             #     cursor.execute(s)
#             #     r.set(s, str(cursor))
#             #     result = cursor.fetchall()
#             exit_time = time.time()
#             total_time.append(exit_time - begin_time)
#         end_time = time.time()
#         execute_time = end_time - start
#         print("SQL", execute_time)
#         return render_template("ass2.html", begin_time = begin_time, exit_time = exit_time,total_time = total_time, x=x, execute_time = execute_time)
#     return render_template("ass2.html")


# @app.route('/ass2', methods=['POST','GET'])
# def ass2():
#     conn = pyodbc.connect(connection_string)
#     cursor = conn.cursor()
#     if request.method == "POST":
#         r.flushall()
#         number = request.form['n']
#         mag = float(request.form['mag'])
#         total_time = []
#         start_time = time.time()
#         s = "select time, latitude, longitude,mag, place from dbo.all_month where mag < "+str(mag)+";"
#         for i in range(0,int(number)):
#             begin_time = time.time() 
#             if r.exists(s):
#                 r.get(s)
#             else:
#                 cursor.execute(s)
#                 r.set(s, str(cursor))
#                 result = cursor.fetchall()
#             exit_time = time.time()
#             total_time.append(exit_time-begin_time)        
#         end_time = time.time()
#         execute_time = end_time - start_time
#         print("redis", execute_time)
#         return render_template("ass2.html",  execute_time = execute_time, x = result, total_time = total_time)
#     return render_template("ass2.html")

@app.route('/create_table', methods=['POST'])
def create_table():
    creation_time = create_earthquake_table()
    return render_template('creation_time.html', creation_time=creation_time)

states = [
    'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia',
    'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland',
    'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire',
    'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania',
    'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
    'West Virginia', 'Wisconsin', 'Wyoming'
]
earthquake_types = [
    'Tectonic', 'Volcanic', 'Induced', 'Reservoir-induced', 'Explosion', 'Ice quake', 'Mine collapse', 'Unknown'
]

# Function to generate random tuples
def generate_random_tuples(n):
    tuples = []
    
    for _ in range(n):
        random_time = datetime.now() - timedelta(days=random.randint(1, 365))
        time = random_time.strftime('%Y-%m-%d')
        latitude = random.uniform(-90, 90)
        longitude = random.uniform(-180, 180)
        depth = random.uniform(0, 100)
        mag = random.uniform(0, 10)
        net = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))  # Random net (10 characters)
        id = random.randint(1, 1000)
        updated = random_time.strftime('%Y-%m-%d')
        place = random.choice(states)
        type = random.choice(earthquake_types)
        tuples.append((time, latitude, longitude, depth, mag, net, id, updated, place, type))
    return tuples

# Function to insert random data into the table
def insert_random_data(num_rows):
    # Generate random tuples
    tuples = generate_random_tuples(num_rows)
    # execution time
    start_time = time.time()  # Start measuring execution time


    # Connect to the database
    conn = pyodbc.connect(connection_string)


    # Execute the insert queries
    cursor = conn.cursor()
    for tuple in tuples:
        cursor.execute("INSERT INTO EQ (time, latitude, longitude, depth, mag, net, id, updated, place, type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       tuple)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

    end_time = time.time()  # Stop measuring execution time
    execution_time = end_time - start_time
    return execution_time



# Route to process the form submission
@app.route('/execute', methods=['POST'])
def execute():
    num_queries = int(request.form['num_queries'])
    execution_time = insert_random_data(num_queries)

    # Insert random data
    # insert_random_data(num_queries)
    

    # execution_time = insert_random_data(num_queries)
    return f"Execution Time: {execution_time} seconds for {num_queries} queries"

@app.route('/restricted_queries')
def restricted_queries():
    return render_template('restricted_queries.html')

# Route to process the restricted queries form submission
@app.route('/execute_restricted', methods=['POST'])
def execute_restricted_queries():
    num_queries = int(request.form['num_queries'])
    location = {
        'min_lat': float(request.form['min_lat']),
        'max_lat': float(request.form['max_lat']),
        'min_long': float(request.form['min_long']),
        'max_long': float(request.form['max_long'])
    }
    execution_time = insert_random_data(num_queries, location)
    return f"Execution Time: {execution_time} seconds for {num_queries} queries"

if __name__ == '__main__':
    app.run(debug=True)