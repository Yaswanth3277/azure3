import textwrap
from flask import Flask, render_template, request
from werkzeug.utils import secure_filename
import pandas as pd
import pyodbc
import timeit
import redis
import hashlib
import pickle
from pymongo import MongoClient

app = Flask(__name__)

driver = '{ODBC Driver 17 for SQL Server}'
server_name = 'anonymous'
database_name = 'csvdatabase'
username = "anonymous"
password = "Yash@3277"
server = '{server_name}.database.windows.net,1433'.format(server_name=server_name)
connection_string = textwrap.dedent('''
    Driver={driver};
    Server={server};
    Database={database};
    Uid={username};
    Pwd={password};
    Encrypt=yes;
    TrustServerCertificate=no;
    Connection Timeout=30;
'''.format(
    driver=driver,
    server=server,
    database=database_name,
    username=username,
    password=password
))
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()
r = redis.StrictRedis(host='adb3.redis.cache.windows.net',port=6380, db=0, password='qPurtLYjO4JUn0r0jvbjmifZd5+PA7KkBiOyKD8QK10=', ssl=True)
result = r.ping()
print("Ping returned : " + str(result))

mconnection_string = "mongodb://adb3:hSancCqD5F8mYEkorkOAEtoFVqY8p9sOj6kUoUOFkVRgCDnGxfQuJ9SPnQH8xEsKW7Rn5avZn6A5X5C3PBc7VQ==@adb3.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@adb3@"

client = MongoClient(r"mongodb://adb3:hSancCqD5F8mYEkorkOAEtoFVqY8p9sOj6kUoUOFkVRgCDnGxfQuJ9SPnQH8xEsKW7Rn5avZn6A5X5C3PBc7VQ==@adb3.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@adb3@")
db = client.adb3
todos = db.quakedata_3


@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template("index.html")


@app.route('/create', methods=['GET', 'POST'])
def create_table():
    if request.method == 'POST':
        start_time = timeit.default_timer()
        cursor.execute('CREATE TABLE quakedata_3(Time nvarchar(50), Latitude float, Longitude float, Depth float, Mag float NULL DEFAULT 0.0, Magtype nvarchar(50) NULL DEFAULT 0.0, Nst int NULL DEFAULT 0.0, Gap float NULL DEFAULT 0.0, Dmin  float NULL DEFAULT 0.0, Rms float, Net nvarchar(50), ID nvarchar(50), Updated nvarchar(50), Place nvarchar(MAX), Type nvarchar(50), HorizontalError float NULL DEFAULT 0.0, DepthError float, MagError float NULL DEFAULT 0.0, MagNst int NULL DEFAULT 0.0, Status nvarchar(50), LocationSource nvarchar(50), MagSource nvarchar(50))')
        cursor.execute('CREATE INDEX indexes on quakedata_3(Time, Latitude, Longitude, Mag, Magtype)')
        conn.commit()
        elapsed = timeit.default_timer() - start_time
        print("Time taken to create a table and add indexes is :", elapsed)
    return render_template('index.html', celapsed = elapsed)


@app.route('/insert', methods=['GET', 'POST'])
def insert_data():
    if request.method == 'POST':
        f = request.files['csvupload']
        f.save(secure_filename(f.filename))
        df = pd.read_csv(f.filename)
        print(df)
        columns = df.columns
        print(df['magError'])
        df['time'] = df['time'].fillna("NA")
        df['latitude'] = df['latitude'].fillna(0.0)
        df['longitude'] = df['longitude'].fillna(0.0)
        df['depth'] = df['depth'].fillna(0.0)
        df['mag'] = df['mag'].fillna(0.0)
        df['magType'] = df['magType'].fillna("NA")
        df['nst'] = df['nst'].fillna(0)
        df['gap'] = df['gap'].fillna(0.0)
        df['dmin'] = df['dmin'].fillna(0.0)
        df['horizontalError'] = df['horizontalError'].fillna(0.0)
        df['magError'] = df['magError'].fillna(0.0)
        df['magNst'] = df['magNst'].fillna(0.0)
        df['depthError'] = df['depthError'].fillna(0.0)
        print(columns[0], columns[21])
        start_time = timeit.default_timer()
        for row in df.itertuples():
            print(row)
            cursor.execute(
                "INSERT INTO csvdatabase.dbo.quakedata_3 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                row.time, row.latitude, row.longitude, row.depth, row.mag, row.magType, row.nst, row.gap, row.dmin,
                row.rms, row.net, row.id, row.updated, row.place, row.type, row.horizontalError, row.depthError,
                row.magError, row.magNst, row.status, row.locationSource, row.magSource)

        conn.commit()
        elapsed = timeit.default_timer() - start_time
        print('Time taken to insert earthquake data into table is :', elapsed)
    return render_template("index.html", ielapsed = elapsed)


@app.route('/randomq', methods=['GET', 'POST'])
def random_q():
    if request.method == 'POST':
        num = request.form.get('randomq')
        start_time = timeit.default_timer()
        for data in range(int(num)):
            cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from quakedata_3")
            cursor.execute("INSERT INTO csvdatabase.dbo.quakedata_3 VALUES('2021-06-20T00:22:13.990Z', 19.23016739, -155.0514984, 42.52000046, 2.29, 'ml', 55, 215, 0.03837, 0.129999995, 'hv', 'hv72535902', '2021-06-20T00:27:46.240Z', '27 km SSE of Fern Forest, Hawaii', 'earthquake', 0.67, 0.479999989,3.22,26, 'automatic', 'hv', 'hv')")
            conn.commit()
            print("Hello")
        elapsed = timeit.default_timer() - start_time
    return render_template("index.html", qelapsed = elapsed)


@app.route('/rrandomq', methods=['GET', 'POST'])
def rrandom_q():
    if request.method == 'POST':
        num = request.form.get('rrandomq')
        startmag = 2
        stopmag = 4
        location = 'CA'
        latitude1 = 25
        longitude1 = 10
        latitude2 = 130
        longitude2 = 150
        start_time = timeit.default_timer()
        for data in range(int(num)):
            cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from quakedata_3 where Mag>? and Mag<? and LocationSource = ? and latitude>? and longitude>? and latitude<? and longitude<?", startmag, stopmag, location, latitude1, longitude1, latitude2, longitude2)
            print("HI")
        elapsed = timeit.default_timer() - start_time
    return render_template("index.html", rqelapsed = elapsed)


@app.route('/nrandomq', methods=['GET', 'POST'])
def nrandom_q():
    if request.method == 'POST':
        num = request.form.get('nrandomq')
        startmag = 2
        stopmag = 4

        query = "select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from quakedata_3 where Mag > ? and Mag < ?"
        hash = hashlib.sha224(query.encode('utf-8')).hexdigest()
        key = "redis_cache:" + hash
        start_time = timeit.default_timer()
        for data in range(int(num)):
            if (r.get(key)):
                print("redis cached")
            else:
                print(key)
                print(r)
                cursor.execute(query, startmag, stopmag)
                fetchimpres = list(cursor.fetchall())
                r.set(key, pickle.dumps(list(fetchimpres)))
                r.expire(key, 36)
            print("Hello")
        elapsed = timeit.default_timer() - start_time
    return render_template("index.html", nqelapsed = elapsed)


@app.route('/nrrandomq', methods=['GET', 'POST'])
def nrrandom_q():
    if request.method == 'POST':
        if request.method == 'POST':
            num = request.form.get('nrrandomq')
            startmag = 2
            stopmag = 4
            location = 'CA'
            latitude1 = 25
            longitude1 = 10
            latitude2 = 130
            longitude2 = 150
            query = "select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from quakedata_3 where Mag>? and Mag<? and LocationSource = ? and latitude>? and longitude>? and latitude<? and longitude<?"
            hash = hashlib.sha224(query.encode('utf-8')).hexdigest()
            key = "redis_cache:" + hash
            start_time = timeit.default_timer()
            for data in range(int(num)):
                if (r.get(key)):
                    print("redis cached")
                else:
                    print(key)
                    print(r)
                    cursor.execute(query, startmag, stopmag, location, latitude1, longitude1, latitude2, longitude2)
                    fetchimpres = list(cursor.fetchall())
                    r.set(key, pickle.dumps(list(fetchimpres)))
                    r.expire(key, 36)
                print("Hello")
            elapsed = timeit.default_timer() - start_time
    return render_template("index.html", nrqelapsed = elapsed)


@app.route('/mrandomq', methods=['GET', 'POST'])
def mrandom_q():
    if request.method == 'POST':
        num = request.form.get('mrandomq')
        startmag = 2
        stopmag = 4
        start_time = timeit.default_timer()
        for data in range(int(num)):
            todo = todos.find()
            print(todo)
            print("Hello")
        elapsed = timeit.default_timer() - start_time
    return render_template("index.html", mqelapsed = elapsed)


@app.route('/mrrandomq', methods=['GET', 'POST'])
def mrrandom_q():
    if request.method == 'POST':
        num = request.form.get('mrrandomq')
        startmag = 2
        stopmag = 4
        location = 'CA'
        latitude1 = 25
        longitude1 = 10
        latitude2 = 130
        longitude2 = 150
        start_time = timeit.default_timer()
        for data in range(int(num)):
            todo = todos.find({"Mag": {"$gt":startmag,"$lt":stopmag}})
            print("HI")
            print(todo)
        elapsed = timeit.default_timer() - start_time
    return render_template("index.html", mrqelapsed = elapsed)


if __name__ == '__main__':
    app.run()