from flask import Flask , request
from flask_restx import Api , Resource, fields
import cherrypy
from cherrypy.process.plugins import Daemonizer
from paste.translogger import TransLogger

import datetime as dt

from pyhive import hive

flask_app = Flask(__name__)
app = Api(app=flask_app,
          version='1.0',
          title="Weather_Monitoring",
          description="Weather Monitoring Apis")
name_space = app.namespace('wm',description='Weather Monitoring')
model = app.model('WeatherDetailModel',
                  {'CityName': fields.String(required=True,
                                             description='name of city',
                                             help="city cannnot be blank."),
                    'Temperature': fields.String(required=True,
                                                  description="City's Temperature",
                                                  help='temperature cannot be blank.'),
                    'Humidity': fields.String(required=True,
                                                  description="City's Humidity",
                                                  help='humidity cannot be blank.'),
                    'RainIntensity': fields.String(required=True,
                                                  description="City's Rain Intensity",
                                                  help='rain intensity cannot be blank.'),                              
                    'WindSpeed': fields.String(required=True,
                                                  description="City's wind speed",
                                                  help='wind speed cannot be blank.'), 
                    'WindDirection': fields.String(required=True,
                                                  description="City's wind direction",
                                                  help='wind direction cannot be blank.'),  
                    'CreationTime': fields.String(required=True,
                                                  description="Record's event time(received time)",
                                                  help='even time cannot be blank.') })


def get_hive_connection():
    print("creating hive connection ...")
    conn = hive.Connection(
    host="localhost",  
    port=10000,           
    username="hive",     
    database="default",
    configuration={"mapreduce.job.queuename": "queueA"})
    return conn

def get_weather_details():
    conn = get_hive_connection()
    print("connection established!")
    cursor = conn.cursor()

    # Fetch weather details from the Hive table
    query = "SELECT * FROM weather_details_tb1 order by CreationTime desc limit 3"

    cursor.execute(query)
    weather_details = cursor.fetchall()

    # Convert the result into a list of dictionaries
    weather_detail_list = []
    for row in weather_details:
        weather_detail = {
            'CityName': row[0],
            'Temperature': row[1],
            'Humidity': row[2],
            'RainIntensity': row[3],
            'WindSpeed': row[4],
            'WindDirection': row[5],
            'CreationTime': row[6]
        }
        weather_detail_list.append(weather_detail)

    cursor.close()
    conn.close()

    return weather_detail_list

@name_space.route("/v1")
class WeatherMonitoring(Resource):
    @app.doc(responses={200 : 'OK', 400: "Invalid Argument Provided", 500: "Mapping Key Error Occured"})
    def get(self):
        try:
            weather_detail_list = []
            weather_detail_list = get_weather_details()
            return weather_detail_list
        except KeyError as e:
            name_space.abort(500, e.__doc__, status='Could not retreive weather informations', statuscode="500")
        except  Exception as e:
            name_space.abort(400, e.__doc__, status='Could not retreive weather informations', statuscode="400")

def run_server(flask_app):
    app_logged = TransLogger(flask_app)

    cherrypy.tree.graft(app_logged,'/')

    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5000,
        'server_socket_host': '10.0.2.10'
    })

    Daemonizer(cherrypy.engine).subscribe()

    cherrypy.engine.start()
    cherrypy.engine.block()

if __name__ == '__main__':
    print("Strating Weather Api Server Application ...")
    #flask_app.run(port=5001,debug=True)
    run_server(flask_app)