import multiprocessing
from multiprocessing import Process, Pool
import requests
from functools import reduce
import sqlite3
import json
import folium
from folium import plugins
from folium.vector_layers import PolyLine
import time
#from helper_matrixOperations_02 import MatrixUtils
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
import mysql.connector
import os


class Scrapper:

    '''
    Constructor function which initiate empty dictionaries, and specifiy
    the number of cores used for the scrapper

    :ivar geoinfo: Stores the scraped data
    :vartype geoinfo: dict
    :ivar core: Stores number of cores
    :vartype core: int
    :ivar lat_limit: The smallest lattitude splitting unit
    :vartype lat_limit: float
    :ivar lon_limit: The smallest longitude splitting unit
    :vartype lat_limit: float
    :ivar para: Parameters for the Scraper function
    :vartype para: list
    '''
    def __init__(self):
        self.geoinfo = {}
        self.cores = multiprocessing.cpu_count()
        self.lat_limit = 0.00210146171
        self.lon_limit = 0.00680744647
        self.para = []


    def taskManager(self, ne_lat, ne_lon, sw_lat, sw_lon):
        '''
        Main function that splits the targeting map into small chuncks and assign a core to scrape each small chunck
        :param ne_lat: Lattitude of the northeast boundary point
        :type ne_lat: float
        :param ne_lon: Longitude of the northeast boundary point
        :type ne_lon: float
        :param sw_lat: Lattitude of the southwest boundary point
        :type sw_lat: float
        :param sw_lon: Longitude of the southwest boundary point
        :type sw_lon: float
        '''
        total_param = self.split_4_by_3(ne_lat, ne_lon, sw_lat, sw_lon)
        self.para.append(total_param)
        # p = Pool(multiprocessing.cpu_count())
        # results = p.map(self.scrapper, total_param)
        # p.close()
        # p.join()
        results = []
        for l in total_param:
            results.append(self.scrapper(l))
            
        self.toDict(results)


    def split_4_by_3(self, ne_lat, ne_lon, sw_lat, sw_lon):
        '''
        Helper function to split the map into 3*4 small chuncks
        :param ne_lat: Lattitude of the northeast boundary point
        :type ne_lat: float
        :param ne_lon: Longitude of the northeast boundary point
        :type ne_lon: float
        :param sw_lat: Lattitude of the southwest boundary point
        :type sw_lat: float
        :param sw_lon: Longitude of the southwest boundary point
        :type sw_lon: float
        '''
        cores = self.cores
        total_param = []
        if ne_lat > sw_lat and ne_lon > sw_lon and ne_lat-sw_lat > self.lat_limit and ne_lon-sw_lon > self.lon_limit:
            if (ne_lat-sw_lat > ne_lon-sw_lon):
                lat_div = (ne_lat-sw_lat)/4
                lon_div = (ne_lon-sw_lon)/3
                for x in range(0,4):
                    for y in range(0,3):
                        total_param.append([ne_lat-x*lat_div, ne_lon-y*lon_div, ne_lat-x*lat_div - lat_div, ne_lon-y*lon_div - lon_div])

            elif ne_lat-sw_lat > self.lat_limit:
                lat_div = (ne_lat-sw_lat)/3
                lon_div = (ne_lon-sw_lon)/4
                for x in range(0,3):
                    for y in range(0,4):
                        total_param.append([ne_lat-x*lat_div, ne_lon-y*lon_div, ne_lat-x*lat_div - lat_div, ne_lon-y*lon_div - lon_div])

        return total_param


    def toDict(self, results):
        '''
        Helper function. If the first element of the given result is true, meaning the map containing more data, then it is fed into the taskmanager again. Or it will be stored into a dictionary
        :param results: first element is a boolean indicating if the given chunck of map should be scrape further
        :type results: list
        '''
        for each in results:
            if  each != None and each[0] == True:
                self.taskManager(each[1], each[2], each[3], each[4])
            else:
                if each != None:
                    for x in each:
                        self.geoinfo[(x[0], x[1])] = x[2:]

    def scrapper(self, arr):
        '''
        Main function to fetch and parse data. Due to the characteristics of websites implemented using AJAX architecture, not all data are shown at one time. For ChargePoint website in particular,
        if there are more than 51 data points on a given region, the response of the API call will only return 51 data points. Therefore, we need to zoom in and split the given region in chuncks and
        re-scrap the region

        :param arr: list of ne and sw boundries
        :type arr: list

        '''
        ne_lat = arr[0]
        ne_lon = arr[1]
        sw_lat = arr[2]
        sw_lon = arr[3]
        data = self.getInfo(ne_lat, ne_lon, sw_lat, sw_lon)
        try:
            station_list_size = len(data["station_list"]["summaries"])
        except:
            print(data["station_list"])
            station_list_size = 0


        if (station_list_size >= 50):
            return [True, ne_lat, ne_lon, sw_lat, sw_lon]
        else:
            if (station_list_size != 0):
                output = []
                for x in range(0, len(data["station_list"]["summaries"])):
                    lat = data["station_list"]["summaries"][x]["lat"]
                    lon = data["station_list"]["summaries"][x]["lon"]
                    available_port = data["station_list"]["summaries"][x]["port_count"]["available"]
                    total_port = data["station_list"]["summaries"][x]["port_count"]["total"]
                    availability = str(available_port) + ":" + str(total_port)
                    try:
                        port_type_info = str(data["station_list"]["port_type_info"])
                    except: 
                        port_type_info = "Not Specified"
                    try:
                        port_type_count = str(data["station_list"]["summaries"][x]["port_type_count"])
                    except: 
                        port_type_count = "Not Specified"
                    try:
                        station_status = str(data["station_list"]["summaries"][x]["station_status"])
                    except: 
                        station_status = "Not Specified"
                        
                    try:
                        station_power_shed_status = str(data["station_list"]["summaries"][x]["station_power_shed_status"])
                    except: 
                        station_power_shed_status = "Not Specified"
                        
                    try:
                        device_id = str(data["station_list"]["summaries"][x]["device_id"])
                    except: 
                        device_id = "Not Specified"
                    
                    try:
                        address = str(data["station_list"]["summaries"][x]["address"])
                    except: 
                        address = "Not Specified"
                    
                    try:
                        station_name = str(data["station_list"]["summaries"][x]["station_name"])
                    except: 
                        station_name = "Not Specified"
                    
                    try:
                        connected = str(data["station_list"]["summaries"][x]["is_connected"])
                    except: 
                        connected = "Not Specified"
                    try:
                        fee = str(data["station_list"]["summaries"][x]["estimated_fee"])
                    except:
                        fee = "Not Specified"
                    try:
                        level = list(data["station_list"]["summaries"][x]["map_data"].keys())[0]
                    except:
                        level = "Not Specified"
                    output.append([lat, lon, total_port, level, availability, 
                                   fee, connected, station_name, address, 
                                   device_id, station_power_shed_status,
                                   station_status, port_type_count,
                                   port_type_info])                
                
                return output


    def getInfo(self,ne_lat, ne_lon, sw_lat, sw_lon):
        '''
        Helper function that make request calls to the server
        :param ne_lat: Lattitude of the northeast boundary point
        :type ne_lat: float
        :param ne_lon: Longitude of the northeast boundary point
        :type ne_lon: float
        :param sw_lat: Lattitude of the southwest boundary point
        :type sw_lat: float
        :param sw_lon: Longitude of the southwest boundary point
        :type sw_lon: float
        '''
        url = self.getURL(ne_lat, ne_lon, sw_lat, sw_lon)
        r = requests.get(url, timeout = 20)
        data = r.text
        data = json.loads(data)
        return data

    def getURL(self, ne_lat, ne_lon, sw_lat, sw_lon):
        url = 'https://mc.chargepoint.com/map-prod/get?{{"station_list":{{"ne_lat":{},"ne_lon":{},"sw_lat":{},"sw_lon":{},"page_size":100,"page_offset":"","sort_by":"distance","screen_width":800,"screen_height":600,"filter":{{"connector_l1":false,"connector_l2":false,"is_bmw_dc_program":false,"is_nctc_program":false,"connector_chademo":false,"connector_combo":false,"connector_tesla":false,"price_free":false,"status_available":false,"network_chargepoint":false,"network_blink":false,"network_semacharge":false,"network_evgo":false,"connector_l2_nema_1450":false,"connector_l2_tesla":false}},"user_lat":49.2626692,"user_lon":-123.24743289999999,"include_map_bound":true,"estimated_fee_input":{{"arrival_time":"10:00","battery_size":30}}}},"user_id":0}}'.format(ne_lat, ne_lon, sw_lat, sw_lon)
        return url

    def paint(self):
        print("now printing markers")
        m = folium.Map(location=[49.2829, -123.0750])
        # cluster = folium.plugins.MarkerCluster().add_to(m)
        for key in self.geoinfo.keys():
            lat = key[0]
            lon = key[1]
            if self.geoinfo[key][2].split(":")[0] == "0":
                folium.Marker(location=[lat, lon],popup=self.geoinfo[key][2],icon=folium.Icon(color = 'red')).add_to(m)#.add_to(cluster)
            else:
                folium.Marker(location=[lat, lon],popup=self.geoinfo[key][2],icon=folium.Icon(color = 'blue')).add_to(m)#.add_to(cluster)
        m.save("availability.html")


    def saveToDBbyTime(self):
        '''
        Save the scraped data to sqlite3 database
        '''
        # t = time.strftime("time_%H_%M_%S", time.localtime())
        t = datetime.datetime.now()
        t = t.strftime("time_%Y_%m_%d_%H_%M_%S")
        # conn = sqlite3.connect("10_min_2021.db")#os.path.join(os.pardir, "databases\\10_min_2021.db")
        conn = sqlite3.connect(os.path.join(os.pardir, "10_min_2021.db"))
        geoinfo = self.modify()
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS %s (lat float ,lon float, \
                  port int, level text, availability, \
                      fee, connected, station_name, address, \
                               device_id, station_power_shed_status, \
                               station_status, port_type_count, port_type_info, PRIMARY KEY (lat, lon))''' %t)
            
        c.executemany("INSERT INTO %s (lat, lon, port, level, availability, \
                      fee, connected, station_name, address, device_id, \
                          station_power_shed_status, station_status, \
                              port_type_count, port_type_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" %t, geoinfo)
            
        #  c.execute('''CREATE TABLE IF NOT EXISTS %s (lat float ,lon float, \
        #           port int, level text, availability, \PRIMARY KEY (lat, lon))''' %t)
        # c.executemany("INSERT INTO %s (lat, lon, port, level, availability) VALUES (?, ?, ?, ?, ?)" %t, geoinfo)
            
        conn.commit()
        conn.close()


    def saveToMySQLbyTime(self):
        '''
        Save the scraped data to mysql database
        '''
        print("Running database scripts")
        t = datetime.datetime.now()
        t = t.strftime("time_%Y_%m_%d_%H_%M_%S")
        self.geoinfo_list = self.modify()
        mydb = mysql.connector.connect(
                host = "localhost",
                user = "chargepoint",
                database = "CP")
        c = mydb.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS "+t+" (lat float ,lon float, port int, level text, availability text);")
        #sql_statement = ("INSERT INTO %s (lat, lon, port, level, availability) VALUES (%s,%s,%s,%s,%s)"%t)
        #sql_input = (t,self.geoinfo_list[0],self.geoinfo_list[1],self.geoinfo_list[2],self.geoinfo_list[3],self.geoinfo_list[4])
        c.executemany('''INSERT INTO ''' + t + '''(lat, lon, port, level, availability) VALUES (%s,%s,%s,%s,%s)''', self.geoinfo_list)
        mydb.commit()
        mydb.close()

    def modify(self):
        '''
        Modify the dictionary of scraped data into a list
        :param geoinfo: scraped data in a dictionary
        :type geoinfo: list
        '''
        output = []
        for key in self.geoinfo.keys():
            aux = [key[0], key[1]]
            aux.extend(self.geoinfo[key])
            output.append(aux)
        return output


def run():
    S = Scrapper()
    # S.taskManager(51.522764, -113.402441, 49.003905, -123.320867)
    # S.taskManager(60.042299, -102.0935, 46.503905, -123.320867)
    # S.taskManager(49.331702291033785, -123.06885393341035, 49.32638707912375, -123.08162885850105)
    S.taskManager(49.314549, -123.027079, 49.185826, -123.310445)
    S.paint()
    S.saveToDBbyTime()
    # S.saveToMySQLbyTime()

# start_time = time.time()
# run()
# print(time.time()-start_time)
# scheduler = BlockingScheduler()
# scheduler.add_job(run, 'interval', minutes = 30)
# scheduler.start()

import datetime as dt
samplingTime = 60*10

starttime=time.time()

while True:
    time.sleep(samplingTime - 
              ((time.time() - starttime) % samplingTime)) #sleep for 15 mins
    
    try:
        run()
        
    except Exception as e:
        curr = dt.datetime.now()
        print('{time} - {text} - {args}'.format(time=curr.strftime('%Y-%m-%d %H:%M:%S'), text=str(e), args=e.args))
        
        
