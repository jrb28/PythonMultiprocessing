# -*- coding: utf-8 -*-
"""
Created on Wed Apr 05 10:10:48 2017

@author: jrbrad
"""

import mysql.connector as mySQL
import math

mysql_user_name =  'Jim'
mysql_password = 'MySQL'
mysql_ip = '127.0.0.1'
mysql_db = 'mst_dt'

import time
import multiprocessing
#from multiprocessing import Manager
#from functools import partial
   
def db_connect():
    cnx = mySQL.connect(user=mysql_user_name, passwd=mysql_password,
                        host=mysql_ip, db=mysql_db)
    return cnx
    
def getDBDataDict(commandString):
    cnx = db_connect()
    cursor = cnx.cursor()
    cursor.execute(commandString)
    items = {}
    for item in list(cursor):
        items[item[0]] = item[1:len(item)]
    cursor.close()
    cnx.close()
    return items

def getDBDataDictSpec(spName):
    cnx = db_connect()
    cursor = cnx.cursor()
    cursor.callproc(spName)
    items = {}
    for result in cursor.stored_results():
        for item in result:
            items[item[0]] = [(item[1],item[2]),item[3]]
    cursor.close()
    cnx.close()
    return items

def getDBDataList(spName):
    cnx = db_connect()
    cursor = cnx.cursor()
    cursor.callproc(spName)
    items = []
    for result in cursor.stored_results():
        for row in result:
            new_row = []
            for element in row:
                new_row.append(element)
            items.append(new_row)
    cursor.close()
    cnx.close()
    return items
    
def bearing_sort(dc,stores):
    import math

    """ Dictionary of points in the format {id:(lat,lng), ...}"""
    #locs = {0:['Richmond',(37.538815, -77.433647)],1:['Williamsburg',(37.271624, -76.713207)],2:['Virginia Beach',(36.855211, -75.975812)],3:['Chesapeake',(36.764064, -76.272614)],4:['Portsmouth',(36.872869, -76.357965)],5:['Newport News',(37.112369, -76.495079)]}
    root = (dc[0][0],dc[0][1])
    #R = 6371 * 0.621371
    
    locs_bearing = []
    lat1 = root[0]
    lon1 = root[1]
    for this_key in [x for x in stores.keys()]:
        lat2 = stores[this_key][0][0]
        lon2 = stores[this_key][0][1]
        locs_bearing.append([this_key,math.atan2(math.sin(lon2-lon1)*math.cos(lat2),math.cos(lat1)*math.sin(lat2)-math.sin(lat1)*math.cos(lat2)*math.cos(lon2-lon1))])
        """ see these sites for the formula:
            http://www.movable-type.co.uk/scripts/latlong.html
            http://mathforum.org/library/drmath/view/55417.html """
    locs_bearing.sort(key=lambda x:x[1])
    #name_bearing = [stores[x[0]] for x in locs_bearing]
    
    return locs_bearing

def groupStores(bearing_order,route_len):
    routes = []
    while len(bearing_order) > 0:
        routes.append([x[0] for x in bearing_order[0:min(route_len,len(bearing_order))]])
        #routes.append(bearing_order[0:min(route_len,len(routes))])
        del bearing_order[0:min(route_len,len(bearing_order))]
    return routes

def hav_dist(lat1, lon1, lat2, lon2):
    """ latitude and longitude inputs are in degrees """
    R = 6371 * 0.621371
    """ convert latitude and longitude to radians """
    lat1 = lat1 * math.pi /180.0
    lon1 = lon1 * math.pi /180.0
    lat2 = lat2 * math.pi /180.0
    lon2 = lon2 * math.pi /180.0
    
    a = math.sin((lat2-lat1)/2)**2 + math.cos(lat1) * math.cos(lat2) * (math.sin((lon2-lon1)/2))**2
    return R * 2 * math.atan2(math.sqrt(a),math.sqrt(1-a))

def getDist(dc,stores):
    dist = {}
    lat_dc = dc[0][0]
    lon_dc = dc[0][1]
    sorted_store_keys = sorted(stores.keys())
    for store in sorted_store_keys:
        dist[(store,'dc')] = hav_dist(lat_dc, lon_dc, stores[store][0][0], stores[store][0][1])
        for store1 in sorted_store_keys:
            dist[(store,store1)] = hav_dist(stores[store][0][0], stores[store][0][1], stores[store1][0][0], stores[store1][0][1])
    return dist
    
def init_clusters(loc_list):
    clusters = {}
    for i in loc_list:
        clusters[i] = [i]
    return clusters

def mst(dist,stores):
    stores.append('dc')
    clusters = init_clusters(stores)   # initial a list of clusters with each location in its own sub-list
    dist_list = [[k,v] for k,v in dist.items() if set([k[0],k[1]]).issubset(set(stores))] # need a list for distances to enable sorting
    dist_list.sort(key = lambda x:x[1]) # sort the list
    mst = []                            # original empty list for MST solution
    
    " Use the variable i to index through the sorted list of distances """
    i = -1
    """ execute loop until the MST solution has the apporpirate number of links included """
    while len(mst) < len(stores) - 1:
        i += 1
        c_ids = []                # this variable is for keeping the clusters that are being connected in each loop iteration
        for this_key in clusters:
            if dist_list[i][0][0] in clusters[this_key] and dist_list[i][0][1]in clusters[this_key]:
                break
            elif dist_list[i][0][0] in clusters[this_key] or dist_list[i][0][1] in clusters[this_key]:
                c_ids.append(this_key)
        else:
            mst.append(dist_list[i][0])
            clusters[c_ids[0]] = clusters[c_ids[0]]+ clusters[c_ids[1]]  # combine clusters into the first cluster
            del clusters[c_ids[1]]                                       # delete the second cluster

    #print mst
    #msts.append(mst)
    return mst
    #msts.append(mst)                                     
    
    
if __name__ == '__main__':
    dc = getDBDataList('spGetDCData')
    stores = getDBDataDictSpec('spGetRealStoresData')
    order = bearing_sort(dc,stores)
    routes = groupStores(order,3)
    dist = getDist(dc,stores)
    
    num_cores = 5
    time_start = time.time()
    pool = multiprocessing.Pool(num_cores)
    #func = partial(mst,dist)
    args = [(dist, routes[i]) for i in range(len(routes))]
    results = pool.starmap(mst,args)
    pool.close()
    pool.join()
    exec_time = time.time() - time_start
    print("Execution time:",exec_time)
    
    print('Results using pool.starmap():')
    for x in results:
        print('\t', x)