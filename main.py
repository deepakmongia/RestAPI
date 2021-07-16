from flask import Flask, render_template, request, jsonify
import re
import logging as lg
import mysql.connector as connection
import csv
import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pymongo
from configparser import ConfigParser

app = Flask(__name__)

lg.basicConfig(filename = "logfile.log", level = lg.INFO, format = '%(asctime)s %(name)s %(message)s')

file = 'config.ini'

config = ConfigParser()
config.read(file)

@app.route('/mysql', methods=['POST']) # for calling the API from Postman/SOAPUI
def mysql_operations():

    try:
        #print("C")
        if (request.method=='POST'):
            #print("D")
            request_type = request.json['request_type']

            # file = 'config.ini'
            #
            # config = ConfigParser()
            # config.read(file)

            #print("E")
            mydb = connection.connect(host="localhost", user=config['mysql']['user'],
                                      passwd=config['mysql']['passwd'],
                                      database=config['mysql']['database'],
                                      use_pure=True)

            # print("B")
            # print(request_type)
            if request_type == "create_table":

                table_name = request.json['table_name']
                col_definitions = request.json['column_definitions']

                query = "create table if not exists " + table_name + "("

                for i in col_definitions.items():
                    query = query + i[0] + " " + i[1] + ","

                query = re.split(",$", query)[0]

                query += ")"

                # print(query)

                cur = mydb.cursor()
                cur.execute(query)

                return jsonify("table " + table_name + " created successfully")

            elif request_type == "insert_single":
                table_name = request.json['table_name']
                insert_values = request.json["column_values"]

                query = "insert into " + table_name + " values " + str(tuple(insert_values))
                # print(query)

                cur = mydb.cursor()
                cur.execute(query)
                mydb.commit()

                return jsonify("table " + table_name + " record inserted successfully")

            elif request_type == "update":
                # print("A")
                table_name = request.json['table_name']

                update_query = request.json['update_query']

                # print(update_query)

                cur = mydb.cursor()
                cur.execute(update_query)
                mydb.commit()

                return jsonify("table " + table_name + " record updated successfully")

            elif request_type =="bulk_insert":
                table_name = request.json['table_name']
                csv_file_path = request.json['csv_file_path']

                # print(csv_file_path)

                with open(csv_file_path, 'r') as file:
                    data_csv = csv.reader(file, delimiter='\n')
                    # print(data_csv)
                    for i in data_csv:
                        # print(i)
                        query = "insert into " + table_name + " values ({});".format(', '.join([value for value in i]))
                        query = query.replace('“', '"').replace('”', '"')
                        # print(query)

                        cur = mydb.cursor()
                        cur.execute(query)
                        # print(', '.join([value for value in i]))
                        # print([value for value in i])

                    mydb.commit()

                return jsonify("table " + table_name + " records inserted successfully")

            elif request_type == "delete_from_table":
                table_name = request.json['table_name']

                delete_query = request.json['delete_query']

                # print(delete_query)

                cur = mydb.cursor()
                cur.execute(delete_query)
                mydb.commit()

                return jsonify("table " + table_name + " record deleted successfully")

            elif request_type == "download_table":
                table_name = request.json['table_name']

                download_file_path = request.json['download_file_path']

                # print(download_file_path)

                os.chdir(download_file_path)

                query = "select * from " + table_name
                cur = mydb.cursor()
                cur.execute(query)

                downloaded_data = cur.fetchall()

                file_name = table_name + '.csv'

                with open(file_name, 'w') as downloaded_file:
                    file_writer = csv.writer(downloaded_file)
                    for i in range(len(downloaded_data)):
                        file_writer.writerow(downloaded_data[i])

                file_path = os.path.join(download_file_path, file_name)
                return jsonify("table " + table_name + " downloaded successfully in file location " +
                               file_path)

            else:
                return "invalid request"

    except Exception as e:
        print("Check logs for error")
        lg.error("Error occured here")
        lg.exception(e)

        return jsonify("an error occured")


@app.route('/cassandra', methods=['POST']) # for calling the API from Postman/SOAPUI
def cassandra_operations():

    try:
        #print("C")
        if (request.method=='POST'):
            #print("D")
            request_type = request.json['request_type']
            #print("E")

            cloud_config = {'secure_connect_bundle': config['cassandra']['secure_connect_bundle'] }
            auth_provider = PlainTextAuthProvider(config['cassandra']['client_id'],
                                                  config['cassandra']['client_secret'])
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            # print("B")
            # print(request_type)
            if request_type == "create_table":

                keyspace_name = request.json['keyspace_name']
                table_name = request.json['table_name']
                col_definitions = request.json['column_definitions']

                query = "create table if not exists " + keyspace_name + "." + table_name + "("

                for i in col_definitions.items():
                    query = query + i[0] + " " + i[1] + ","

                query = re.split(",$", query)[0]

                query += ");"

                print(query)

                session.execute(query)

                return jsonify("table " + table_name + " created successfully")

            elif request_type == "insert_single":
                keyspace_name = request.json['keyspace_name']
                table_name = request.json['table_name']
                insert_values = request.json["column_values"]

                col_names_insert = ""
                col_names_query = " SELECT keyspace_name, column_name FROM system_schema.columns where keyspace_name = '" + \
                    keyspace_name + "'" + " AND table_name = " + "'" + table_name + "' ;"

                print(col_names_query)
                row = session.execute(col_names_query)
                for i in row:
                    col_names_insert += list(i)[1]
                    col_names_insert += ", "

                col_names_insert = re.split(", $", col_names_insert)[0]

                query = "insert into " + keyspace_name + "." + table_name + "(" + col_names_insert + ")" + \
                        " values " + str(tuple(insert_values)) + ";"
                print(query)
                session.execute(query)
                return jsonify("table " + table_name + " record inserted successfully")

            elif request_type == "update":
                # print("A")
                keyspace_name = request.json['keyspace_name']
                table_name = request.json['table_name']
                update_query = request.json['update_query']

                # print(update_query)

                session.execute(update_query)

                return jsonify("table " + keyspace_name + "." + table_name + " record updated successfully")

            elif request_type =="bulk_insert":
                keyspace_name = request.json['keyspace_name']
                table_name = request.json['table_name']
                csv_file_path = request.json['csv_file_path']

                # print(csv_file_path)

                col_names_insert = ""
                col_names_query = " SELECT keyspace_name, column_name FROM system_schema.columns where keyspace_name = '" + \
                                  keyspace_name + "'" + " AND table_name = " + "'" + table_name + "' ;"

                print(col_names_query)
                row = session.execute(col_names_query)
                for i in row:
                    col_names_insert += list(i)[1]
                    col_names_insert += ", "

                col_names_insert = re.split(", $", col_names_insert)[0]

                with open(csv_file_path, 'r') as file:
                    data_csv = csv.reader(file, delimiter='\n')
                    # print(data_csv)
                    for i in data_csv:

                        query = "insert into " + keyspace_name + "." + table_name + "(" + col_names_insert + ")" + \
                                " values ({});"
                        query = query.format(', '.join([value for value in i]))
                        query = query.replace('“', "'").replace('”', "'")

                        print(query)

                        session.execute(query)

                return jsonify("table " + table_name + " records inserted successfully")

            elif request_type == "delete_from_table":
                keyspace_name = request.json['keyspace_name']
                table_name = request.json['table_name']

                delete_query = request.json['delete_query']

                # print(delete_query)

                session.execute(delete_query)

                return jsonify("table " + keyspace_name + "." + table_name + " record deleted successfully")

            elif request_type == "download_table":
                keyspace_name = request.json['keyspace_name']
                table_name = request.json['table_name']

                download_file_path = request.json['download_file_path']

                # print(download_file_path)

                os.chdir(download_file_path)

                query = "select * from " + keyspace_name + "." + table_name

                downloaded_data = []

                download_data = session.execute(query)
                for row in download_data:
                    downloaded_data.append(row)

                file_name = table_name + '.csv'

                with open(file_name, 'w') as downloaded_file:
                    file_writer = csv.writer(downloaded_file)
                    for i in range(len(downloaded_data)):
                        file_writer.writerow(downloaded_data[i])

                file_path = os.path.join(download_file_path, file_name)
                return jsonify("table " + table_name + " downloaded successfully in file location " +
                               file_path)

            else:
                return "invalid request"


    except Exception as e:
        print("Check logs for error")
        lg.error("Error occured here")
        lg.exception(e)

        return jsonify("an error occured")


@app.route('/mongodb', methods=['POST']) # for calling the API from Postman/SOAPUI
def mongodb_operations():

    try:
        print("C")
        if (request.method=='POST'):
            print("D")

            request_type = request.json['request_type']
            print("E")

            cluster_client = pymongo.MongoClient(config['mongodb']['client'])

            try:
                cluster_client.admin.command('ismaster')
            except Exception as e:
                print("Check logs for error")
                lg.error("Error occured here")
                lg.exception(e)

                return jsonify("MongoDB connection error")

            # print("B")
            # print(request_type)
            if request_type == "create_collection":

                dbname = request.json['dbname']
                collection_name = request.json['collection_name']

                # if dbname.lower in cluster_client.list_database_names():
                #     print('A1')
                #     database = cluster_client.db
                # else:
                #     print('A2')
                #     database = cluster_client[dbname]

                database = cluster_client.dbname

                # for db in cluster_client.list_database_names():
                #     if dbname == db.lower():
                #         print('db already exists')
                #         database = cluster_client.dbname
                # else:
                #     print('db does not exist')
                #     database = cluster_client[dbname]

                if collection_name in database.collection_names():
                    return jsonify("collection: " + collection_name + " already exists in database: " +
                                   dbname)

                else:
                    database.create_collection(collection_name)

                    return jsonify("collection: " + collection_name + " successfully created in database: " +
                                   dbname)

            # elif request_type == "insert_single":
            #     print('D1')
            #     dbname = request.json['dbname']
            #     collection_name = request.json['collection_name']
            #     data = request.json['data']
            #
            #     print(data)
            #
            #     for db in cluster_client.list_database_names():
            #         if dbname == db.lower():
            #             #database = cluster_client.dbname
            #             database = cluster_client[dbname]
            #     else:
            #         print('db does not exist')
            #         return jsonify("database: " + dbname + " does not exist" )
            #
            #     if collection_name in database.collection_names():
            #         collection = database[collection_name]
            #
            #     else:
            #
            #         return jsonify("collection: " + collection_name + " does not exist in database: " +
            #                        dbname)
            #
            #     collection.insert_one(data)
            #
            #     return jsonify("record successfully inserted in collection: " + collection_name +
            #                    " in database: " + dbname)

            else:
                return "invalid request"


    except Exception as e:
        print("Check logs for error")
        lg.error("Error occured here")
        lg.exception(e)

        return jsonify("an error occured")

if __name__ == '__main__':
    app.run()
