#!/usr/bin/env python

# This script takes a MML file and tests that the queries can be run

from __future__ import print_function
import argparse
import psycopg2
import sys
import yaml

class PostgresConn:
    def __init__(self, dbname, host, port, user):
        self.dbname = dbname
        self.host = host
        self.port = port
        self.user = user
        self.conns = {}

    def getCursor(self, host, port, dbname, user, password):
        # A tuple to identify the DB by. Order is from Mapnik docs, but doesn't really matter
        connkey = (self.host or host, self.port or port, self.dbname or dbname, self.user or user, password)
        if connkey not in self.conns:
            self.conns[connkey] = psycopg2.connect(host=connkey[0], port=connkey[1], dbname=connkey[2], user=connkey[3], password=connkey[4])

class Layer:
    def __init__(self, yaml_layer):
        self.id = yaml_layer["id"]
        self.datasource = Datasource(yaml_layer["Datasource"])

    def isValid(self, dbconn = None):
        return self.datasource.isValid(dbconn)

class Datasource:
    # This could be multiple classes that inherit from a base class, but we're only validating postgis
    def __init__(self, yaml_datasource):
        self.type = yaml_datasource["type"]
        if self.type == "postgis":
            self.sql = yaml_datasource.get("table")
            self.host = yaml_datasource.get("host")
            self.port = yaml_datasource.get("port")
            self.dbname = yaml_datasource.get("dbname")
            self.user = yaml_datasource.get("user")
            self.password = yaml_datasource.get("password")

    def isValid(self, dbconn = None):
        # Assume all shapefiles are valid
        if self.type == "shape":
            return True
        elif self.type == "postgis":
            curs = dbconn.getCursor(host=self.host, port=self.port, dbname=self.dbname, user=self.user, password=self.password)
            return True

def main():
    parser = argparse.ArgumentParser(description='Run MML queries')

    parser.add_argument("-f", "--file", type=argparse.FileType('r'), action="store", help="MML file with queries")
    parser.add_argument("-d", "--database", action="store", help="Override database name to connect to")
    parser.add_argument("-H", "--host", action="store", help="Override database server host or socket directory")
    parser.add_argument("-p", "--port", action="store", help="Override database server port")
    parser.add_argument("-U", "--username", action="store", help="Override database user name")
    opts = parser.parse_args()

    # Reuse a db connection when possible
    dbconn = PostgresConn(opts.database, opts.host, opts.port, opts.username)

    valid = True
    for yaml_layer in yaml.safe_load(opts.file)["Layer"]:
        l = Layer(yaml_layer)
        if not l.isValid(dbconn):
            print("Layer {0} is not valid")
            valid = False

    if not valid:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == '__main__':
  main()
