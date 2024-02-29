import psycopg2
import sys
import string
import random
import argparse

class Generator:

    def __init__(self):
        self.tables = [];


    def connect(self, host, user, password, dbname, port=5432):
        self.connection = psycopg2.connect(user=user,
                                           password=password,
                                           host=host,
                                           port=port,
                                           database=dbname)

        self.connection.set_session(autocommit=False)

        self.cursor = self.connection.cursor()


    def execute(self, sql):
        self.cursor.execute(sql)


    def create_table(self):
        idx = len(self.tables)

        sql = "DROP TABLE IF EXISTS table_{}".format(idx)
        self.execute(sql)

        sql = "CREATE TABLE table_{} (id SERIAL8 PRIMARY KEY, a int, b text)".format(idx)
        self.execute(sql)

        self.tables.append("table_{}".format(idx))

        return idx;


    def update(self, table_idx, a, b):
        table = self.tables[table_idx]

        if a is not None and b is None:
            sql = "UPDATE {} SET a={}".format(table, a)
        elif a is not None and b is not None:
            sql = "UPDATE {} SET a={}, b='{}'".format(table, a, b)
        elif a is None and b is not None:
            sql = "UPDATE {} SET b='{}'".format(table, a, b)

        self.execute(sql)


    def insert(self, table_idx, a, b):
        table = self.tables[table_idx]

        sql = "INSERT INTO {} (a, b) VALUES ({}, '{}')".format(table, a, b)
        self.execute(sql)

    def sub_insert(self, table_idx, a, b):
        table = self.tables[table_idx]

        # this should generate a savepoint and release
        sql = "SAVEPOINT sub_insert"
        self.execute(sql)

        sql = "INSERT INTO {} (a, b) VALUES ({}, '{}')".format(table, a, b)
        self.execute(sql)

        sql = "RELEASE SAVEPOINT sub_insert"
        # sql = "ROLLBACK TO SAVEPOINT sub_insert"
        self.execute(sql)

    def savepoint(self, name):
        sql = "SAVEPOINT {}".format(name)
        self.execute(sql)

    def release_savepoint(self, name):
        sql = "RELEASE SAVEPOINT {}".format(name)
        self.execute(sql)

    def rollback_savepoint(self, name):
        sql = "ROLLBACK TO SAVEPOINT {}".format(name)
        self.execute(sql)

    def commit(self):
        self.connection.commit()


    def close(self):
        self.cursor.close()
        self.connection.close()


    def generate_text(self, length):
        str = "";
        for i in range(length):
            str += random.choice(string.ascii_letters)

        return str


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Generate SQL load against a database.')
    parser.add_argument('-H', help='hostname', dest='host', required=True)
    parser.add_argument('-u', help='username', dest='user', required=True)
    parser.add_argument('-p', help='password', dest='password', required=True)
    parser.add_argument('-d', help='database name', dest='dbname', required=True)
    parser.add_argument('-i', help='number of inserts per table', dest='iters', type=int, default=1)
    parser.add_argument('-s', help='row size (B) per insert', dest='rsize', type=int, default=200)
    parser.add_argument('-n', help='number of tables', dest='ntables', type=int, default=1)

    args = parser.parse_args();

    generator = Generator()

    print ("Connecting")
    generator.connect(args.host, args.user, args.password, args.dbname)

    print ("Creating tables: {}".format(args.ntables))
    tables = []
    for i in range(0, args.ntables):
        table = generator.create_table()
        tables.append(table)
    generator.commit()

    print ("Generating inserts: {}".format(args.iters))
    text = generator.generate_text(args.rsize);

    # start transaction
    generator.insert(tables[0], 0, text)

    generator.savepoint("subxact")
    for i in range(0, args.iters):
        for j in range(0, args.ntables):
            generator.insert(tables[j], i, text)

    generator.rollback_savepoint("subxact")

    # generate sub query
    # generator.sub_insert(tables[0], 0, text)

    generator.commit()

    generator.close()





