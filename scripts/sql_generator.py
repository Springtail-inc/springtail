import psycopg2
import sys
import string
import random

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

    idx = 1
    print (len(sys.argv))

    while idx < len(sys.argv):
        if sys.argv[idx] == '-h':
            host = sys.argv[idx+1]
        elif sys.argv[idx] == '-u':
            user = sys.argv[idx+1]
        elif sys.argv[idx] == '-p':
            password = sys.argv[idx+1]
        elif sys.argv[idx] == '-d':
            dbname = sys.argv[idx+1]
        else:
            print ("Unknown option: {}".format(sys.argv[idx]))
            print ("Usage: -h hostname -u username -p password -d dbname")
            sys.exit(1)

        print ("{} {}".format(sys.argv[idx], sys.argv[idx+1]))

        idx = idx + 2

    if host is None or user is None or password is None or dbname is None:
        print ("Usage: -h hostname -u username -p password -d dbname")
        sys.exit(1)


    generator = Generator()

    print ("Connected")
    generator.connect(host, user, password, dbname)

    print ("Table created")
    table = generator.create_table()
    generator.commit()

    print ("8K text generated")
    text = generator.generate_text(8192)
    generator.insert(table, 8192, text)
    generator.commit()


    print ("64k text generated")
    text = generator.generate_text(65536)
    generator.insert(table, 65536, text)
    generator.commit()


    print ("512K text generated")
    text = generator.generate_text(512 * 1024)
    generator.insert(table, 512 * 1024, text)
    generator.commit()


    print ("1MB text generated")
    text = generator.generate_text(1024 * 1024)
    generator.insert(table, 1024 * 1024, text)
    generator.commit()

    generator.close()





