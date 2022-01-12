
import pymysql
import pandas as pd
from sqlalchemy import create_engine


class MysqlDb(object):
    def __init__(self, user, password, host, database, port=3306):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.port = port
        connect_str = 'mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8' % (self.user,
                                                                       self.password,
                                                                       self.host,
                                                                       self.port,
                                                                       self.database
                                                                       )
        self.yconnect = create_engine(connect_str)

    # 从数据库中获取实时数据
    def get_data_from_mysql(self, sql):
        con = pymysql.Connect(host=self.host,
                              user=self.user,
                              password=self.password,
                              db=self.database,
                              charset='utf8',
                              port=int(self.port))
        data = pd.read_sql(sql, con)
        con.close()
        return data

    # 上传数据到数据库中
    def data_to_database(self, data, table_name, index_type):
        data.to_sql(table_name,
                    self.yconnect,
                    if_exists='append',
                    index=index_type)

    # commit_sql
    def commit_sql(self, sql):
        con = pymysql.Connect(host=self.host,
                              user=self.user,
                              password=self.password,
                              db=self.database,
                              charset='utf8',
                              port=int(self.port))
        cur = con.cursor()
        cur.execute(sql)
        con.commit()
        con.close()


mysql_conf = {'host': '101.201.46.114',
            'user': 'crypto_rw',
            'password': 'crypto_rw_password',
            'database': 'ido_server_bak',
            'port': 3306}

mysql_con = MysqlDb(host=mysql_conf.get('host'),
                    user=mysql_conf.get('user'),
                    password=mysql_conf.get('password'),
                    database=mysql_conf.get('database')
                    )