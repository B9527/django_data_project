class ImportCommandBuilder(object):
    
    def __init__(self):
        self.split_by = None
        self.fetch_size = 500
        self.mcount = None
        self.delimiter = '\\0x01'

    def set_delimiter(self, delimiter):
        self.delimiter = delimiter

    def set_username(self, username):
        self.username = username

    def set_jdbc_url(self, jdbc_url):
        self.jdbc_url = jdbc_url

    def set_password(self, password):
        self.password = password

    def set_table(self, table):
        self.table = table

    def set_columns(self, columns):
        if isinstance(columns, list):
            self.columns = ",".join(columns)
        else:
            self.columns = columns

    def set_fetch_size(self, fetch_size):
        self.fetch_size = fetch_size

    def set_target_dir(self, target_dir):
        self.target_dir = target_dir

    def set_split_by(self, column):
        self.split_by = column

    def set_mapper_count(self, count):
        self.mcount = count


    def build(self):
        prefix = (u'sudo -u hive sqoop import --connect "{jdbc_url}" '
                    '--username {username} --password {password} --table {src_table} '
                    '--columns {columns}'.format(jdbc_url=self.jdbc_url, username=self.username,
                        password=self.password, src_table=self.table, columns=self.columns))

        option = ""
        if self.split_by is None:
            option += "-m 1 "
        else:
            option += "--split-by %s " % self.split_by
            if self.mcount is not None:
                option += "-m %s " % self.mcount

        if self.fetch_size is not None:
            option += "--fetch-size %s " % self.fetch_size

        suffix = '--target-dir {target_dir} --fields-terminated-by "{delimiter}" --lines-terminated-by "\\n"'.format(target_dir=self.target_dir, delimiter=self.delimiter)

        return prefix + " " + option + suffix

class JdbcBuidler(object):
    
    def __init__(self):
        self.default_port = {"oracle": 1521, "postgresql": 5432,
                     "sqlserver": 1433, "mysql": 3306}
        self._port = None

    def set_port(self, port):
        self._port =port

    @property
    def port(self):
        return self._port or self.default_port.get(self.driver)

    def set_username(self, username):
        self.username = username

    def set_password(self, password):
        self.password = password

    def set_host(self, host):
        self.host = host

    def set_database(self, database):
        self.database = database

    def set_driver(self, driver):
        self.driver = driver

    def build(self):
        if self.driver == "sqlserver":
            return "jdbc:sqlserver://{host}:{port};databaseName={database}".format(host=self.host, port=self.port, database=self.database)
        elif self.driver == "oracle":
            return "jdbc:oracle:thin:@//{host}:{port}/{database}".format(host=self.host, port=self.port, database=self.database)
        else:
            return "jdbc:{driver}://{host}:{port}/{database}".format(driver=self.driver, host=self.host, port=self.port, database=self.database)

    


if __name__ == "__main__":
    builder = ImportCommandBuilder()
    builder.set_jdbc_url("jdbc:oracle:thin:@//192.168.2.95:1521/orcl")
    builder.set_username("shanghaishi")
    builder.set_password("shanghaishi")
    builder.set_table("shanghaipassport")
    builder.set_split_by("id")
    builder.set_mapper_count(8)
    builder.set_columns("PYXM,CSRQ,ZJLB,ZJHM,PIN,XM,XB,CSD,WHCD,HKSZD,ROWID1,XZZ,SPJG,QFRQ,CJSY,ZJYXQZ,ACCEPT_ID,QWGJDQ,DW,SSPCS,SFZHM")
    builder.set_fetch_size(100)
    builder.set_target_dir("/tmp/sqoop/shanghaipassport-20170928-154150")
    print builder.build()
