# At the very top of your webserver_config.py
import pymysql
pymysql.install_as_MySQLdb()

# Rest of your webserver configuration follows...
