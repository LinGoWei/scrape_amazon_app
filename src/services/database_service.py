import MySQLdb
import datetime

__author__ = 'Blyde'

db_info = {
    'host': "localhost",
    'user': "lgw",
    'passwd': "",
    'db': "app_db",
}


class DatabaseService(object):
    def __init__(self):
        self.database_connect = MySQLdb.connect(host=db_info['host'],
                                                user=db_info['user'],
                                                passwd=db_info['passwd'],
                                                db=db_info['db'])

    def close(self):
        self.database_connect.close()

    def save_event(self, app_id, last_detail_dict, current_detail_dict):
        """Save event"""
        cursor = self.database_connect.cursor()
        keys = ['app_id', 'old_name', 'old_desc', 'new_name', 'new_desc', 'date_time']
        val = [app_id,
               last_detail_dict['name'],
               last_detail_dict['description'],
               current_detail_dict['name'],
               current_detail_dict['description'],
               datetime.datetime.now().strftime('%Y-%m-%d %H:%M')]
        insert_sql = """INSERT INTO tb_event ({}) VALUES (\"{}\")""".format(','.join(keys), "\",\"".join(val))
        cursor.execute(insert_sql)
        self.database_connect.commit()

    def save_icon_event(self, app_id, last_detail_dict, current_detail_dict):
        cursor = self.database_connect.cursor()
        keys = ['app_id', 'old_value', 'new_value', 'date_time']
        val = [app_id,
               last_detail_dict['icon_url'],
               current_detail_dict['icon_url'],
               datetime.datetime.now().strftime('%Y-%m-%d %H:%M')]
        insert_sql = """INSERT INTO tb_icon_event ({}) VALUES (\"{}\")""".format(','.join(keys), "\",\"".join(val))
        cursor.execute(insert_sql)
        self.database_connect.commit()

    def update_app_detail(self, market, app_id, current_detail_dict):
        cursor = self.database_connect.cursor()
        values = list()
        for key, value in current_detail_dict.iteritems():
            values.append("{}=\"{}\"".format(key, value))

        update_sql = """UPDATE tb_products SET {} WHERE app_id='{}' AND market='{}' """.format(
            ','.join(values), app_id, market)
        cursor.execute(update_sql)
        self.database_connect.commit()

    def get_app_detail(self, market, app_id):
        """Get app detail from db"""
        cursor = self.database_connect.cursor()
        last_detail_sql = "SELECT name, description, icon_url"\
                          " FROM tb_products" \
                          " WHERE app_id='{}'" \
                          "   AND market='{}'".format(app_id, market)
        cursor.execute(last_detail_sql)
        desc = cursor.description
        last_detail_dict = {}
        for key, value in zip(desc, cursor.fetchone()):
            last_detail_dict[key[0]] = value if value else 'NULL'
        return last_detail_dict

    def import_ids(self, market, app_ids):
        cursor = self.database_connect.cursor()
        keys = ['market', 'app_id']
        insert_sql_template = """INSERT INTO tb_products (market, app_id) VALUES (\"{}\", \"{}\")"""
        for app_id in app_ids:
            if not self.exist(market, app_id):
                insert_sql = insert_sql_template.format(market, app_id)
                cursor.execute(insert_sql)
        self.database_connect.commit()

    def exist(self, market, app_id):
        cursor = self.database_connect.cursor()
        exist_sql_template = """SELECT EXISTS(SELECT 1 FROM tb_products WHERE market=\"{}\" AND app_id=\"{}\" limit 1)"""
        exists_sql = exist_sql_template.format(market, app_id)
        cursor.execute(exists_sql)
        is_exist = cursor.fetchone()[0]
        if is_exist == 1:
            return True
        return False

    def load_ids(self, market, start_id, end_id):
        cursor = self.database_connect.cursor()
        select_sql = "SELECT app_id FROM tb_products WHERE market='{}' and id between {} and {}".format(market, start_id, end_id)
        cursor.execute(select_sql)
        app_ids = list()
        for row in cursor.fetchall():
            app_ids.append(row[0])
        return app_ids
