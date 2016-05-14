import argparse
from bs4 import BeautifulSoup
import datetime

from utils import get_connection, load_ids_set, chunks

__author__ = 'Blyde'


class AmazonAppImporter(object):
    def __init__(self):
        self.date_base = get_connection()

    def importer(self, args):
        app_ids_list = list(load_ids_set(self.date_base))[args.after:]
        amazon_app_parser = AmazonAppParser()
        amazon_app_saver = AmazonAppSaver(self.date_base)

        for batch_app_ids in chunks(app_ids_list, 100):
            print 'Started to import batch:', len(batch_app_ids)
            app_detail_dict = amazon_app_parser.parser(args.date, batch_app_ids)
            amazon_app_saver.save(app_detail_dict)
            app_detail_dict.clear()
        self.date_base.close()


class AmazonAppParser(object):
    def parser(self, date_str, app_ids_list):
        app_detail_dict = dict()
        for app_id in app_ids_list:
            content = self._load_page(date_str, app_id)
            detail = self._parser(content)
            if detail:
                app_detail_dict[app_id] = detail
        return app_detail_dict

    def _load_page(self, date_str, app_id):
        read_file = open('res/{date}/{app_id}.txt'.format(date=date_str, app_id=app_id), 'r')
        content = read_file.read()
        return content

    def _parser(self, content):
        soup = BeautifulSoup(content, 'html.parser')
        try:
            name = soup.find(id='btAsinTitle').string.encode('utf-8')
            description = soup.find(id='mas-product-description').div.contents[0].encode('utf-8').replace('\"', '')
            if not name or not description:
                print 'Failed to parser app name and description'
                return None
            return {'name': name, 'description': description}
        except:
            print 'Failed find tags.'
            return None


class AmazonAppSaver(object):
    def __init__(self, date_base):
        self.date_base = date_base

    def save(self, app_detail_dict):
        cursor = self.date_base.cursor()
        for app_id, current_detail_dict in app_detail_dict.iteritems():
            last_detail_sql = "SELECT name, description FROM tb_products WHERE app_id='{}'".format(app_id)
            cursor.execute(last_detail_sql)
            desc = cursor.description
            last_detail_dict = {}
            for key, value in zip(desc, cursor.fetchone()):
                last_detail_dict[key[0]] = value if value else 'NULL'

            if self._need_to_update(last_detail_dict, current_detail_dict):
                try:
                    self._update_app(app_id, current_detail_dict)
                    print 'Succeed update app'
                    self._save_event(app_id, last_detail_dict, current_detail_dict)
                    self.date_base.commit()
                    print 'Succeed save event'
                except:
                    self.date_base.rollback()

    @staticmethod
    def _need_to_update(last_detail_dict, current_detail_dict):
        if last_detail_dict['name'] != current_detail_dict['name']:
            return True

    def _save_event(self, app_id, last_detail_dict, current_detail_dict):
        """Save event if found detail has been changed"""
        cursor = self.date_base.cursor()
        keys = ['app_id', 'old_name', 'old_desc', 'new_name', 'new_desc', 'date_time']
        val = [app_id, last_detail_dict['name'], last_detail_dict['description'], current_detail_dict['name'],
               current_detail_dict['description'], datetime.datetime.now().strftime('%Y-%m-%d')]
        insert_sql = """INSERT INTO tb_event ({}) VALUES (\"{}\")""".format(','.join(keys), "\",\"".join(val))
        cursor.execute(insert_sql)

    def _update_app(self, app_id, current_detail_dict):
        cursor = self.date_base.cursor()
        values = list()
        for key, value in current_detail_dict.iteritems():
            values.append("{}=\"{}\"".format(key, value))
        update_sql = """UPDATE tb_products SET {} WHERE app_id='{}'""".format(','.join(values), app_id)
        cursor.execute(update_sql)


if __name__ == '__main__':
    par = argparse.ArgumentParser()
    par.add_argument(
        '--after', dest='after', type=int, required=True,
        help='Name of file'
    )
    par.add_argument(
        '--date', dest='date', type=str, required=True,
        help='Name of file'
    )
    args = par.parse_args()
    amazon_app_importer = AmazonAppImporter()
    amazon_app_importer.importer(args)
    print 'Succeed to finish.'
