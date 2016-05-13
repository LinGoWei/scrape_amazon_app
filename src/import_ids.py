import argparse
import csv
from utils import chunks, get_connection

__author__ = 'Blyde'


def import_ids(file_name, batch=False):
    ids = read_ids(file_name)
    print len(ids)

    db = get_connection()
    cur = db.cursor()
    if batch:
        batch_insert(ids, db, cur)
    else:
        for id in ids:
            try:
                insert_sql = "INSERT INTO tb_products(app_id) VALUES ('{}')".format(id)
                cur.execute(insert_sql)
                db.commit()
            except:
                db.rollback()
    db.close()


def batch_insert(ids, db, cur):
    for batch_ids in chunks(list(ids), 1000):
        print 'batch ids size:', len(batch_ids)
        try:
            batch_insert_sql = "INSERT INTO tb_products(app_id) VALUES {}".format("('{}')".format("'),('".join(batch_ids)))
            cur.execute(batch_insert_sql)
            db.commit()
            print 'batch insert successfully.'
        except:
            db.rollback()


def read_ids(file_name):
    with file(file_name, 'r') as csv_file:
        reader = csv.reader(csv_file)
        batch_ids = list()
        for app_id in reader:
            batch_ids.extend(app_id)
        return set(batch_ids)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-f', '--filename', dest='filename', type=str, required=True,
        help='Name of file '
    )
    args = parser.parse_args()
    import_ids(args.filename, True)
    print 'Succeed to finish.'
