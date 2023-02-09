import psycopg2 as psycopg2
from configparser import ConfigParser
import argparse
import datetime

conn = psycopg2.connect("dbname=bjorndb user=postgres password=")


def config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def create_tables():
    command = """
        CREATE TABLE books (
            id SERIAL PRIMARY KEY,
            date_added date DEFAULT now(),
            date_finished date DEFAULT NULL,
            title VARCHAR(255) UNIQUE NOT NULL,
            author VARCHAR(255) NOT NULL,
            days_read INTEGER
        );
        """

    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_books():
    """ query data from the vendors table """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("SELECT * FROM books ORDER BY id")
        row = cur.fetchone()

        while row is not None:
            print(row)
            row = cur.fetchone()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()



def insert_book(book_title, book_author):
    """ insert a new vendor into the vendors table """
    sql = """INSERT INTO books (title, author)
             VALUES(%s, %s) RETURNING id;"""
    conn = None
    book_id = None

    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (book_title, book_author))
        book_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return book_id

def delete_book(book_id):
    """ delete part by part id """
    conn = None
    rows_deleted = 0
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("DELETE FROM books WHERE id = %s", (book_id,))
        rows_deleted = cur.rowcount
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return rows_deleted

def delete_all_books():
    """ delete all books and restart id count """
    conn = None
    rows_deleted = 0
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("TRUNCATE books RESTART IDENTITY")
        rows_deleted = cur.rowcount
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return rows_deleted



def finish_book(book_id):
    """ finish book """
    sql = """ UPDATE books
                SET date_finished = now()
                WHERE id = %s
                RETURNING title, author, date_added, date_finished;"""
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (book_id,))
        title, author, added, finished = cur.fetchall()[0]
        time_spend_reading = finished - added
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return time_spend_reading.days

def update_time_spent(book_id, days):
    """ update days spent reading the book """
    sql = """ UPDATE books
                   SET days_read = %s
                   WHERE id = %s;"""
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (days, book_id,))

        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':


    #create_tables()

    parser = argparse.ArgumentParser()
    parser.add_argument('--author', '-a', help='Book author')
    parser.add_argument('--title', '-t', help='Book author')
    parser.add_argument('--finish', '-f', help='Finished [id]')
    args = parser.parse_args()


    if args.finish is not None:
        if isinstance(args.finish, str):
            days = finish_book(args.finish)
            update_time_spent(args.finish, days)
    if args.title is not None and args.author is not None:
        if isinstance(args.title, str) and isinstance(args.author, str):
            insert_book(args.title, args.author)

    else:
        get_books()

