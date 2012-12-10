"""
Collection of query wrappers / abstractions to both facilitate data
retrieval and to reduce dependency on DB-specific API.
"""
from datetime import datetime, date

import numpy as np
import traceback

from pandas.core.datetools import format as date_format
from pandas.core.api import DataFrame, isnull

#------------------------------------------------------------------------------
# Helper execution function


def execute(sql, con, retry=True, cur=None, params=None):
    """
    Execute the given SQL query using the provided connection object.

    Parameters
    ----------
    sql: string
        Query to be executed

    Returns
    -------
    Cursor object
    """
    try:
        if cur is None:
            cur = con.cursor()

        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        return cur
    except Exception:
        try:
            con.rollback()
        except Exception:  # pragma: no cover
            pass

        print 'Error on sql %s' % sql
        raise


def _safe_fetch(cur):
    try:
        result = cur.fetchall()
        if not isinstance(result, list):
            result = list(result)
        return result
    except Exception, e:  # pragma: no cover
        excName = e.__class__.__name__
        if excName == 'OperationalError':
            return []


def tquery(sql, con=None, cur=None, retry=True):
    """
    Returns list of tuples corresponding to each row in given sql
    query.

    If only one column selected, then plain list is returned.

    Parameters
    ----------
    sql: string
        SQL query to be executed
    con: SQLConnection or DB API 2.0-compliant connection
    cur: DB API 2.0 cursor

    Provide a specific connection or a specific cursor if you are executing a
    lot of sequential statements and want to commit outside.
    """
    cur = execute(sql, con, cur=cur)
    result = _safe_fetch(cur)

    if con is not None:
        try:
            cur.close()
            con.commit()
        except Exception, e:
            excName = e.__class__.__name__
            if excName == 'OperationalError':  # pragma: no cover
                print 'Failed to commit, may need to restart interpreter'
            else:
                raise

            traceback.print_exc()
            if retry:
                return tquery(sql, con=con, retry=False)

    if result and len(result[0]) == 1:
        # python 3 compat
        result = list(list(zip(*result))[0])
    elif result is None:  # pragma: no cover
        result = []

    return result


def uquery(sql, con=None, cur=None, retry=True, params=()):
    """
    Does the same thing as tquery, but instead of returning results, it
    returns the number of rows affected.  Good for update queries.
    """
    cur = execute(sql, con, cur=cur, retry=retry, params=params)

    result = cur.rowcount
    try:
        con.commit()
    except Exception, e:
        excName = e.__class__.__name__
        if excName != 'OperationalError':
            raise

        traceback.print_exc()
        if retry:
            print 'Looks like your connection failed, reconnecting...'
            return uquery(sql, con, retry=False)
    return result


def read_frame(sql, con, index_col=None, coerce_float=True):
    """
    Returns a DataFrame corresponding to the result set of the query
    string.

    Optionally provide an index_col parameter to use one of the
    columns as the index. Otherwise will be 0 to len(results) - 1.

    Parameters
    ----------
    sql: string
        SQL query to be executed
    con: DB connection object, optional
    index_col: string, optional
        column name to use for the returned DataFrame object.
    """
    cur = execute(sql, con)
    rows = _safe_fetch(cur)
    columns = [col_desc[0] for col_desc in cur.description]

    cur.close()
    con.commit()

    result = DataFrame.from_records(rows, columns=columns,
                                    coerce_float=coerce_float)

    if index_col is not None:
        result = result.set_index(index_col)

    return result

frame_query = read_frame


def write_frame(frame, name, con, flavor='sqlite', if_exists='fail'):
    """
    Write records stored in a DataFrame to a SQL database.

    Parameters
    ----------
    frame: DataFrame
    name: name of SQL table
    conn: an open SQL database connection object
    flavor: {'sqlite', 'mysql', 'oracle'}, default 'sqlite' 
    if_exists: {'fail', 'replace', 'append'}, default 'fail'
        fail: If table exists, do nothing.
        replace: If table exists, drop it, recreate it, and insert data.
        append: If table exists, insert data.
    """
    
    exists = table_exists(name, con, flavor)
    if if_exists == 'fail' and exists:
        raise ValueError, "Table '%s' already exists." % name
    if if_exists == 'append' and not exists:
        raise ValueError, "Table '%s' does not exist. Cannot append." % name
    if if_exists == 'replace' and exists:
        cur = con.cursor()
        cur.execute("DROP TABLE %s" % name)
        cur.close()
    if if_exists == 'replace' or not exists:
        cur = con.cursor()
        create_table = get_schema(frame, name, flavor)
        cur.execute(create_table)
        cur.close()

    cur = con.cursor()
    # Replace spaces in DataFrame column names with _.
    safe_names = [s.replace(' ', '_').strip() for s in frame.columns]
    if flavor == 'sqlite':
        bracketed_names = ['[' + column + ']' for column in safe_names]
        col_names = ','.join(bracketed_names)
        wildcards = ','.join(['?'] * len(safe_names))
        insert_query = 'INSERT INTO %s (%s) VALUES (%s)' % (
            name, col_names, wildcards)
        data = [tuple(x) for x in frame.values]
        cur.executemany(insert_query, data)
    elif flavor == 'mysql':
        col_names = ','.join(safe_names)
        wildcards = ','.join([r'%s'] * len(safe_names))
        insert_query = "INSERT INTO %s (%s) VALUES (%s)" % (
            name, col_names, wildcards)
        print insert_query
        data = [tuple(x) for x in frame.values]
        cur.executemany(insert_query, data)
    elif flavor == 'oracle':
        col_names = ','.join(safe_names)
        col_pos = ', '.join([':'+str(i+1) for i,f in enumerate(safe_names)])
        insert_query = "INSERT INTO %s (%s) VALUES (%s)" % (
            name, col_names, col_pos)
        data = [sequence2dict(record) for record in frame.values]
        cur.executemany(insert_query, data)
    else:
        raise NotImplementedError
    cur.close()
    con.commit()

def table_exists(name, con, flavor):
    if flavor == 'sqlite':
        query = """SELECT name FROM sqlite_master 
                   WHERE type='table' AND name='%s';""" % name
    elif flavor == 'mysql':
        query = "SHOW TABLES LIKE '%s'" % name
    elif flavor == 'postgresql':
        query = "SELECT * FROM pg_tables WHERE tablename='%s';" % name
    elif flavor == 'oracle':
        query ="""SELECT table_name FROM user_tables
                  WHERE table_name='%s'""" % name.upper()
    elif flavor == 'odbc':
        raise NotImplementedError
    else:
        raise NotImplementedError
    return len(tquery(query, con)) > 0

def get_sqltype(pytype, flavor):
    sqltype = {'mysql': 'VARCHAR',
                'oracle': 'VARCHAR2',
                'sqlite': 'TEXT',
                'postgresql': 'TEXT'}
    if issubclass(pytype, np.number):
        sqltype['mysql'] = 'FLOAT'
        sqltype['oracle'] = 'NUMBER'
        sqltype['sqlite'] = 'REAL'
        sqltype['postgresql'] = 'NUMBER'
        if issubclass(pytype, np.integer):
            # Assume 32 bit. TODO: Refine further down.
            sqltype['mysql'] = 'INT'
            sqltype['oracle'] = 'PLS_INTEGER'
            sqltype['sqlite'] = 'INTEGER'
            sqltype['postgresql'] = 'INTEGER' 
    if issubclass(pytype, np.datetime64) or pytype is datetime:
        # Caution: np.datetime64 is also a subclass of np.number.
        sqltype['mysql'] = 'DATETIME'
        sqltype['oracle'] = 'DATE'
        sqltype['sqlite'] = 'TIMESTAMP'
        sqltype['postgresql'] = 'TIMESTAMP'
    if pytype is datetime.date:
        sqltype['mysql'] = 'DATE'
        sqltype['oracle'] = 'DATE'
        sqltype['sqlite'] = 'TIMESTAMP'
        sqltype['postgresql'] = 'TIMESTAMP'
    return sqltype[flavor]

def get_schema(frame, name, flavor, keys=None):
    "Return a CREATE TABLE statement to suit the contents of a DataFrame."
    lookup_type = lambda dtype: get_sqltype(dtype.type, flavor)
    # Replace spaces in DataFrame column names with _.
    safe_columns = [s.replace(' ', '_').strip() for s in frame.dtypes.index]
    column_types = zip(safe_columns, map(lookup_type, frame.dtypes))
    if flavor == 'sqlite':
        columns = ',\n  '.join('[%s] %s' % x for x in column_types)
    else:
        columns = ',\n  '.join('%s %s' % x for x in column_types)
    keystr = ''
    if keys is not None:
        if isinstance(keys, basestring):
            keys = (keys,)
        keystr = ', PRIMARY KEY (%s)' % ','.join(keys)
    template = """CREATE TABLE %(name)s (
                  %(columns)s
                  %(keystr)s
                  );"""
    create_statement = template % {'name': name, 'columns': columns, 'keystr': keystr}
    if flavor == 'oracle':
        create_statement = create_statment.replace(';', '')
    return create_statement

def sequence2dict(seq):
    """Helper function for cx_Oracle.

    For each element in the sequence, creates a dictionary item equal
    to the element and keyed by the position of the item in the list.
    >>> sequence2dict(("Matt", 1))
    {'1': 'Matt', '2': 1}

    Source:
    http://www.gingerandjohn.com/archives/2004/02/26/cx_oracle-executemany-example/
    """
    d = {}
    for k,v in zip(range(1, 1 + len(seq)), seq):
        d[str(k)] = v
    return d

def test_sqlite(name, testdf):
    print '\nsqlite, using detect_types=sqlite3.PARSE_DECLTYPES for datetimes'
    import sqlite3
    with sqlite3.connect('test.db', detect_types=sqlite3.PARSE_DECLTYPES) as conn:
        #conn.row_factory = sqlite3.Row
        write_frame(testdf, name, con=conn, flavor='sqlite', if_exists='replace')
        df_sqlite = read_db('select * from '+name, con=conn)    
        print 'loaded dataframe from sqlite', len(df_sqlite)   
    print 'done with sqlite'


def test_oracle(name, testdf):
    print '\nOracle'
    import cx_Oracle
    with cx_Oracle.connect('YOURCONNECTION') as ora_conn:
        testdf['d64'] = np.datetime64( testdf['hire_date'] )
        write_frame(testdf, name, con=ora_conn, flavor='oracle', if_exists='replace')    
        df_ora2 = read_db('select * from '+name, con=ora_conn)    

    print 'done with oracle'
    return df_ora2
   
    
def test_postgresql(name, testdf):
    #from pg8000 import DBAPI as pg
    import psycopg2 as pg
    print '\nPostgresQL, Greenplum'    
    pgcn = pg.connect(YOURCONNECTION)
    print 'df frame_query'
    try:
        write_frame(testdf, name, con=pgcn, flavor='postgresql', if_exists='replace')   
        print 'pg copy_from'    
        postgresql_copy_from(testdf, name, con=pgcn)    
        df_gp = read_db('select * from '+name, con=pgcn)    
        print 'loaded dataframe from greenplum', len(df_gp)
    finally:
        pgcn.commit()
        pgcn.close()
    print 'done with greenplum'

 
def test_mysql(name, testdf):
    import MySQLdb
    print '\nmysql'
    cn= MySQLdb.connect(YOURCONNECTION)    
    try:
        write_frame(testdf, name='test_df', con=cn, flavor='mysql', if_exists='replace')
        df_mysql = read_db('select * from '+name, con=cn)    
        print 'loaded dataframe from mysql', len(df_mysql)
    finally:
        cn.close()
    print 'mysql done'


##############################################################################

if __name__=='__main__':

    print """sqlite should work out of the box. For the other test, you must
             install the driver and provide a connection string."""
    
    test_data = {
        "name": [ 'Joe', 'Bob', 'Jim', 'Suzy', 'Cathy', 'Sarah' ],
        "hire_date": [ datetime(2012,1,1), datetime(2012,2,1), datetime(2012,3,1), datetime(2012,4,1), datetime(2012,5,1), datetime(2012,6,1) ],
        "erank": [ 1,   2,   3,   4,   5,   6  ],
        "score": [ 1.1, 2.2, 3.1, 2.5, 3.6, 1.8]
    }
    df = DataFrame(test_data)

    name='test_df'
    test_sqlite(name, df)
    #test_oracle(name, df)
    #test_postgresql(name, df)    
    #test_mysql(name, df)        
    
    print 'done'
