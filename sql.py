from psycopg2 import pool
DATABASE_URL = "URL"
if not DATABASE_URL:
    raise RuntimeError("Please set DATABASE_URL environment variable (postgresql://user:pass@host:port/db)")
try:
    pg_pool = pool.ThreadedConnectionPool(1, 20, dsn=DATABASE_URL)
except Exception as e:
    raise RuntimeError(f"Failed to create Postgres pool: {e}")
def get_conn():
    conn = pg_pool.getconn()
    return conn
def put_conn(conn):
    try:
        pg_pool.putconn(conn)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
def initdb():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            year INT,
            rating NUMERIC(3,1)
        );
        """)
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        put_conn(conn)
def insertmovies():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO movies (title, year, rating)
            VALUES ('Inception', 2010, 8.8),
                ('The Batman', 2022, 7.9),
                ('Interstellar', 2014, 8.6);
        """)
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        put_conn(conn)
def getmovies(minyear):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM movies WHERE year > %s;", (minyear,))
        movies = cur.fetchall()
        cur.execute("SELECT * FROM movies ORDER BY rating DESC LIMIT 1;")
        top_movie = cur.fetchone()
        cur.close()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        put_conn(conn)
    