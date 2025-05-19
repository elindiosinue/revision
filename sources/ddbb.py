#!/usr/bin/python3
# pip install psycopg2-binary
import os
import psycopg2
from datetime import datetime

def connect():
    conn = None
    try:
        # Conectamos con el servidor PostgreSQL
        #print(f"Conectando con PostgreSQL... {os.environ["DB_HOST"]} {os.environ["DB_SCHEMA"]} {os.environ["DB_USER"]} {os.environ["DB_PASS"]}")
        conn = psycopg2.connect(
            host=os.environ["DB_HOST"],
            database=os.environ["DB_SCHEMA"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASS"],
            port="5432")

        # creamos un cursor
        #cur = conn.cursor()

        # Ejecutamos una sentencia SQL
        #print('PostgreSQL database version:')
        #cur.execute('SELECT version()')

        # Mostramos la versión de PostgreSQL que hemos solicitado con la sentencia anterior
        #db_version = cur.fetchone()
        #print(db_version)

        # Cerramos la comunicación con PostgreSQL
        #cur.close()
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def safe_convert(value, target_type):
    if value is None or value == '':
        return None
    try:
        if target_type == int:
            return int(value)
        elif target_type == float:
            return float(str(value).replace('.', '').replace(',', '.')) if isinstance(value, str) else float(value)
        elif target_type == datetime.date:
            if isinstance(value, str):
                formatos = [
                    "%d/%m/%Y",     # Formato DD/MM/YYYY
                    "%d-%m-%Y",     # Formato DD-MM-YYYY
                    "%d.%m.%Y",      # Formato DD.MM.YYYY
                    "%d de %B de %Y",     # Formato YYYY-MM-DD
                    "%Y-%m-%d",     # Formato YYYY-MM-DD
                    "%m.%d.%Y",      # Formato MM.DD.YYYY
                ]
                for fmt in formatos:
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
                return None
            elif isinstance(value, datetime):
                return value.date()
            #return value
        elif target_type == datetime:
            #return datetime.fromisoformat(value.strip())
            if isinstance(value, str):
                formatos = [
                    "%d/%m/%Y",     # Formato DD/MM/YYYY
                    "%d-%m-%Y",     # Formato DD-MM-YYYY
                    "%d.%m.%Y",      # Formato DD.MM.YYYY
                    "%d de %B de %Y",     # Formato YYYY-MM-DD
                    "%Y-%m-%d",     # Formato YYYY-MM-DD
                    "%m.%d.%Y",      # Formato MM.DD.YYYY
                ]
                for fmt in formatos:
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
                return None
            elif isinstance(value, datetime):
                return value
            #return value
        elif target_type == str:
            return str(value).strip() if value else None
        else:
            return value
    except Exception as e:
        print(f"Error al convertir '{value}' a {target_type}: {e}")
        return None


def get_table_columns(cur, table_name='rnts_resumenes', schema='public'):
    cur.execute("""
        SELECT 
            column_name, 
            data_type 
        FROM 
            information_schema.columns
        WHERE 
            table_name = %s AND 
            table_schema = %s AND
            column_name not in ('id', 'created_at', 'updated_at')
        ORDER BY ordinal_position;
    """, (table_name, schema))

    columns = cur.fetchall()
    
    type_mapping = {
        'integer': int,
        'bigint': int,
        'character varying': str,
        'date': datetime.date,
        'timestamp without time zone': datetime,
        'timestamp with time zone': datetime,
        'numeric': float,
        'real': float,
        'double precision': float,
        'text': str,
        'boolean': bool,
        'uuid': str,
    }

    fields = []
    for col in columns:
        name, sql_type = col
        python_type = type_mapping.get(sql_type.lower(), str)
        fields.append((name, python_type))
    
    return fields

if __name__ == '__main__':
    connect()