#!/usr/bin/python3
# pip install psycopg2-binary
import os
import psycopg2
import logging
from datetime import datetime

REFERENCE_ID = None  # Variable global

# Configuración de logging (opcional si usas la BD como único registro)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def set_reference_id(value):
    global REFERENCE_ID
    REFERENCE_ID = value

def log_tipo():
    """
    Registra eventos en una tabla de tipos de logs con código y observaciones.    
    """
    try:
        LOG_TABLE = "logs_tipos"  # Nombre de tu tabla de logs
        # Datos estructurados para inserción
        data = {
            "codigo": f"{datetime.now().strftime("%Y%m%d%H%M%S")}_RNT"
        }
        
        # Insertar en BD y obtener ID del registro (opcional)
        log_id = insertar(LOG_TABLE, data)
        if log_id:
            #logging.debug(f"Log registrado exitosamente: ID {log_id}")
            set_reference_id(log_id)
        
    except Exception as e:
        # Fallback a logging por consola si falla inserción
        logging.error(f"Fallo al insertar log en BD: {e}")

def logs_datos(mensaje, nivel='E'):
    """
    Registra eventos en una tabla de logs con clave foránea, timestamp y mensaje.
    
    Args:
        mensaje (str): Mensaje descriptivo del evento.
        nivel (str): Nivel del error. 
    """
    try:
        LOG_TABLE = "logs_datos"  # Nombre de tu tabla de logs
        # Datos estructurados para inserción
        datos = {
            "reference_id": REFERENCE_ID,
            "timestamp": datetime.now(),
            "nivel_logs_cgrc": nivel,
            "message": mensaje
        }
        
        # Insertar en BD y obtener ID del registro (opcional)
        log_id = insertar(LOG_TABLE, datos)
        if log_id:
            logging.debug(f"Log registrado exitosamente: ID {log_id}")
        return log_id
        
    except Exception as e:
        # Fallback a logging por consola si falla inserción
        logging.error(f"Fallo al insertar log en BD: {e}")
        return None

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

def insertar(table_name, raw_values):
    id = None
    sql = ''
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                # Obtener estructura de la tabla
                table_fields = get_table_columns(cur, table_name)
                
                # Convertimos los valores
                converted_values = []
                columns = []
                for field_name, field_type in table_fields:
                    if field_name in ('created_at', 'updated_at', 'id'):
                        continue  # Se generan automáticamente
                    value = raw_values.get(field_name)
                    converted = safe_convert(value, field_type)
                    converted_values.append(converted)
                    columns.append(field_name)

                # Preparamos SQL
                #columns = [f'"{name}"' for name, _ in table_fields if name not in ('created_at', 'updated_at', 'id')]
                placeholders = ['%s'] * len(converted_values)

                sql = f"""
                    INSERT INTO public.{table_name} ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                    RETURNING id;
                """

                #print(f"sql: {sql}, converted_values: {converted_values}")
                cur.execute(sql, converted_values)
                id = cur.fetchone()[0]
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"insertar: Error al insertar en la tabla: {table_name} ## sql: {sql} ## {error}")
    finally:
        if conn is not None:
            conn.close()
    return id

def actualizar(table_name, id, raw_values):
    sql = ''
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                table_fields = get_table_columns(cur, table_name)
                            
                # Convertimos los valores
                converted_values = []
                columns = []
                for field_name, field_type in table_fields:
                    if field_name in ('created_at', 'updated_at', 'id'):
                        continue  # Se generan automáticamente
                    value = raw_values.get(field_name)
                    converted = safe_convert(value, field_type)
                    converted_values.append(converted)
                    columns.append(field_name)

                # Crear SET parte de la consulta
                placeholders = ', '.join([f"{col} = %s" for col in columns])

                # Filtrar campos excluidos
                #update_columns = [col for col in columns if col not in ('created_at', 'updated_at', 'id')]

                # Construir consulta completa
                sql = f"""
                    UPDATE public.{table_name}
                    SET {placeholders}
                    WHERE id = %s;
                """

                # Preparar valores en orden correcto según campos de la tabla
                converted_values.append(id)  # Añadir el ID al final para el WHERE

                # Ejecutar
                #print(f"sql: {sql}, converted_values: {converted_values}")
                cur.execute(sql, converted_values)
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error al actualizar en la tabla: {table_name} ## sql: {sql} ### {error}")
        conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    connect()