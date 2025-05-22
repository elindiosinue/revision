#!/usr/bin/python3
#pip install PyPDF2
#apt -y install build-essential libpoppler-cpp-dev pkg-config python3-dev
#pip3 install pdftotext
#pip3 install pikepdf
# importing required modules 
import sys
import os
import argparse
import csv
import re
import logging
import psycopg2
import pdftotext
import pikepdf
from datetime import datetime
from pathlib import Path  # Usar Path para manejo correcto de rutas
from os.path import isfile, isdir
from sources.ddbb import *
from sources.renombrar import renombrar
from sources.fechas import formatoFechas

rootDir = '/opt/TC/procesar'
saveDir = '/opt/TC'
# Configuración de logging (opcional si usas la BD como único registro)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def descrypt(fichero, dir):
    nombre_fichero = Path(fichero).stem  # Elimina la extensión sin asumir '.pdf'
    # Construir rutas con Path
    directorio = Path(dir)
    archivo_entrada = directorio / fichero  # Ruta de entrada

    try:
        with connect() as conn:
            with conn.cursor() as cur:
                conn.set_client_encoding('UTF-8')
                sql = f"select e.codigo, e.nombre, c.codigo, c.denominacion, c.clave from centros c, empresas e where c.empresa_id = e.id and c.clave is not null and e.codigo is not null"
                #print(f"sql: {sql}")
                cur.execute(sql)
                rows = cur.fetchall()
                l_error = False
                for row in rows:
                    try:
                        codigo_empresa = row[0]
                        codigo_centro = row[2]
                        clave_centro = row[4]

                        #print(f"Codigo centro: {codigo_centro}, Codigo empresa: {codigo_empresa}")  # ¿Hay caracteres no-ASCII?
                        
                        nombre_salida = f"{codigo_centro}_{codigo_empresa}_{nombre_fichero}.pdf"
                        archivo_salida = directorio / nombre_salida  # Ruta de salida

                        pdf = pikepdf.open(archivo_entrada, password=clave_centro)
                        pdf.save(archivo_salida)
                        l_error = False
                        os.remove(archivo_entrada)
                        #print(f"descrypt {archivo_salida} desencriptado exitosamente.")
                        return nombre_salida
                    except (pikepdf.PasswordError) as error:
                        l_error = True
                        continue
                        #print(f"descrypt {nombre_fichero} Contraseña incorrecta. # error: {error}")
                if l_error:
                    print(f"descrypt {nombre_fichero} Contraseña incorrecta.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f'descrypt fichero: {nombre_fichero} # error: {error}')
    finally:
        if conn is not None:
            conn.close()

    return None

def pdfToText(fichero, dir):
    nombre_fichero = Path(fichero).stem  # Elimina la extensión sin asumir '.pdf'

    # Construir rutas con Path
    directorio = Path(dir)
    archivo_entrada = directorio / fichero  # Ruta de entrada
    nombre_salida = f"{nombre_fichero}.txt"
    archivo_salida = directorio / nombre_salida  # Ruta de salida
    
    if not os.path.exists(archivo_salida):
        # creating a pdf reader object 
        with open(archivo_entrada, "rb") as f:
            pdf = pdftotext.PDF(f, physical=True)

        output_text = ''

        # All pages
        for text in pdf:
            output_text += text

        # Write the extracted text to a text file
        with open(archivo_salida, 'w') as txt_file:
            txt_file.write(output_text)

        return nombre_salida

    return None

def pdfToCsvRNT(fichero, dir):
    nombre_fichero = Path(fichero).stem  # Elimina la extensión sin asumir '.pdf'

    # Construir rutas con Path
    directorio = Path(dir)
    archivo_entrada = directorio / fichero  # Ruta de entrada

    pagina_aux = None
    # RNT Resumenes
    rnts_resumen_id = None
    num_autorizacion = None
    razon_social = None
    codigo_empresario = None
    codigo_cuenta_cotizacon = None
    num_liquidacion = None
    periodo = None
    periodo_desde = None
    periodo_hasta = None
    num_trabajadores = None
    calificador = None
    liquidacion = None
    fecha_control = None
    entidad_atep = None
    referencia = None
    fecha = None
    hora = None
    huella = None
    pagina = 1
    total_pagina = None
    suma_base_cont_com = None
    suma_comp_enf_com = None
    suma_acci_trab = None
    empresa_id = None
    empresa_codigo = None
    centro_id = None
    centro_codigo = None
    ###
    sql = ''

    # creating a pdf reader object 
    #print(f"pdfToCsvRNT({fichero}, {dir})")
    if os.path.exists(archivo_entrada):
        #print(f"pdfToCsvRNT fichero: {fichero}")
        try:
            with open(archivo_entrada) as f:
                csv_line = []
                datos_referencia = False
                linea_rnt = False
                table_name = 'rnts_resumenes'

                # RNT Datos
                naf = None
                ipf = None
                caf = None
                ####

                raw_values = {
                                "num_autorizacion": num_autorizacion,
                                "razon_social": razon_social,
                                "codigo_empresario": codigo_empresario,
                                "codigo_cuenta_cotizacon": codigo_cuenta_cotizacon,
                                "num_liquidacion": num_liquidacion,
                                "periodo": periodo,
                                "periodo_desde": periodo_desde,
                                "periodo_hasta": periodo_hasta,
                                "num_trabajadores": num_trabajadores,
                                "calificador": calificador,
                                "liquidacion": liquidacion,
                                "fecha_control": fecha_control,
                                "entidad_atep": entidad_atep,
                                "referencia": referencia,
                                "fecha": fecha,
                                "hora": hora,
                                "huella": huella,
                                "pagina": pagina,
                                "total_pagina": total_pagina,
                                "suma_base_cont_com": suma_base_cont_com,
                                "suma_comp_enf_com": suma_comp_enf_com,
                                "suma_acci_trab": suma_acci_trab,
                                "empresa_id": empresa_id,
                                "centro_id": centro_id,
                            }
                
                rnts_resumen_id = insertar(table_name, raw_values)

                for line in f:
                    output_text = line

                    # RNT Datos
                    naf_aux = None
                    ipf_aux = None
                    caf_aux = None
                    ftrades = None
                    ftrahas = None
                    dias_coti = None
                    horas_coti = None
                    horas_comp = None
                    tipo_complemento = None
                    base = None
                    ###


                    patron = r'^[\s\.]*$'
                    if len(output_text) > 0 and re.search(patron, output_text):
                        output_text = re.sub(patron, r'', output_text)
                    if len(output_text) > 0:
                        #print(f"INICIO output_text: {output_text}, naf: {naf}, ipf: {ipf}, caf: {caf}, ftrades: {ftrades}, ftrahas: {ftrahas}, dias_coti: {dias_coti}, horas_coti: {horas_coti}, horas_comp: {horas_comp}, tipo_complemento: {tipo_complemento}, base: {base}")
                        patron = r'^.*RELACI.N\s*NOMINAL\s*DE\s*TRABAJADORES.*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*N.mero\s*de\s*autorizaci.n\s*(\w[\w\W]+[\S]){0,1}\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            num_autorizacion = re.sub(patron, r'\g<1>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*Datos\s*identificativos\s*de\s*la\s*liquidaci.n.*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*Raz.n\s*social\s*(\w[\w\W]+[\S]){0,1}\s*C.digo\s*de\s*empresario\s*(\w[\w\W]+[\S]){0,1}\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            razon_social = re.sub(patron, r'\g<1>', output_text)
                            codigo_empresario = re.sub(patron, r'\g<2>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*C.digo\s*cuenta\s*cotizaci.n\s*(\w[\w\W]+[\S]){0,1}\s*N.mero\s*de\s*la\s*liquidaci.n\s*(\w[\w\W]+[\S]){0,1}\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            codigo_cuenta_cotizacon = re.sub(patron, r'\g<1>', output_text)
                            num_liquidacion = re.sub(patron, r'\g<2>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*Periodo\s*de\s*liquidaci.n\s*(\w[\w\W]+[\S]){0,1}\s*N.mero\s*de\s*trabajadores\s*(\d+)\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            periodo = re.sub(patron, r'\g<1>', output_text)
                            arr_periodo = periodo.split('-')
                            periodo_desde = arr_periodo[0]
                            periodo_hasta = arr_periodo[1]
                            num_trabajadores = re.sub(patron, r'\g<2>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*Calificador\s*de\s*la\s*liquidaci.n\s*(\w[\w\W]+[\S]){0,1}\s*Liquidaci.n\s*(\w[\w\W]+[\S]){0,1}\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            calificador = re.sub(patron, r'\g<1>', output_text)
                            liquidacion = re.sub(patron, r'\g<2>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*Fecha\s*de\s*control\s*(\w[\w\W]+[\S]){0,1}\s*Entidad\s*de\s*AT.EP\s*(\w[\w\W]+[\S]){0,1}\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            fecha_control = re.sub(patron, r'\g<1>', output_text)
                            entidad_atep = re.sub(patron, r'\g<2>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*Fechas\s*Fechas\s*Días\s*Horas\s*Horas\s*Bases y compensaciones\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*NAF\s*I\.P\.F\.\s*C\.A\.F\.\s*Tramo\s*Tramo\s*Coti\.\s*Coti\.\s*Compl\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*Desde\s*Hasta\s*Descripci.n\s*Importe\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*SUMA\s*DE\s*BASES\s*SUMA\s*DE\s*COMPENSACIONES\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^BASE\s*DE\s*CONTINGENCIAS\s*COMUNES\s*(\w[\w\W]+[\S]){0,1}\s*COMPENSACION\s*I\.T\.\s*ENFERMEDAD\s*COM.N\s*(\w[\w\W]+[\S]){0,1}\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            suma_base_cont_com = re.sub(patron, r'\g<1>', output_text)
                            suma_comp_enf_com = re.sub(patron, r'\g<2>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^BASE\s*DE\s*CONTINGENCIAS\s*COMUNES\s*(\w[\w\W]+[\S]){0,1}\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            suma_base_cont_com = re.sub(patron, r'\g<1>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^BASE\s*DE\s*ACCIDENTES\s*DE\s*TRABAJO\s*(\w[\w\W]+[\S]){0,1}\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            suma_acci_trab = re.sub(patron, r'\g<1>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*CODIFICACIONES\s*INFORM.TICAS\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            output_text = re.sub(patron, r'', output_text)
                        if datos_referencia:
                            patron = r'^\s*(\w+){0,1}\s*(\d{1,2}.\d{1,2}.\d{4}){0,1}\s*(\d{2}\:\d{2}\:\d{2}){0,1}\s*(\w+){0,1}\s*P.gina\s*(\d+)\s*de\s*(\d+)\s*$'
                            if len(output_text) > 0 and re.search(patron, output_text):
                                referencia = re.sub(patron, r'\g<1>', output_text)
                                fecha = re.sub(patron, r'\g<2>', output_text)
                                hora = re.sub(patron, r'\g<3>', output_text)
                                huella = re.sub(patron, r'\g<4>', output_text)
                                pagina_aux = re.sub(patron, r'\g<5>', output_text)
                                pagina += 1
                                total_pagina = re.sub(patron, r'\g<6>', output_text)
                                output_text = re.sub(patron, r'', output_text)
                            datos_referencia = False
                        patron = r'^.*Referencia\s*Fecha\s*Hora\s*Huella\s*Página\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            datos_referencia = True
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*Este\s*documento.*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^(.{12})(.{18})(.{16})(.{14})(.{14})(.{9})(.{9})(.{10})(.{60})(.{12})\s*\d*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            linea_rnt = True
                            naf_aux = re.sub(r'\s*', '', re.sub(patron, r'\g<1>', output_text))
                            ipf_aux = re.sub(r'\s*', '', re.sub(patron, r'\g<2>', output_text))
                            caf_aux = re.sub(r'\s*$', '', re.sub(r'^\s*', '', re.sub(patron, r'\g<3>', output_text)))
                            ftrades = re.sub(r'\s*', '', re.sub(patron, r'\g<4>', output_text))
                            ftrahas = re.sub(r'\s*', '', re.sub(patron, r'\g<5>', output_text))
                            dias_coti = re.sub(r'\s*', '', re.sub(patron, r'\g<6>', output_text))
                            horas_coti = re.sub(r'\s*', '', re.sub(patron, r'\g<7>', output_text))
                            horas_comp = re.sub(r'\s*', '', re.sub(patron, r'\g<8>', output_text))
                            tipo_complemento = re.sub(r'\s*$', '',
                                                        re.sub(r'^\s*', '', re.sub(patron, r'\g<9>', output_text)))
                            base = re.sub(r'\s*', '', re.sub(patron, r'\g<10>', output_text))
                            output_text = re.sub(patron,
                                                r'\g<1>;\g<2>;\g<3>;\g<4>;\g<5>;\g<6>;\g<7>;\g<8>;\g<9>;\g<10>',
                                                output_text)

                        #print(f"FIN output_text: {output_text}, naf: {naf}, ipf: {ipf}, caf: {caf}, ftrades: {ftrades}, ftrahas: {ftrahas}, dias_coti: {dias_coti}, horas_coti: {horas_coti}, horas_comp: {horas_comp}, tipo_complemento: {tipo_complemento}, base: {base}")
                        if linea_rnt:
                            linea_rnt = False
                            if len(naf_aux) > 0:
                                naf = naf_aux

                            if len(ipf_aux) > 0:
                                ipf = ipf_aux

                            if len(caf_aux) > 0:
                                caf = caf_aux

                            csv_line.append([str(naf), str(ipf), str(caf), str(ftrades), str(ftrahas), str(dias_coti),
                                            str(horas_coti), str(horas_comp), str(tipo_complemento), str(base), rnts_resumen_id,
                                            nombre_fichero])
    
            sql = ''
            try:
                with connect() as conn:
                    with conn.cursor() as cur:
                        cen_table_name = "centros"
                        emp_table_name = "empresas"
                        #print(f"centro 001: razón social: {razon_social}, centro: {num_liquidacion[:2]}")
                        sql = f"select e.id empresa_id, e.codigo empresa_codigo, c.id centro_id, c.codigo centro_codigo from {emp_table_name} e, {cen_table_name} c, provincias p where c.empresa_id = e.id and c.provincia_id = p.id AND p.codigo = '{num_liquidacion[:2]}' and sinacentos(e.nombre) = sinacentos('{razon_social}')"
                        #print(f"sql: {sql}")
                        cur.execute(sql)
                        #print(f"centro 002")
                        row_centro = cur.fetchone()
                        #print(f"row_centro: {row_centro} and len(row_centro): {len(row_centro)}")
                        if row_centro and len(row_centro) > 0:
                            #print(f"centro 003: centro: {row_centro[0]} - {row_centro[2]}")
                            empresa_id = row_centro[0]
                            empresa_codigo = row_centro[1]
                            centro_id = row_centro[2]
                            centro_codigo = row_centro[3]
            except (Exception, psycopg2.DatabaseError) as error:
                print(f'pdfToCsvRNT fichero: {fichero} # error: {error}')
            finally:
                if conn is not None:
                    conn.close()

            raw_values = {
                "num_autorizacion": num_autorizacion,
                "razon_social": razon_social,
                "codigo_empresario": codigo_empresario,
                "codigo_cuenta_cotizacon": codigo_cuenta_cotizacon,
                "num_liquidacion": num_liquidacion,
                "periodo": periodo,
                "periodo_desde": periodo_desde,
                "periodo_hasta": periodo_hasta,
                "num_trabajadores": num_trabajadores,
                "calificador": calificador,
                "liquidacion": liquidacion,
                "fecha_control": fecha_control,
                "entidad_atep": entidad_atep,
                "referencia": referencia,
                "fecha": fecha,
                "hora": hora,
                "huella": huella,
                "pagina": pagina,
                "total_pagina": total_pagina,
                "suma_base_cont_com": suma_base_cont_com,
                "suma_comp_enf_com": suma_comp_enf_com,
                "suma_acci_trab": suma_acci_trab,
                "empresa_id": empresa_id,
                "centro_id": centro_id,
            }

            actualizar(table_name, rnts_resumen_id, raw_values)
            
            tipo = re.sub(r'\w+(RNT|RNL)\w+', r'\g<1>', nombre_fichero)
            nombre_salida = f"{periodo_desde.replace("/", "")}_{empresa_codigo}_{centro_codigo}_{tipo}.csv"
            archivo_salida = directorio / nombre_salida  # Ruta de salida

            nombre_pdf = f"{nombre_fichero}.pdf"
            archivo_pdf = directorio / nombre_pdf  # Ruta de salida
            nombre_salida_pdf = f"{periodo_desde.replace("/", "")}_{empresa_codigo}_{centro_codigo}_{tipo}.pdf"
            archivo_salida_pdf = directorio / nombre_salida_pdf  # Ruta de salida
            os.rename(archivo_pdf, archivo_salida_pdf)

            if len(csv_line) > 0:
                with open(archivo_salida, 'w') as out_file:
                    writer = csv.writer(out_file, delimiter=';')
                    writer.writerow(('naf', 'ipf', 'caf', 'ftrades', 'ftrahas', 'dias_coti', 'horas_coti', 'horas_comp',
                                    'tipo_complemento', 'base', 'rnts_resumen_id', 'nombre_fichero'))
                    writer.writerows(csv_line)
            

            os.remove(archivo_entrada)
            return nombre_salida
        except (Exception, pdftotext.Error) as error:
            print(f'pdfToCsvRNT FICHERO: {fichero}, ERROR: {error}')
    else:
        print(f"NO EXISTE {fichero}")


def csvToSQLRNT(fichero, dir):
    # Construir rutas con Path
    directorio = Path(dir)
    archivo_entrada = directorio / fichero  # Ruta de entrada

    if os.path.exists(archivo_entrada):
        rntd_table_name = 'rnts_tmp'
        with open(archivo_entrada, newline='') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            i = 1
            rnts_resumen_id = None
            resumen = None
            anno_id = None
            mes_id = None
            empresa_id = None
            centro_id = None
            for row in reader:
                trabajador_id = None
                naf = row['naf']
                ndni = row['ipf'][:1]
                dni = row['ipf'][2:]
                caf = row['caf']
                ftrades = formatoFechas(row['ftrades'])
                ftrahas = formatoFechas(row['ftrahas'])
                dias_coti = row['dias_coti']
                horas_coti = row['horas_coti']
                horas_comp = row['horas_comp']
                complementos_tipo_id = None
                complementos_tipo = row['tipo_complemento']
                base = row['base']
                n_dias_coti = re.sub(r'[\s\D]*(\d*)[\s\D]*', r'\g<1>', dias_coti)
                n_horas_coti = re.sub(r'[\s\D]*(\d*)[\s\D]*', r'\g<1>', horas_coti)
                n_horas_comp = re.sub(r'[\s\D]*(\d*)[\s\D]*', r'\g<1>', horas_comp)
                reducciones_tipo_id = None
                num_errors = 0
                if rnts_resumen_id is None or row['rnts_resumen_id'] != rnts_resumen_id:
                    rnts_resumen_id = row['rnts_resumen_id']
                    try:
                        with connect() as conn:
                            with conn.cursor() as cur:
                                sql = f"select num_autorizacion, razon_social, codigo_empresario, codigo_cuenta_cotizacon, num_liquidacion, periodo, periodo_desde, periodo_hasta, num_trabajadores, calificador, liquidacion, fecha_control, entidad_atep, referencia, fecha, hora, huella, pagina, total_pagina, suma_base_cont_com, suma_comp_enf_com, suma_acci_trab, empresa_id, centro_id from rnts_resumenes where id = {rnts_resumen_id}"
                                #print(f"sql: {sql}")
                                cur.execute(sql)
                                row_resumen = cur.fetchone()
                                if row_resumen and len(row_resumen) > 0:
                                    resumen = {
                                        "num_autorizacion": row_resumen[0],
                                        "razon_social": row_resumen[1],
                                        "codigo_empresario": row_resumen[2],
                                        "codigo_cuenta_cotizacon": row_resumen[3],
                                        "num_liquidacion": row_resumen[4],
                                        "periodo": row_resumen[5],
                                        "periodo_desde": row_resumen[6],
                                        "periodo_hasta": row_resumen[7],
                                        "num_trabajadores": row_resumen[8],
                                        "calificador": row_resumen[9],
                                        "liquidacion": row_resumen[10],
                                        "fecha_control": row_resumen[11],
                                        "entidad_atep": row_resumen[12],
                                        "referencia": row_resumen[13],
                                        "fecha": row_resumen[14],
                                        "hora": row_resumen[15],
                                        "huella": row_resumen[16],
                                        "pagina": row_resumen[17],
                                        "total_pagina": row_resumen[18],
                                        "suma_base_cont_com": row_resumen[19],
                                        "suma_comp_enf_com": row_resumen[20],
                                        "suma_acci_trab": row_resumen[21],
                                        "empresa_id": row_resumen[22],
                                        "centro_id": row_resumen[23],
                                    }
                    except (Exception, psycopg2.DatabaseError) as error:
                        print(f'csvToSQLRNT fichero: {fichero} # error: {error}')
                    finally:
                        if conn is not None:
                            conn.close()
                    if resumen is not None:
                        #print(f"csvToSQLRNT: resumen: {resumen}")
                        mes_id = int(resumen.get('periodo_desde').split('/')[0])
                        anno_id = int(resumen.get('periodo_desde').split('/')[1])
                        empresa_id = resumen.get('empresa_id')
                        centro_id = resumen.get('centro_id')


                try:
                    with connect() as conn:
                        with conn.cursor() as cur:
                            sql = f"select id from trabajadores where nif = '{dni}'"
                            #print(f"sql: {sql}")
                            cur.execute(sql)
                            row_trabajador = cur.fetchone()
                            if row_trabajador and len(row_trabajador) > 0:
                                trabajador_id = row_trabajador[0]
                except (Exception, psycopg2.DatabaseError) as error:
                    print(f'csvToSQLRNT trabajadores fichero: {fichero} # error: {error}')
                finally:
                    if conn is not None:
                        conn.close()


                try:
                    with connect() as conn:
                        with conn.cursor() as cur:
                            ct_table_name = 'complementos_tipos'
                            sql = f"select id from {ct_table_name} where sinacentos(descripcion) = sinacentos('{complementos_tipo}')"
                            #print(f"sql: {sql}")
                            cur.execute(sql)
                            rows_complementos_tipo = cur.fetchone()
                            if rows_complementos_tipo and len(rows_complementos_tipo) > 0:
                                complementos_tipo_id = rows_complementos_tipo[0]
                            else:
                                ct_raw_values = {
                                    "codigo": '',
                                    "descripcion": complementos_tipo,
                                }

                                complementos_tipo_id = insertar(ct_table_name, ct_raw_values)
                                print(f"Insertado Tipo de Complemento con ID: {complementos_tipo_id}")

                except (Exception, psycopg2.DatabaseError) as error:
                    print(f'csvToSQLRNT trabajadores fichero: {fichero} # error: {error}')
                finally:
                    if conn is not None:
                        conn.close()

                rntd_raw_values = {
                        "trabajador_id" : trabajador_id,
                        "naf" : naf,
                        "ndni" : ndni,
                        "dni" : dni,
                        "caf" : caf,
                        "ftrades" : ftrades,
                        "ftrahas" : ftrahas,
                        "mes_id" : mes_id,
                        "anno_id" : anno_id,
                        "dias_coti" : dias_coti,
                        "horas_coti" : horas_coti,
                        "horas_comp" : horas_comp,
                        "complementos_tipo_id" : complementos_tipo_id,
                        "base" : base,
                        "centro_id" : centro_id,
                        "n_dias_coti" : n_dias_coti,
                        "n_horas_coti" : n_horas_coti,
                        "n_horas_comp" : n_horas_comp,
                        "reducciones_tipo_id" : reducciones_tipo_id,
                        "empresa_id" : empresa_id,
                        "rnts_resumen_id" : rnts_resumen_id,
                    }

                rntd_id = insertar(rntd_table_name, rntd_raw_values)

                i += 1
            print(f"TODO OK; errors: {num_errors}")

def pdfToCsvRLC(fichero, dir):
    nombre_fichero = Path(fichero).stem  # Elimina la extensión sin asumir '.pdf'

    # Construir rutas con Path
    directorio = Path(dir)
    archivo_entrada = directorio / fichero  # Ruta de entrada

    pagina_aux = None
    # RNT Resumenes
    rnts_resumen_id = None
    num_autorizacion = None
    razon_social = None
    codigo_empresario = None
    codigo_cuenta_cotizacon = None
    num_liquidacion = None
    periodo = None
    periodo_desde = None
    periodo_hasta = None
    num_trabajadores = None
    calificador = None
    liquidacion = None
    fecha_control = None
    entidad_atep = None
    referencia = None
    fecha = None
    hora = None
    huella = None
    pagina = 1
    total_pagina = None
    suma_base_cont_com = None
    suma_comp_enf_com = None
    suma_acci_trab = None
    empresa_id = None
    empresa_codigo = None
    centro_id = None
    centro_codigo = None
    ###
    sql = ''

    # creating a pdf reader object 
    #print(f"pdfToCsvRNT({fichero}, {dir})")
    if os.path.exists(archivo_entrada):
        #print(f"pdfToCsvRNT fichero: {fichero}")
        try:
            with open(archivo_entrada) as f:
                csv_line = []
                datos_referencia = False
                linea_rnt = False
                table_name = 'rnts_resumenes'

                # RNT Datos
                naf = None
                ipf = None
                caf = None
                ####

                raw_values = {
                                "num_autorizacion": num_autorizacion,
                                "razon_social": razon_social,
                                "codigo_empresario": codigo_empresario,
                                "codigo_cuenta_cotizacon": codigo_cuenta_cotizacon,
                                "num_liquidacion": num_liquidacion,
                                "periodo": periodo,
                                "periodo_desde": periodo_desde,
                                "periodo_hasta": periodo_hasta,
                                "num_trabajadores": num_trabajadores,
                                "calificador": calificador,
                                "liquidacion": liquidacion,
                                "fecha_control": fecha_control,
                                "entidad_atep": entidad_atep,
                                "referencia": referencia,
                                "fecha": fecha,
                                "hora": hora,
                                "huella": huella,
                                "pagina": pagina,
                                "total_pagina": total_pagina,
                                "suma_base_cont_com": suma_base_cont_com,
                                "suma_comp_enf_com": suma_comp_enf_com,
                                "suma_acci_trab": suma_acci_trab,
                                "empresa_id": empresa_id,
                                "centro_id": centro_id,
                            }
                
                rnts_resumen_id = insertar(table_name, raw_values)

                for line in f:
                    output_text = line

                    # RNT Datos
                    naf_aux = None
                    ipf_aux = None
                    caf_aux = None
                    ftrades = None
                    ftrahas = None
                    dias_coti = None
                    horas_coti = None
                    horas_comp = None
                    tipo_complemento = None
                    base = None
                    ###


                    patron = r'^[\s\.]*$'
                    if len(output_text) > 0 and re.search(patron, output_text):
                        output_text = re.sub(patron, r'', output_text)
                    if len(output_text) > 0:
                        #print(f"INICIO output_text: {output_text}, naf: {naf}, ipf: {ipf}, caf: {caf}, ftrades: {ftrades}, ftrahas: {ftrahas}, dias_coti: {dias_coti}, horas_coti: {horas_coti}, horas_comp: {horas_comp}, tipo_complemento: {tipo_complemento}, base: {base}")
                        patron = r'^.*RELACI.N\s*NOMINAL\s*DE\s*TRABAJADORES.*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*N.mero\s*de\s*autorizaci.n\s*(\w[\w\W]+[\S]){0,1}\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            num_autorizacion = re.sub(patron, r'\g<1>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*Datos\s*identificativos\s*de\s*la\s*liquidaci.n.*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*Raz.n\s*social\s*(\w[\w\W]+[\S]){0,1}\s*C.digo\s*de\s*empresario\s*(\w[\w\W]+[\S]){0,1}\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            razon_social = re.sub(patron, r'\g<1>', output_text)
                            codigo_empresario = re.sub(patron, r'\g<2>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*C.digo\s*cuenta\s*cotizaci.n\s*(\w[\w\W]+[\S]){0,1}\s*N.mero\s*de\s*la\s*liquidaci.n\s*(\w[\w\W]+[\S]){0,1}\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            codigo_cuenta_cotizacon = re.sub(patron, r'\g<1>', output_text)
                            num_liquidacion = re.sub(patron, r'\g<2>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*Periodo\s*de\s*liquidaci.n\s*(\w[\w\W]+[\S]){0,1}\s*N.mero\s*de\s*trabajadores\s*(\d+)\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            periodo = re.sub(patron, r'\g<1>', output_text)
                            arr_periodo = periodo.split('-')
                            periodo_desde = arr_periodo[0]
                            periodo_hasta = arr_periodo[1]
                            num_trabajadores = re.sub(patron, r'\g<2>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*Calificador\s*de\s*la\s*liquidaci.n\s*(\w[\w\W]+[\S]){0,1}\s*Liquidaci.n\s*(\w[\w\W]+[\S]){0,1}\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            calificador = re.sub(patron, r'\g<1>', output_text)
                            liquidacion = re.sub(patron, r'\g<2>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*Fecha\s*de\s*control\s*(\w[\w\W]+[\S]){0,1}\s*Entidad\s*de\s*AT.EP\s*(\w[\w\W]+[\S]){0,1}\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            fecha_control = re.sub(patron, r'\g<1>', output_text)
                            entidad_atep = re.sub(patron, r'\g<2>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*Fechas\s*Fechas\s*Días\s*Horas\s*Horas\s*Bases y compensaciones\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*NAF\s*I\.P\.F\.\s*C\.A\.F\.\s*Tramo\s*Tramo\s*Coti\.\s*Coti\.\s*Compl\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*Desde\s*Hasta\s*Descripci.n\s*Importe\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*SUMA\s*DE\s*BASES\s*SUMA\s*DE\s*COMPENSACIONES\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^BASE\s*DE\s*CONTINGENCIAS\s*COMUNES\s*(\w[\w\W]+[\S]){0,1}\s*COMPENSACION\s*I\.T\.\s*ENFERMEDAD\s*COM.N\s*(\w[\w\W]+[\S]){0,1}\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            suma_base_cont_com = re.sub(patron, r'\g<1>', output_text)
                            suma_comp_enf_com = re.sub(patron, r'\g<2>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^BASE\s*DE\s*CONTINGENCIAS\s*COMUNES\s*(\w[\w\W]+[\S]){0,1}\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            suma_base_cont_com = re.sub(patron, r'\g<1>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^BASE\s*DE\s*ACCIDENTES\s*DE\s*TRABAJO\s*(\w[\w\W]+[\S]){0,1}\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            suma_acci_trab = re.sub(patron, r'\g<1>', output_text)
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*CODIFICACIONES\s*INFORM.TICAS\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            output_text = re.sub(patron, r'', output_text)
                        if datos_referencia:
                            patron = r'^\s*(\w+){0,1}\s*(\d{1,2}.\d{1,2}.\d{4}){0,1}\s*(\d{2}\:\d{2}\:\d{2}){0,1}\s*(\w+){0,1}\s*P.gina\s*(\d+)\s*de\s*(\d+)\s*$'
                            if len(output_text) > 0 and re.search(patron, output_text):
                                referencia = re.sub(patron, r'\g<1>', output_text)
                                fecha = re.sub(patron, r'\g<2>', output_text)
                                hora = re.sub(patron, r'\g<3>', output_text)
                                huella = re.sub(patron, r'\g<4>', output_text)
                                pagina_aux = re.sub(patron, r'\g<5>', output_text)
                                pagina += 1
                                total_pagina = re.sub(patron, r'\g<6>', output_text)
                                output_text = re.sub(patron, r'', output_text)
                            datos_referencia = False
                        patron = r'^.*Referencia\s*Fecha\s*Hora\s*Huella\s*Página\s*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            datos_referencia = True
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^.*Este\s*documento.*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            output_text = re.sub(patron, r'', output_text)
                        patron = r'^(.{12})(.{18})(.{16})(.{14})(.{14})(.{9})(.{9})(.{10})(.{60})(.{12})\s*\d*$'
                        if len(output_text) > 0 and re.search(patron, output_text):
                            linea_rnt = True
                            naf_aux = re.sub(r'\s*', '', re.sub(patron, r'\g<1>', output_text))
                            ipf_aux = re.sub(r'\s*', '', re.sub(patron, r'\g<2>', output_text))
                            caf_aux = re.sub(r'\s*$', '', re.sub(r'^\s*', '', re.sub(patron, r'\g<3>', output_text)))
                            ftrades = re.sub(r'\s*', '', re.sub(patron, r'\g<4>', output_text))
                            ftrahas = re.sub(r'\s*', '', re.sub(patron, r'\g<5>', output_text))
                            dias_coti = re.sub(r'\s*', '', re.sub(patron, r'\g<6>', output_text))
                            horas_coti = re.sub(r'\s*', '', re.sub(patron, r'\g<7>', output_text))
                            horas_comp = re.sub(r'\s*', '', re.sub(patron, r'\g<8>', output_text))
                            tipo_complemento = re.sub(r'\s*$', '',
                                                        re.sub(r'^\s*', '', re.sub(patron, r'\g<9>', output_text)))
                            base = re.sub(r'\s*', '', re.sub(patron, r'\g<10>', output_text))
                            output_text = re.sub(patron,
                                                r'\g<1>;\g<2>;\g<3>;\g<4>;\g<5>;\g<6>;\g<7>;\g<8>;\g<9>;\g<10>',
                                                output_text)

                        #print(f"FIN output_text: {output_text}, naf: {naf}, ipf: {ipf}, caf: {caf}, ftrades: {ftrades}, ftrahas: {ftrahas}, dias_coti: {dias_coti}, horas_coti: {horas_coti}, horas_comp: {horas_comp}, tipo_complemento: {tipo_complemento}, base: {base}")
                        if linea_rnt:
                            linea_rnt = False
                            if len(naf_aux) > 0:
                                naf = naf_aux

                            if len(ipf_aux) > 0:
                                ipf = ipf_aux

                            if len(caf_aux) > 0:
                                caf = caf_aux

                            csv_line.append([str(naf), str(ipf), str(caf), str(ftrades), str(ftrahas), str(dias_coti),
                                            str(horas_coti), str(horas_comp), str(tipo_complemento), str(base), rnts_resumen_id,
                                            nombre_fichero])
    
            sql = ''
            try:
                with connect() as conn:
                    with conn.cursor() as cur:
                        cen_table_name = "centros"
                        emp_table_name = "empresas"
                        #print(f"centro 001: razón social: {razon_social}, centro: {num_liquidacion[:2]}")
                        sql = f"select e.id empresa_id, e.codigo empresa_codigo, c.id centro_id, c.codigo centro_codigo from {emp_table_name} e, {cen_table_name} c, provincias p where c.empresa_id = e.id and c.provincia_id = p.id AND p.codigo = '{num_liquidacion[:2]}' and sinacentos(e.nombre) = sinacentos('{razon_social}')"
                        #print(f"sql: {sql}")
                        cur.execute(sql)
                        #print(f"centro 002")
                        row_centro = cur.fetchone()
                        #print(f"row_centro: {row_centro} and len(row_centro): {len(row_centro)}")
                        if row_centro and len(row_centro) > 0:
                            #print(f"centro 003: centro: {row_centro[0]} - {row_centro[2]}")
                            empresa_id = row_centro[0]
                            empresa_codigo = row_centro[1]
                            centro_id = row_centro[2]
                            centro_codigo = row_centro[3]
            except (Exception, psycopg2.DatabaseError) as error:
                print(f'pdfToCsvRNT fichero: {fichero} # error: {error}')
            finally:
                if conn is not None:
                    conn.close()

            raw_values = {
                "num_autorizacion": num_autorizacion,
                "razon_social": razon_social,
                "codigo_empresario": codigo_empresario,
                "codigo_cuenta_cotizacon": codigo_cuenta_cotizacon,
                "num_liquidacion": num_liquidacion,
                "periodo": periodo,
                "periodo_desde": periodo_desde,
                "periodo_hasta": periodo_hasta,
                "num_trabajadores": num_trabajadores,
                "calificador": calificador,
                "liquidacion": liquidacion,
                "fecha_control": fecha_control,
                "entidad_atep": entidad_atep,
                "referencia": referencia,
                "fecha": fecha,
                "hora": hora,
                "huella": huella,
                "pagina": pagina,
                "total_pagina": total_pagina,
                "suma_base_cont_com": suma_base_cont_com,
                "suma_comp_enf_com": suma_comp_enf_com,
                "suma_acci_trab": suma_acci_trab,
                "empresa_id": empresa_id,
                "centro_id": centro_id,
            }

            actualizar(table_name, rnts_resumen_id, raw_values)
            
            tipo = re.sub(r'\w+(RNT|RNL)\w+', r'\g<1>', nombre_fichero)
            nombre_salida = f"{periodo_desde.replace("/", "")}_{empresa_codigo}_{centro_codigo}_{tipo}.csv"
            archivo_salida = directorio / nombre_salida  # Ruta de salida

            nombre_pdf = f"{nombre_fichero}.pdf"
            archivo_pdf = directorio / nombre_pdf  # Ruta de salida
            nombre_salida_pdf = f"{periodo_desde.replace("/", "")}_{empresa_codigo}_{centro_codigo}_{tipo}.pdf"
            archivo_salida_pdf = directorio / nombre_salida_pdf  # Ruta de salida
            os.rename(archivo_pdf, archivo_salida_pdf)

            if len(csv_line) > 0:
                with open(archivo_salida, 'w') as out_file:
                    writer = csv.writer(out_file, delimiter=';')
                    writer.writerow(('naf', 'ipf', 'caf', 'ftrades', 'ftrahas', 'dias_coti', 'horas_coti', 'horas_comp',
                                    'tipo_complemento', 'base', 'rnts_resumen_id', 'nombre_fichero'))
                    writer.writerows(csv_line)
            

            os.remove(archivo_entrada)
            return nombre_salida
        except (Exception, pdftotext.Error) as error:
            print(f'pdfToCsvRNT FICHERO: {fichero}, ERROR: {error}')
    else:
        print(f"NO EXISTE {fichero}")


def csvToSQLRLC(fichero, dir):
    nombre_fichero = Path(fichero).stem  # Elimina la extensión sin asumir '.pdf'

    # Construir rutas con Path
    directorio = Path(dir)
    archivo_entrada = directorio / fichero  # Ruta de entrada

    if os.path.exists(archivo_entrada):
        rntd_table_name = 'rnts_tmp'
        with open(archivo_entrada, newline='') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            i = 1
            rnts_resumen_id = None
            resumen = None
            anno_id = None
            mes_id = None
            empresa_id = None
            centro_id = None
            for row in reader:
                trabajador_id = None
                naf = row['naf']
                ndni = row['ipf'][:1]
                dni = row['ipf'][2:]
                caf = row['caf']
                ftrades = formatoFechas(row['ftrades'])
                ftrahas = formatoFechas(row['ftrahas'])
                dias_coti = row['dias_coti']
                horas_coti = row['horas_coti']
                horas_comp = row['horas_comp']
                complementos_tipo_id = None
                complementos_tipo = row['tipo_complemento']
                base = row['base']
                n_dias_coti = re.sub(r'[\s\D]*(\d*)[\s\D]*', r'\g<1>', dias_coti)
                n_horas_coti = re.sub(r'[\s\D]*(\d*)[\s\D]*', r'\g<1>', horas_coti)
                n_horas_comp = re.sub(r'[\s\D]*(\d*)[\s\D]*', r'\g<1>', horas_comp)
                reducciones_tipo_id = None
                num_errors = 0
                if rnts_resumen_id is None or row['rnts_resumen_id'] != rnts_resumen_id:
                    rnts_resumen_id = row['rnts_resumen_id']
                    try:
                        with connect() as conn:
                            with conn.cursor() as cur:
                                sql = f"select num_autorizacion, razon_social, codigo_empresario, codigo_cuenta_cotizacon, num_liquidacion, periodo, periodo_desde, periodo_hasta, num_trabajadores, calificador, liquidacion, fecha_control, entidad_atep, referencia, fecha, hora, huella, pagina, total_pagina, suma_base_cont_com, suma_comp_enf_com, suma_acci_trab, empresa_id, centro_id from rnts_resumenes where id = {rnts_resumen_id}"
                                #print(f"sql: {sql}")
                                cur.execute(sql)
                                row_resumen = cur.fetchone()
                                if row_resumen and len(row_resumen) > 0:
                                    resumen = {
                                        "num_autorizacion": row_resumen[0],
                                        "razon_social": row_resumen[1],
                                        "codigo_empresario": row_resumen[2],
                                        "codigo_cuenta_cotizacon": row_resumen[3],
                                        "num_liquidacion": row_resumen[4],
                                        "periodo": row_resumen[5],
                                        "periodo_desde": row_resumen[6],
                                        "periodo_hasta": row_resumen[7],
                                        "num_trabajadores": row_resumen[8],
                                        "calificador": row_resumen[9],
                                        "liquidacion": row_resumen[10],
                                        "fecha_control": row_resumen[11],
                                        "entidad_atep": row_resumen[12],
                                        "referencia": row_resumen[13],
                                        "fecha": row_resumen[14],
                                        "hora": row_resumen[15],
                                        "huella": row_resumen[16],
                                        "pagina": row_resumen[17],
                                        "total_pagina": row_resumen[18],
                                        "suma_base_cont_com": row_resumen[19],
                                        "suma_comp_enf_com": row_resumen[20],
                                        "suma_acci_trab": row_resumen[21],
                                        "empresa_id": row_resumen[22],
                                        "centro_id": row_resumen[23],
                                    }
                    except (Exception, psycopg2.DatabaseError) as error:
                        print(f'csvToSQLRNT fichero: {fichero} # error: {error}')
                    finally:
                        if conn is not None:
                            conn.close()
                    if resumen is not None:
                        #print(f"csvToSQLRNT: resumen: {resumen}")
                        mes_id = int(resumen.get('periodo_desde').split('/')[0])
                        anno_id = int(resumen.get('periodo_desde').split('/')[1])
                        empresa_id = resumen.get('empresa_id')
                        centro_id = resumen.get('centro_id')


                try:
                    with connect() as conn:
                        with conn.cursor() as cur:
                            sql = f"select id from trabajadores where nif = '{dni}'"
                            #print(f"sql: {sql}")
                            cur.execute(sql)
                            row_trabajador = cur.fetchone()
                            if row_trabajador and len(row_trabajador) > 0:
                                trabajador_id = row_trabajador[0]
                except (Exception, psycopg2.DatabaseError) as error:
                    print(f'csvToSQLRNT trabajadores fichero: {fichero} # error: {error}')
                finally:
                    if conn is not None:
                        conn.close()


                try:
                    with connect() as conn:
                        with conn.cursor() as cur:
                            ct_table_name = 'complementos_tipos'
                            sql = f"select id from {ct_table_name} where sinacentos(descripcion) = sinacentos('{complementos_tipo}')"
                            #print(f"sql: {sql}")
                            cur.execute(sql)
                            rows_complementos_tipo = cur.fetchone()
                            if rows_complementos_tipo and len(rows_complementos_tipo) > 0:
                                complementos_tipo_id = rows_complementos_tipo[0]
                            else:
                                ct_raw_values = {
                                    "codigo": '',
                                    "descripcion": complementos_tipo,
                                }

                                complementos_tipo_id = insertar(ct_table_name, ct_raw_values)
                                print(f"Insertado Tipo de Complemento con ID: {complementos_tipo_id}")

                except (Exception, psycopg2.DatabaseError) as error:
                    print(f'csvToSQLRNT trabajadores fichero: {fichero} # error: {error}')
                finally:
                    if conn is not None:
                        conn.close()

                rntd_raw_values = {
                        "trabajador_id" : trabajador_id,
                        "naf" : naf,
                        "ndni" : ndni,
                        "dni" : dni,
                        "caf" : caf,
                        "ftrades" : ftrades,
                        "ftrahas" : ftrahas,
                        "mes_id" : mes_id,
                        "anno_id" : anno_id,
                        "dias_coti" : dias_coti,
                        "horas_coti" : horas_coti,
                        "horas_comp" : horas_comp,
                        "complementos_tipo_id" : complementos_tipo_id,
                        "base" : base,
                        "centro_id" : centro_id,
                        "n_dias_coti" : n_dias_coti,
                        "n_horas_coti" : n_horas_coti,
                        "n_horas_comp" : n_horas_comp,
                        "reducciones_tipo_id" : reducciones_tipo_id,
                        "empresa_id" : empresa_id,
                        "rnts_resumen_id" : rnts_resumen_id,
                    }

                rntd_id = insertar(rntd_table_name, rntd_raw_values)

                i += 1
            print(f"TODO OK; errors: {num_errors}")

def organizar(fichero, dir):
    nombre_fichero = Path(fichero).stem  # Elimina la extensión sin asumir '.pdf'

    # Construir rutas con Path
    directorio = Path(dir)
    nombre_pdf = f"{nombre_fichero}.pdf"
    archivo_pdf = directorio / nombre_pdf  # Ruta de entrada
    nombre_csv = f"{nombre_fichero}.csv"
    archivo_csv = directorio / nombre_csv  # Ruta de entrada

    if os.path.exists(archivo_pdf):
        arr_fichero = nombre_fichero.split('_')
        periodo = arr_fichero[0]
        empresa_codigo = arr_fichero[1]
        centro_codigo = arr_fichero[2]
        path_empresa = os.path.join(saveDir, empresa_codigo) 
        if not os.path.exists(path_empresa):
            os.mkdir(path_empresa)
        path_centro = os.path.join(path_empresa, centro_codigo) 
        if not os.path.exists(path_centro):
            os.mkdir(path_centro)
        path_anno = os.path.join(path_centro, periodo[2:]) 
        if not os.path.exists(path_anno):
            os.mkdir(path_anno)
        
        archivo_salida_pdf = Path(path_anno) / nombre_pdf

        os.rename(archivo_pdf, archivo_salida_pdf)

    if os.path.exists(archivo_csv):
        arr_fichero = nombre_fichero.split('_')
        periodo = arr_fichero[0]
        empresa_codigo = arr_fichero[1]
        centro_codigo = arr_fichero[2]
        path_empresa = os.path.join(saveDir, empresa_codigo) 
        if not os.path.exists(path_empresa):
            os.mkdir(path_empresa)
        path_csv = os.path.join(path_empresa, 'csv') 
        if not os.path.exists(path_csv):
            os.mkdir(path_csv)
        path_anno = os.path.join(path_csv, periodo[2:]) 
        if not os.path.exists(path_anno):
            os.mkdir(path_anno)

        archivo_salida_csv = Path(path_anno) / nombre_csv

        os.rename(archivo_csv, archivo_salida_csv)

def main(fichero, dir, toSQL="N"):
    """
    Procesa un archivo PDF y registra eventos en una tabla de logs.
    
    Args:
        fichero (str): Nombre del archivo PDF.
        dir (str): Directorio de procesamiento.
        toSQL (str): Indicador para enviar datos a SQL ('S' o 'N').
    """
    
    try:
        # Validación inicial
        if not fichero or not re.search(r'\.pdf$', fichero, re.IGNORECASE):
            logs_datos(f"Archivo no válido o no es PDF: {fichero}")
            return

        ruta_completa = Path(dir) / fichero
        
        # Flujo de procesamiento con validación paso a paso
        try:
            fichero_renombrado = renombrar(fichero, dir)
            fichero_descifrado = descrypt(fichero_renombrado, dir)
            if not fichero_descifrado:
                raise ValueError("Fallo en descifrado")
                
            texto_procesado = pdfToText(fichero_descifrado, dir)
            if not texto_procesado:
                raise ValueError("Fallo en conversión a texto")
                
        except Exception as e:
            logs_datos(f"Error en etapa de preprocesamiento: {e}")
            return

        # Procesamiento específico por tipo de documento
        if re.search(r'RNT', texto_procesado):
            resultado = procesar_rnt(texto_procesado, dir, toSQL)
        elif re.search(r'RLC', texto_procesado):
            resultado = procesar_rlc(texto_procesado, dir, toSQL)
        else:
            logs_datos(f"Documento sin tipo definido (RNT/RLC): {fichero}")
            return

        if not resultado:
            logs_datos(f"Fallo al procesar documento: {fichero}")
            
    except Exception as e:
        logs_datos(f"Error crítico en main: {e}")
        logging.error(f"Error crítico en main: {e}", exc_info=True)

def procesar_rnt(texto_procesado, dir, toSQL):
    """Procesa documentos de tipo RNT con registro de logs."""
    try:
        csv_rnt = pdfToCsvRNT(texto_procesado, dir)
        if not csv_rnt:
            logs_datos("Fallo al generar CSV para RNT")
            return False
            
        if toSQL == 'S':
            csvToSQLRNT(csv_rnt, dir)
            organizar(csv_rnt, dir)
        return True
        
    except Exception as e:
        logs_datos(f"Error en procesamiento RNT: {e}")
        return False

def procesar_rlc(texto_procesado, dir, toSQL):
    """Procesa documentos de tipo RLC con registro de logs."""
    try:
        csv_rlc = pdfToCsvRLC(texto_procesado, dir)
        if not csv_rlc:
            logs_datos("Fallo al generar CSV para RLC")
            return False
            
        if toSQL == 'S':
            csvToSQLRLC(csv_rlc, dir)
            organizar(csv_rlc, dir)
        return True
        
    except Exception as e:
        logs_datos(f"Error en procesamiento RLC: {e}")
        return False
    

def main(fichero, dir, toSQL=False, desencriptar=True):
    error = False
    if len(fichero) > 0 and re.search(r'.pdf', fichero):
        fichero = renombrar(fichero, dir)
        if desencriptar:
            fichero = descrypt(fichero, dir)
        if fichero is not None:
            fichero = pdfToText(fichero, dir)
        else:
            error = True
        if fichero is not None:
            if re.search(r'RNT', fichero):
                fichero = pdfToCsvRNT(fichero, dir)
                if fichero is not None:
                    if toSQL:
                        #print(f'toSQL: {toSQL}')
                        csvToSQLRNT(fichero, dir)
                        organizar(fichero, dir)
                else:
                    error = True
            elif re.search(r'RLC', fichero):
                fichero = pdfToCsvRLC(fichero, dir)
                if fichero is not None:
                    if toSQL == 'S':
                        #print(f'toSQL: {toSQL}')
                        csvToSQLRLC(fichero, dir)
                        organizar(fichero, dir)
                else:
                    error = True
        else:
            error = True
        if error:
            logs_datos(f"Error al procesar el fichero: {fichero}")


def recorreDirectorio(dir, toSQL=False, desencriptar=True):
    for root, dirs, files in os.walk(dir):  # Recorrido automático
        for fname in files:
            if re.search(r'\.pdf$', fname):
                main(fname, root, toSQL, desencriptar)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Procesar parámetros de entrada")
    parser.add_argument("--toSQL", choices=["S", "N"], default="N", help="¿Cargar datos en la tabla final? (S/N)")
    parser.add_argument("--desencriptar", choices=["S", "N"], default="S", help="¿Habilitar desencriptación? (S/N)")
    parser.add_argument("ruta", nargs="?", default="rootDir", help="Ruta del archivo o directorio")

    args = parser.parse_args()

    log_tipo()

    toSQL=False
    if args.toSQL == 'S':
        toSQL=True

    desencriptar=True
    if args.desencriptar == 'N':
        desencriptar=False

    if args.ruta == "rootDir" or isdir(args.ruta):
        recorreDirectorio(args.ruta, toSQL, desencriptar)
    elif isfile(args.ruta):
        main(args.ruta, toSQL, desencriptar)
    else:
        logs_datos(f"Error: La ruta {args.ruta} especificada no es válida.")
        sys.exit(1)