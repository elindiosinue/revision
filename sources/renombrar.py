#!/usr/bin/python3
# importing required modules 
import sys
import os
import re
from os import listdir
from os.path import isfile, isdir

rootDir = './'

def renombrar(fichero, dir):
	arr = fichero.split('.')
	extension = arr[-1]
	nombreFichero = fichero.replace(f".{extension}", '')
	nombreFichero = nombreFichero.upper()
	nombreFichero = re.sub(r' ', '_', nombreFichero)
	nombreFichero = re.sub(r'\-', '_', nombreFichero)
	nombreFichero = re.sub(r'\.', '_', nombreFichero)
	nombreFichero = re.sub(r'\,', '_', nombreFichero)
	nombreFichero = re.sub(r'[ªºº]', '', nombreFichero)
	nombreFichero = re.sub(r'[Á╡]', 'A', nombreFichero)
	nombreFichero = re.sub(r'É', 'E', nombreFichero)
	nombreFichero = re.sub(r'[Í╓]', 'I', nombreFichero)
	nombreFichero = re.sub(r'Ó', 'O', nombreFichero)
	nombreFichero = re.sub(r'Ú', 'U', nombreFichero)
	nombreFichero = re.sub(r'_+', '_', nombreFichero)
	os.rename(f"{dir}/{fichero}", f"{dir}/{nombreFichero}.{extension}")
	return f"{nombreFichero}.{extension}"


def main(fichero, dir):
	if len(fichero) > 0:
		fichero = renombrar(fichero, dir)

def recorreDirectorio(dir):
	if isdir(dir):
		for fname in listdir(dir):
			if isfile(dir + '/' + fname):
				main(fname, dir)

if __name__ == '__main__':
	# os.system("mkdir /ruta/al/directorio")
	if len(sys.argv) > 1 and isfile(str(sys.argv[1])):
		main(str(sys.argv[1]), rootDir)
	else:
		if len(sys.argv) == 1:
			dir = rootDir
		else:
			dir = str(sys.argv[1])
		recorreDirectorio(dir)
