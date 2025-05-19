import re
from datetime import datetime

def formatoFechas(outPutText):
	if re.search(r'\.{2,}', outPutText):
		outPutText = re.sub(r'\.{2,}', r'\.', outPutText) 
	if re.search(r'\d{2}\.+\d{6}', outPutText):
		outPutText = re.sub(r'(\d{2})\.+(\d{2})(\d{4})', r'\g<1>\/\g<2>\/\g<3>', outPutText)
	if re.search(r'\d{4}\.+\d{4}', outPutText):
		correctDate = False
		try:
			dia = int(re.sub(r'(\d{2})(\d{2})\.+(\d{4})', r'\g<1>', outPutText))
			mes = int(re.sub(r'(\d{2})(\d{2})\.+(\d{4})', r'\g<2>', outPutText))
			anno = int(re.sub(r'(\d{2})(\d{2})\.+(\d{4})', r'\g<3>', outPutText))
			datetime(anno, mes, dia)
			correctDate = True
		except ValueError:
			correctDate = False
		if correctDate:
			outPutText = re.sub(r'(\d{2})(\d{2})\.+(\d{4})', r'\g<1>\/\g<2>\/\g<3>', outPutText)
		else:
			correctDate = False
			try:
				dia = int(re.sub(r'(\d{4})\.+(\d{2})(\d{2})', r'\g<3>', outPutText))
				mes = int(re.sub(r'(\d{4})\.+(\d{2})(\d{2})', r'\g<2>', outPutText))
				anno = int(re.sub(r'(\d{4})\.+(\d{2})(\d{2})', r'\g<1>', outPutText))
				datetime(anno, mes, dia)
				correctDate = True
			except ValueError:
				correctDate = False
			if correctDate:
				outPutText = re.sub(r'(\d{4})\.+(\d{2})(\d{2})', r'\g<3>\/\g<2>\/\g<1>', outPutText)

	return outPutText