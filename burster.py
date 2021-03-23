#!/usr/bin/python


from ConfigParser import *
import csv
import argparse
import datetime
import MySQLdb as mdb
import MySQLdb.cursors

def ReadCSVFile(filename):
	rowdictlist = []
	with open(filename) as csvfile:
		reader = csv.DictReader(csvfile)
    		for row in reader:
				rowdictlist.append(row)
	return rowdictlist

def CalcMTRateLimit(row,perc,main_config):
	ul_rate = {}
	dl_rate = {}
	sbp = float(main_config['sbp'])
	burst_period = float(main_config['burst_period'])
	ul_rate['base'] = float(row['UL']) * 1000
	dl_rate['base'] = float(row['DL']) * 1000
	if (int(perc) >= 100):
		ul_rate['max'] = int(ul_rate['base'] * (float(perc) / 100))
		dl_rate['max'] = int(dl_rate['base'] * (float(perc) / 100))
		return_str = "{ul_max}k/{dl_max}k".format(ul_max=ul_rate['max'],dl_max=dl_rate['max'])
		return return_str
	elif (int(perc) < 100):
		ul_rate['burst'] = int(ul_rate['base'])
		dl_rate['burst'] = int(dl_rate['base'])
		ul_rate['max'] = int(ul_rate['base'] * (float(perc) / 100))
		dl_rate['max'] = int(dl_rate['base'] * (float(perc) / 100))
		ul_rate['thresh'] = int(((float(ul_rate['burst'])-float(ul_rate['max'])) * float(sbp / burst_period)) + float(ul_rate['max']))
		dl_rate['thresh'] = int(((float(dl_rate['burst'])-float(dl_rate['max'])) * float(sbp / burst_period)) + float(dl_rate['max']))
		return_str = "{ul_max}k/{dl_max}k {ul_burst}k/{dl_burst}k {ul_thresh}k/{dl_thresh}k {burst_period}/{burst_period}".format(ul_max=ul_rate['max'],dl_max=dl_rate['max'],ul_burst=ul_rate['burst'],dl_burst=dl_rate['burst'],ul_thresh=ul_rate['thresh'],dl_thresh=dl_rate['thresh'],burst_period=int(burst_period))
		return return_str

def UpdateRADDB(row,perc,raddb_config,main_config):
	mt_rate_limit_str = CalcMTRateLimit(row,perc,main_config)
	update_dict = {}
	update_dict["mtratestr"] = mt_rate_limit_str
	update_dict["groupname"] = row["PLAN"]
	try:
		con = mdb.connect(host=raddb_config["host"],database=raddb_config["db"],user=raddb_config["user"],password=raddb_config["pass"]);
		cur = con.cursor()
		cur.execute("""
			UPDATE radgroupreply SET value=%(mtratestr)s WHERE groupname=%(groupname)s and attribute='Mikrotik-Rate-Limit';
				""", update_dict)
	except mdb.Error, e:
		print "Error %d: %s" % (e.args[0],e.args[1])
		sys.exit(1)
	finally:
		if con:
			con.commit()
			con.close()

def main():
	rowdictlist = []
	config = RawConfigParser()
	config.read('/opt/burster/burster.cfg')
	parser = argparse.ArgumentParser()
	parser.add_argument('-f',"--file", help="Plans CSV file")
	parser.add_argument('-p',"--percent", help="Burst percent")
	args = parser.parse_args()
	perc = args.percent
	rowdictlist = ReadCSVFile(args.file)
	raddb_config = dict(config.items("raddb"))
	main_config = dict(config.items("main"))
	print perc
	for row in rowdictlist:
		UpdateRADDB(row,perc,raddb_config,main_config)
		calcstr = CalcMTRateLimit(row,perc,main_config)
		print "%s,%s" % (row['PLAN'],calcstr)






if __name__ == "__main__":
	main()
