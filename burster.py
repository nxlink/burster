#!/usr/bin/python


from ConfigParser import *
import csv
import argparse
import datetime
import MySQLdb as mdb
import MySQLdb.cursors
import sys

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
	ul_rate['base'] = float(row['UL']) * 1000 * (1 + (float(main_config["boost_perc"]) / 100))
	dl_rate['base'] = float(row['DL']) * 1000 * (1 + (float(main_config["boost_perc"]) / 100))
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


def read_plan_table(config):
	bbdb_creds = dict(config.items("bbdb"))
	try:
		con = mdb.connect(host=bbdb_creds["host"],database=bbdb_creds["db"],user=bbdb_creds["user"],password=bbdb_creds["pass"]);
		cur = con.cursor(MySQLdb.cursors.DictCursor)
		cur.execute("""SELECT * FROM plans;""")
		rows = cur.fetchall()
		return rows
	except mdb.Error, e:
		print "Error %d: %s" % (e.args[0],e.args[1])
		sys.exit(1)
	finally:
		if con:
			con.commit()
			con.close()

def create_temp_tables(config):
	raddb_creds = dict(config.items("raddb"))
	try:
		con = mdb.connect(host=raddb_creds["host"],database=raddb_creds["db"],user=raddb_creds["user"],password=raddb_creds["pass"]);
		cur = con.cursor()
		cur.execute("""DROP table IF EXISTS radgroupcheck_tmp;""")
		con.commit()
		cur.execute("""DROP table IF EXISTS radgroupreply_tmp;""")
		con.commit()
		cur.execute("""CREATE table radgroupcheck_tmp LIKE radgroupcheck_template;""")
		con.commit()
		cur.execute("""CREATE table radgroupreply_tmp LIKE radgroupreply_template;""")
		con.commit()
	except mdb.Error, e:
		print "Error %d: %s" % (e.args[0],e.args[1])
		sys.exit(1)
	finally:
		if con:
			con.commit()
			con.close()

def swap_temp_tables(config):
	raddb_creds = dict(config.items("raddb"))
	try:
		con = mdb.connect(host=raddb_creds["host"],database=raddb_creds["db"],user=raddb_creds["user"],password=raddb_creds["pass"]);
		cur = con.cursor()
		cur.execute("""DROP table radgroupcheck;""")
		con.commit()
		cur.execute("""DROP table radgroupreply;""")
		con.commit()
		cur.execute("""RENAME table radgroupcheck_tmp to radgroupcheck;""")
		con.commit()
		cur.execute("""RENAME table radgroupreply_tmp to radgroupreply;""")
		con.commit()
	except mdb.Error, e:
		print "Error %d: %s" % (e.args[0],e.args[1])
		sys.exit(1)
	finally:
		if con:
			con.commit()
			con.close()

def update_temp_tables(config,row,perc):
	raddb_creds = dict(config.items("raddb"))
	main_config = dict(config.items("main"))
	mt_rate_limit_str = CalcMTRateLimit(row,perc,main_config)

	try:
		con = mdb.connect(host=raddb_creds["host"],database=raddb_creds["db"],user=raddb_creds["user"],password=raddb_creds["pass"]);
		cur = con.cursor()
		cur.execute("""INSERT into radgroupcheck_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname=row['PLAN'],attribute="Auth-Type",radop=":=",radvalue="Local"))
		cur.execute("""INSERT into radgroupreply_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname=row['PLAN'],attribute="Session-Timeout",radop=":=",radvalue=main_config["session_timeout"]))
		cur.execute("""INSERT into radgroupreply_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname=row['PLAN'],attribute="Framed-Pool",radop=":=",radvalue=main_config["framed_pool"]))
		cur.execute("""INSERT into radgroupreply_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname=row['PLAN'],attribute="Mikrotik-Rate-Limit",radop=":=",radvalue=mt_rate_limit_str))
		con.commit()
	except mdb.Error, e:
		print "Error %d: %s" % (e.args[0],e.args[1])
		sys.exit(1)
	finally:
		if con:
			con.commit()
			con.close()


def one_off_groups(config):
	raddb_creds = dict(config.items("raddb"))
	main_config = dict(config.items("main"))
	try:
		con = mdb.connect(host=raddb_creds["host"],database=raddb_creds["db"],user=raddb_creds["user"],password=raddb_creds["pass"]);
		cur = con.cursor()
		cur.execute("""INSERT into radgroupcheck_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname="websafe",attribute="Auth-Type",radop=":=",radvalue="Local"))
		cur.execute("""INSERT into radgroupcheck_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname="nowebsafe",attribute="Auth-Type",radop=":=",radvalue="Local"))
		cur.execute("""INSERT into radgroupcheck_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname="cpe",attribute="Auth-Type",radop=":=",radvalue="Local"))
		cur.execute("""INSERT into radgroupcheck_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname="tech",attribute="Auth-Type",radop=":=",radvalue="Local"))

		cur.execute("""INSERT into radgroupreply_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname="websafe",attribute="Fall-Through",radop=":=",radvalue="Yes"))
		cur.execute("""INSERT into radgroupreply_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname="nowebsafe",attribute="Fall-Through",radop=":=",radvalue="Yes"))
		cur.execute("""INSERT into radgroupreply_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname="nowebsafe",attribute="Mikrotik-Address-List",radop=":=",radvalue="nws"))

		cur.execute("""INSERT into radgroupreply_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname="unauth",attribute="Mikrotik-Address-List",radop=":=",radvalue="unauth"))

		cur.execute("""INSERT into radgroupreply_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname="unlim",attribute="Fall-Through",radop=":=",radvalue="Yes"))

		cur.execute("""INSERT into radgroupreply_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname="tech",attribute="Fall-Through",radop=":=",radvalue="Yes"))
		cur.execute("""INSERT into radgroupreply_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname="tech",attribute="Framed-Pool",radop=":=",radvalue="cust"))
		cur.execute("""INSERT into radgroupreply_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname="tech",attribute="Mikrotik-Address-List",radop=":=",radvalue="nws"))
		cur.execute("""INSERT into radgroupreply_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname="tech",attribute="Session-Timeout",radop=":=",radvalue="3600"))

		cur.execute("""INSERT into radgroupreply_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname="cpe",attribute="Fall-Through",radop=":=",radvalue="Yes"))
		cur.execute("""INSERT into radgroupreply_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname="cpe",attribute="Framed-Pool",radop=":=",radvalue="cpe"))
		cur.execute("""INSERT into radgroupreply_tmp (groupname,attribute,op,value) VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(groupname="cpe",attribute="Session-Timeout",radop=":=",radvalue="3600"))

		#websafe
		#nowebsafe
		#unauth
		#cpe
		#unlim
		#tech
		#LAB

		con.commit()
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
	#parser.add_argument('-f',"--file", help="Plans CSV file")
	parser.add_argument('-p',"--percent", help="Burst percent")
	args = parser.parse_args()
	perc = args.percent
	#rowdictlist = ReadCSVFile(args.file)
	rowdictlist = read_plan_table(config)
	print rowdictlist
	create_temp_tables(config)
	for row in rowdictlist:
		update_temp_tables(config,row,perc)
	one_off_groups(config)
	swap_temp_tables(config)
	# raddb_config = dict(config.items("raddb"))
	# main_config = dict(config.items("main"))
	# print perc
	# for row in rowdictlist:
	# 	UpdateRADDB(row,perc,raddb_config,main_config)
	# 	calcstr = CalcMTRateLimit(row,perc,main_config)
	# 	print "%s,%s" % (row['PLAN'],calcstr)






if __name__ == "__main__":
	main()
