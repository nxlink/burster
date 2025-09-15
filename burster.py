#!/usr/bin/env python3

import os
import csv
import argparse
import sys
import logging
import logging.handlers
import configparser
from typing import Dict, Any, List

from dotenv import load_dotenv
from tqdm import tqdm
import pymysql

pymysql.install_as_MySQLdb()
import MySQLdb as mdb


load_dotenv()


def _require(cfg: configparser.RawConfigParser, section: str, key: str) -> str:
    if cfg.has_option(section, key):
        return cfg.get(section, key)
    raise RuntimeError(f"Missing required config: [{section}] {key} (set in .env)")


def get_bbdb_creds(cfg: configparser.RawConfigParser) -> Dict[str, str]:
    return {
        "host": _require(cfg, "bbdb", "host"),
        "db": _require(cfg, "bbdb", "db"),
        "user": _require(cfg, "bbdb", "user"),
        "pass": _require(cfg, "bbdb", "pass"),
    }


def get_raddb_creds(cfg: configparser.RawConfigParser) -> Dict[str, str]:
    return {
        "host": _require(cfg, "raddb", "host"),
        "db": _require(cfg, "raddb", "db"),
        "user": _require(cfg, "raddb", "user"),
        "pass": _require(cfg, "raddb", "pass"),
    }


def get_main_config(cfg: configparser.RawConfigParser) -> Dict[str, Any]:
    return {
        "sbp": float(_require(cfg, "main", "sbp")),
        "burst_period": float(_require(cfg, "main", "burst_period")),
        "boost_perc": float(_require(cfg, "main", "boost_perc")),
        "session_timeout": _require(cfg, "main", "session_timeout"),
        "framed_pool": _require(cfg, "main", "framed_pool"),
    }


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("burster")
    if logger.handlers:
        return logger  # already configured

    level_name = os.getenv("BURSTER_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logger.setLevel(level)

    formatter = logging.Formatter("burster[%(process)d] %(levelname)s: %(message)s")

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Syslog handler: prefer local socket, fallback to UDP 514
    syslog_handler = None
    try:
        if os.path.exists("/dev/log"):
            syslog_handler = logging.handlers.SysLogHandler(address="/dev/log")
        else:
            syslog_handler = logging.handlers.SysLogHandler(address=("localhost", 514))
        syslog_handler.setLevel(level)
        syslog_handler.setFormatter(formatter)
        logger.addHandler(syslog_handler)
    except Exception:
        # If syslog not available, continue with console only
        pass

    return logger


def read_csv_file(filename: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(filename, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            rows.append(row)
    return rows


def calc_mt_rate_limit(row: Dict[str, Any], perc: int, main_config: Dict[str, Any]) -> str:
    ul_rate: Dict[str, Any] = {}
    dl_rate: Dict[str, Any] = {}
    sbp = float(main_config["sbp"])
    burst_period = float(main_config["burst_period"])
    boost_perc = float(main_config["boost_perc"]) / 100.0

    ul_rate["base"] = float(row["UL"]) * 1000 * (1 + boost_perc)
    dl_rate["base"] = float(row["DL"]) * 1000 * (1 + boost_perc)

    if int(perc) >= 100:
        ul_rate["max"] = int(ul_rate["base"] * (float(perc) / 100))
        dl_rate["max"] = int(dl_rate["base"] * (float(perc) / 100))
        return "{ul_max}k/{dl_max}k".format(
            ul_max=ul_rate["max"], dl_max=dl_rate["max"]
        )

    ul_rate["burst"] = int(ul_rate["base"])
    dl_rate["burst"] = int(dl_rate["base"])
    ul_rate["max"] = int(ul_rate["base"] * (float(perc) / 100))
    dl_rate["max"] = int(dl_rate["base"] * (float(perc) / 100))
    ul_rate["thresh"] = int(
        ((float(ul_rate["burst"]) - float(ul_rate["max"])) * float(sbp / burst_period))
        + float(ul_rate["max"])
    )
    dl_rate["thresh"] = int(
        ((float(dl_rate["burst"]) - float(dl_rate["max"])) * float(sbp / burst_period))
        + float(dl_rate["max"]) 
    )
    return (
        "{ul_max}k/{dl_max}k {ul_burst}k/{dl_burst}k {ul_thresh}k/{dl_thresh}k {bp}/{bp}"
    ).format(
        ul_max=ul_rate["max"],
        dl_max=dl_rate["max"],
        ul_burst=ul_rate["burst"],
        dl_burst=dl_rate["burst"],
        ul_thresh=ul_rate["thresh"],
        dl_thresh=dl_rate["thresh"],
        bp=int(burst_period),
    )


def update_raddb(row: Dict[str, Any], perc: int, raddb_config: Dict[str, Any], main_config: Dict[str, Any]) -> None:
    mt_rate_limit_str = calc_mt_rate_limit(row, perc, main_config)
    update_dict = {"mtratestr": mt_rate_limit_str, "groupname": row["PLAN"]}
    con = None
    try:
        con = mdb.connect(
            host=raddb_config["host"],
            db=raddb_config["db"],
            user=raddb_config["user"],
            password=raddb_config["pass"],
        )
        cur = con.cursor()
        cur.execute(
            """
            UPDATE radgroupreply
            SET value=%(mtratestr)s
            WHERE groupname=%(groupname)s AND attribute='Mikrotik-Rate-Limit';
            """,
            update_dict,
        )
    except mdb.Error as e:
        print("Error: {}".format(e))
        sys.exit(1)
    finally:
        if con:
            con.commit()
            con.close()


def read_plan_table(config: configparser.RawConfigParser) -> List[Dict[str, Any]]:
    bbdb_creds = get_bbdb_creds(config)
    con = None
    try:
        con = mdb.connect(
            host=bbdb_creds["host"],
            db=bbdb_creds["db"],
            user=bbdb_creds["user"],
            password=bbdb_creds["pass"],
        )
        cur = con.cursor(mdb.cursors.DictCursor)
        cur.execute("SELECT * FROM plans;")
        rows = cur.fetchall()
        return list(rows)
    except mdb.Error as e:
        print("Error: {}".format(e))
        sys.exit(1)
    finally:
        if con:
            con.commit()
            con.close()

def create_temp_tables(config: configparser.RawConfigParser) -> None:
    raddb_creds = get_raddb_creds(config)
    con = None
    try:
        con = mdb.connect(
            host=raddb_creds["host"],
            db=raddb_creds["db"],
            user=raddb_creds["user"],
            password=raddb_creds["pass"],
        )
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS radgroupcheck_tmp;")
        con.commit()
        cur.execute("DROP TABLE IF EXISTS radgroupreply_tmp;")
        con.commit()
        cur.execute("CREATE TABLE radgroupcheck_tmp LIKE radgroupcheck_template;")
        con.commit()
        cur.execute("CREATE TABLE radgroupreply_tmp LIKE radgroupreply_template;")
        con.commit()
    except mdb.Error as e:
        print("Error: {}".format(e))
        sys.exit(1)
    finally:
        if con:
            con.commit()
            con.close()

def swap_temp_tables(config: configparser.RawConfigParser) -> None:
    raddb_creds = get_raddb_creds(config)
    con = None
    try:
        con = mdb.connect(
            host=raddb_creds["host"],
            db=raddb_creds["db"],
            user=raddb_creds["user"],
            password=raddb_creds["pass"],
        )
        cur = con.cursor()
        cur.execute("DROP TABLE radgroupcheck;")
        con.commit()
        cur.execute("DROP TABLE radgroupreply;")
        con.commit()
        cur.execute("RENAME TABLE radgroupcheck_tmp TO radgroupcheck;")
        con.commit()
        cur.execute("RENAME TABLE radgroupreply_tmp TO radgroupreply;")
        con.commit()
    except mdb.Error as e:
        print("Error: {}".format(e))
        sys.exit(1)
    finally:
        if con:
            con.commit()
            con.close()

def update_temp_tables(config: configparser.RawConfigParser, row: Dict[str, Any], perc: int) -> None:
    raddb_creds = get_raddb_creds(config)
    main_config = get_main_config(config)
    mt_rate_limit_str = calc_mt_rate_limit(row, perc, main_config)
    ne_ul = str(
        int(float(row["UL"]) * 1_000_000 * (1 + (float(main_config["boost_perc"]) / 100)))
    )
    ne_dl = str(
        int(float(row["DL"]) * 1_000_000 * (1 + (float(main_config["boost_perc"]) / 100)))
    )
    con = None
    try:
        con = mdb.connect(
            host=raddb_creds["host"],
            db=raddb_creds["db"],
            user=raddb_creds["user"],
            password=raddb_creds["pass"],
        )
        cur = con.cursor()
        cur.execute(
            """INSERT INTO radgroupcheck_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname=row["PLAN"], attribute="Auth-Type", radop=":=", radvalue="Local"
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname=row["PLAN"],
                attribute="Session-Timeout",
                radop=":=",
                radvalue=main_config["session_timeout"],
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname=row["PLAN"],
                attribute="Framed-Pool",
                radop=":=",
                radvalue=main_config["framed_pool"],
            )
        )
        # cur.execute("""INSERT INTO radgroupreply_tmp (...) Mikrotik-Rate-Limit ... """)
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{groupname}');""".format(
                groupname=row["PLAN"], attribute="Alc-Subsc-Prof-Str", radop=":="
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{groupname}');""".format(
                groupname=row["PLAN"], attribute="Alc-SLA-Prof-Str", radop=":="
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{ne_ul}');""".format(
                groupname=row["PLAN"],
                attribute="NetElastic-Input-Average-Rate",
                radop=":=",
                ne_ul=ne_ul,
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{ne_dl}');""".format(
                groupname=row["PLAN"],
                attribute="NetElastic-Output-Average-Rate",
                radop=":=",
                ne_dl=ne_dl,
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname=row["PLAN"],
                attribute="NetElastic-Lease-Time",
                radop=":=",
                radvalue=main_config["session_timeout"],
            )
        )

        con.commit()
    except mdb.Error as e:
        print("Error: {}".format(e))
        sys.exit(1)
    finally:
        if con:
            con.commit()
            con.close()


def one_off_groups(config: configparser.RawConfigParser) -> None:
    raddb_creds = get_raddb_creds(config)
    main_config = get_main_config(config)
    con = None
    try:
        con = mdb.connect(
            host=raddb_creds["host"],
            db=raddb_creds["db"],
            user=raddb_creds["user"],
            password=raddb_creds["pass"],
        )
        cur = con.cursor()
        cur.execute(
            """INSERT INTO radgroupcheck_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="websafe", attribute="Auth-Type", radop=":=", radvalue="Local"
            )
        )
        cur.execute(
            """INSERT INTO radgroupcheck_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="nowebsafe",
                attribute="Auth-Type",
                radop=":=",
                radvalue="Local",
            )
        )
        cur.execute(
            """INSERT INTO radgroupcheck_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="cpe", attribute="Auth-Type", radop=":=", radvalue="Local"
            )
        )
        cur.execute(
            """INSERT INTO radgroupcheck_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="tech", attribute="Auth-Type", radop=":=", radvalue="Local"
            )
        )

        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="websafe", attribute="Fall-Through", radop=":=", radvalue="Yes"
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="nowebsafe",
                attribute="Fall-Through",
                radop=":=",
                radvalue="Yes",
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="nowebsafe",
                attribute="Mikrotik-Address-List",
                radop=":=",
                radvalue="nws",
            )
        )

        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="unauth",
                attribute="Mikrotik-Address-List",
                radop=":=",
                radvalue="unauth",
            )
        )

        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="unlim", attribute="Fall-Through", radop=":=", radvalue="Yes"
            )
        )

        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="tech", attribute="Fall-Through", radop=":=", radvalue="Yes"
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="tech", attribute="Framed-Pool", radop=":=", radvalue="cust"
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="tech",
                attribute="Mikrotik-Address-List",
                radop=":=",
                radvalue="nws",
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="tech", attribute="Session-Timeout", radop=":=", radvalue="3600"
            )
        )

        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="cpe", attribute="Fall-Through", radop=":=", radvalue="Yes"
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="cpe", attribute="Framed-Pool", radop=":=", radvalue="cpe"
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="cpe", attribute="Session-Timeout", radop=":=", radvalue="3600"
            )
        )

        cur.execute(
            """INSERT INTO radgroupcheck_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="7750-QOS-TEST",
                attribute="Auth-Type",
                radop=":=",
                radvalue="Local",
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="7750-QOS-TEST",
                attribute="Session-Timeout",
                radop=":=",
                radvalue="3600",
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="7750-QOS-TEST",
                attribute="Framed-Pool",
                radop=":=",
                radvalue="cust",
            )
        )
        # cur.execute("""INSERT INTO radgroupreply_tmp (...) Mikrotik-Rate-Limit ... """)
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{groupname}');""".format(
                groupname="7750-QOS-TEST",
                attribute="Alc-Subsc-Prof-Str",
                radop=":=",
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{groupname}');""".format(
                groupname="7750-QOS-TEST",
                attribute="Alc-SLA-Prof-Str",
                radop=":=",
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="7750-QOS-TEST",
                attribute="Alc-Subscriber-QoS-Override",
                radop="+=",
                radvalue="e:q:1:pir=3000,cir=3000",
            )
        )
        cur.execute(
            """INSERT INTO radgroupreply_tmp (groupname,attribute,op,value)
            VALUES ('{groupname}','{attribute}','{radop}','{radvalue}');""".format(
                groupname="7750-QOS-TEST",
                attribute="Alc-Subscriber-QoS-Override",
                radop="+=",
                radvalue="i:q:1:pir=1000,cir=1000",
            )
        )

        # websafe, nowebsafe, unauth, cpe, unlim, tech, LAB presets added above
        con.commit()
    except mdb.Error as e:
        print("Error: {}".format(e))
        sys.exit(1)
    finally:
        if con:
            con.commit()
            con.close()



def _overlay_env_to_config(cfg: configparser.RawConfigParser) -> None:
    # Map env vars to config sections/keys
    mapping = {
        "bbdb": {
            "host": "BBDB_HOST",
            "db": "BBDB_DB",
            "user": "BBDB_USER",
            "pass": "BBDB_PASS",
        },
        "raddb": {
            "host": "RADDB_HOST",
            "db": "RADDB_DB",
            "user": "RADDB_USER",
            "pass": "RADDB_PASS",
        },
        "main": {
            "sbp": "BURSTER_SBP",
            "burst_period": "BURSTER_BURST_PERIOD",
            "boost_perc": "BURSTER_BOOST_PERC",
            "session_timeout": "BURSTER_SESSION_TIMEOUT",
            "framed_pool": "BURSTER_FRAMED_POOL",
        },
    }
    for section, keys in mapping.items():
        for key, env_key in keys.items():
            val = os.getenv(env_key)
            if val is not None:
                if not cfg.has_section(section):
                    cfg.add_section(section)
                cfg.set(section, key, val)


def main() -> None:
    logger = setup_logging()

    config = configparser.RawConfigParser()
    # Only read config file if explicitly set
    config_path = os.getenv("BURSTER_CONFIG_PATH")
    if config_path and os.path.isfile(config_path):
        config.read(config_path)
    _overlay_env_to_config(config)

    parser = argparse.ArgumentParser()
    # parser.add_argument('-f', "--file", help="Plans CSV file")
    parser.add_argument(
        "-p",
        "--percent",
        help="Burst percent",
        type=int,
        default=int(os.getenv("BURSTER_PERCENT", "100")),
    )
    args = parser.parse_args()
    perc = args.percent

    # rows = read_csv_file(args.file) if args.file else read_plan_table(config)
    rows = read_plan_table(config)
    total = len(rows)
    logger.info("Loaded %d plans", total)

    logger.info("Creating temporary tables")
    create_temp_tables(config)

    logger.info("Updating temp tables for %d plans (percent=%d)", total, perc)
    log_interval = max(1, total // 10)  # 10% intervals
    with tqdm(total=total, desc="Processing plans", unit="plan") as pbar:
        for idx, row in enumerate(rows, 1):
            update_temp_tables(config, row, perc)
            pbar.update(1)
            if idx % log_interval == 0 or idx == total:
                logger.info("Progress: %d/%d (%.0f%%)", idx, total, (idx / total) * 100)

    logger.info("Adding one-off groups")
    one_off_groups(config)

    logger.info("Swapping temp tables into place")
    swap_temp_tables(config)
    logger.info("Completed updating RADIUS policy tables")






if __name__ == "__main__":
    main()
