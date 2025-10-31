#!/usr/bin/env python3

import os
import csv
import argparse
import sys
import logging
import logging.handlers
import configparser
from typing import Dict, Any, List, Tuple

from dotenv import load_dotenv
from tqdm import tqdm
import pymysql

pymysql.install_as_MySQLdb()
import MySQLdb as mdb
import pandas as pd


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

def build_plan_attribute_rows(
    row: Dict[str, Any], perc: int, main_config: Dict[str, Any]
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    mt_rate_limit_str = calc_mt_rate_limit(row, perc, main_config)
    ne_ul = str(
        int(float(row["UL"]) * 1_000_000 * (1 + (float(main_config["boost_perc"]) / 100)))
    )
    ne_dl = str(
        int(float(row["DL"]) * 1_000_000 * (1 + (float(main_config["boost_perc"]) / 100)))
    )

    radgroupcheck_rows = [
        {
            "groupname": row["PLAN"],
            "attribute": "Auth-Type",
            "op": ":=",
            "value": "Local",
        }
    ]

    radgroupreply_rows = [
        {
            "groupname": row["PLAN"],
            "attribute": "Session-Timeout",
            "op": ":=",
            "value": str(main_config["session_timeout"]),
        },
        {
            "groupname": row["PLAN"],
            "attribute": "Framed-Pool",
            "op": ":=",
            "value": str(main_config["framed_pool"]),
        },
        # {
        #     "groupname": row["PLAN"],
        #     "attribute": "Mikrotik-Rate-Limit",
        #     "op": ":=",
        #     "value": mt_rate_limit_str,
        # },
        {
            "groupname": row["PLAN"],
            "attribute": "Alc-Subsc-Prof-Str",
            "op": ":=",
            "value": row["PLAN"],
        },
        {
            "groupname": row["PLAN"],
            "attribute": "Alc-SLA-Prof-Str",
            "op": ":=",
            "value": row["PLAN"],
        },
        {
            "groupname": row["PLAN"],
            "attribute": "NetElastic-Input-Average-Rate",
            "op": ":=",
            "value": ne_ul,
        },
        {
            "groupname": row["PLAN"],
            "attribute": "NetElastic-Output-Average-Rate",
            "op": ":=",
            "value": ne_dl,
        },
        {
            "groupname": row["PLAN"],
            "attribute": "NetElastic-Lease-Time",
            "op": ":=",
            "value": str(main_config["session_timeout"]),
        },
        {
            "groupname": row["PLAN"],
            "attribute": "Filter-Id",
            "op": ":=",
            "value": "cst-acl-profile",
        },
        {
            "groupname": row["PLAN"],
            "attribute": "NetElastic-Portal-Mode",
            "op": ":=",
            "value": "0",
        },
    ]

    return radgroupcheck_rows, radgroupreply_rows


def append_one_off_groups(
    radgroupcheck_rows: List[Dict[str, str]],
    radgroupreply_rows: List[Dict[str, str]],
) -> None:
    radgroupcheck_rows.extend(
        [
            {"groupname": "websafe", "attribute": "Auth-Type", "op": ":=", "value": "Local"},
            {"groupname": "nowebsafe", "attribute": "Auth-Type", "op": ":=", "value": "Local"},
            {"groupname": "cpe", "attribute": "Auth-Type", "op": ":=", "value": "Local"},
            {"groupname": "tech", "attribute": "Auth-Type", "op": ":=", "value": "Local"},
            {
                "groupname": "7750-QOS-TEST",
                "attribute": "Auth-Type",
                "op": ":=",
                "value": "Local",
            },
        ]
    )

    radgroupreply_rows.extend(
        [
            {
                "groupname": "websafe",
                "attribute": "Fall-Through",
                "op": ":=",
                "value": "Yes",
            },
            {
                "groupname": "nowebsafe",
                "attribute": "Fall-Through",
                "op": ":=",
                "value": "Yes",
            },
            {
                "groupname": "nowebsafe",
                "attribute": "Mikrotik-Address-List",
                "op": ":=",
                "value": "nws",
            },
            {
                "groupname": "unauth",
                "attribute": "Mikrotik-Address-List",
                "op": ":=",
                "value": "unauth",
            },
            {
                "groupname": "unauth",
                "attribute": "Filter-Id",
                "op": ":=",
                "value": "unauth-acl-profile",
            },
            {
                "groupname": "unauth",
                "attribute": "NetElastic-Portal-Mode",
                "op": ":=",
                "value": "1",
            },
            {
                "groupname": "unauth",
                "attribute": "NetElastic-HTTP-Redirect-URL",
                "op": ":=",
                "value": "http://captive-new.nxlink.com",
            },
            {
                "groupname": "unlim",
                "attribute": "Fall-Through",
                "op": ":=",
                "value": "Yes",
            },
            {
                "groupname": "tech",
                "attribute": "Fall-Through",
                "op": ":=",
                "value": "Yes",
            },
            {
                "groupname": "tech",
                "attribute": "Framed-Pool",
                "op": ":=",
                "value": "cust",
            },
            {
                "groupname": "tech",
                "attribute": "Mikrotik-Address-List",
                "op": ":=",
                "value": "nws",
            },
            {
                "groupname": "tech",
                "attribute": "Session-Timeout",
                "op": ":=",
                "value": "3600",
            },
            {
                "groupname": "cpe",
                "attribute": "Fall-Through",
                "op": ":=",
                "value": "Yes",
            },
            {
                "groupname": "cpe",
                "attribute": "Framed-Pool",
                "op": ":=",
                "value": "cpe",
            },
            {
                "groupname": "cpe",
                "attribute": "Session-Timeout",
                "op": ":=",
                "value": "3600",
            },
            {
                "groupname": "7750-QOS-TEST",
                "attribute": "Session-Timeout",
                "op": ":=",
                "value": "3600",
            },
            {
                "groupname": "7750-QOS-TEST",
                "attribute": "Framed-Pool",
                "op": ":=",
                "value": "cust",
            },
            {
                "groupname": "7750-QOS-TEST",
                "attribute": "Alc-Subsc-Prof-Str",
                "op": ":=",
                "value": "7750-QOS-TEST",
            },
            {
                "groupname": "7750-QOS-TEST",
                "attribute": "Alc-SLA-Prof-Str",
                "op": ":=",
                "value": "7750-QOS-TEST",
            },
            {
                "groupname": "7750-QOS-TEST",
                "attribute": "Alc-Subscriber-QoS-Override",
                "op": "+=",
                "value": "e:q:1:pir=3000,cir=3000",
            },
            {
                "groupname": "7750-QOS-TEST",
                "attribute": "Alc-Subscriber-QoS-Override",
                "op": "+=",
                "value": "i:q:1:pir=1000,cir=1000",
            },
        ]
    )

    # Ensure unauth group exists even without accompanying check rows
    if not any(row["groupname"] == "unauth" for row in radgroupcheck_rows):
        radgroupcheck_rows.append(
            {
                "groupname": "unauth",
                "attribute": "Auth-Type",
                "op": ":=",
                "value": "Local",
            }
        )


def bulk_insert_dataframe(
    config: configparser.RawConfigParser, table_name: str, dataframe: pd.DataFrame
) -> None:
    if dataframe.empty:
        return

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
        columns = ["groupname", "attribute", "op", "value"]
        rows = [
            tuple(str(value) for value in record)
            for record in dataframe[columns].itertuples(index=False, name=None)
        ]
        cur.executemany(
            f"INSERT INTO {table_name} (groupname, attribute, op, value) VALUES (%s, %s, %s, %s);",
            rows,
        )
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

    main_config = get_main_config(config)
    radgroupcheck_rows: List[Dict[str, str]] = []
    radgroupreply_rows: List[Dict[str, str]] = []

    logger.info("Building attribute dataframes for %d plans (percent=%d)", total, perc)
    log_interval = max(1, total // 10)  # 10% intervals
    with tqdm(total=total, desc="Processing plans", unit="plan") as pbar:
        for idx, row in enumerate(rows, 1):
            plan_check_rows, plan_reply_rows = build_plan_attribute_rows(row, perc, main_config)
            radgroupcheck_rows.extend(plan_check_rows)
            radgroupreply_rows.extend(plan_reply_rows)
            pbar.update(1)
            if idx % log_interval == 0 or idx == total:
                logger.info("Progress: %d/%d (%.0f%%)", idx, total, (idx / total) * 100)

    logger.info("Appending one-off groups")
    append_one_off_groups(radgroupcheck_rows, radgroupreply_rows)

    radgroupcheck_df = pd.DataFrame(radgroupcheck_rows, columns=["groupname", "attribute", "op", "value"])
    radgroupreply_df = pd.DataFrame(radgroupreply_rows, columns=["groupname", "attribute", "op", "value"])

    logger.info(
        "Inserting %d radgroupcheck rows and %d radgroupreply rows",
        len(radgroupcheck_df),
        len(radgroupreply_df),
    )
    bulk_insert_dataframe(config, "radgroupcheck_tmp", radgroupcheck_df)
    bulk_insert_dataframe(config, "radgroupreply_tmp", radgroupreply_df)

    logger.info("Swapping temp tables into place")
    swap_temp_tables(config)
    logger.info("Completed updating RADIUS policy tables")






if __name__ == "__main__":
    main()
