#!/usr/bin/env python3

import argparse
import sqlite3
import sys
import time


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('db_path')
    parser.add_argument('table_name')
    parser.add_argument('key')
    parser.add_argument('value')

    args = parser.parse_args()

    query = '''
    INSERT INTO {}
    ({}, timestamp, line)
    VALUES
    (?, ?, ?);
    '''.format(args.table_name, args.key)

    conn = sqlite3.connect(args.db_path)
    c = conn.cursor()
    for line in sys.stdin:
        c.execute(query, (args.value, int(time.time()), line))
        conn.commit()
