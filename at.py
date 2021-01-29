import argparse
import datetime
import glob
import os
import signal
import sqlite3
import sys

from atd import DB_PATH

ATD_SENTINAL = b'atd.py'

class ProcNotFound(Exception):
    pass

def write_job_record(conn, command, ts, working_dir):
    c = conn.cursor()
    c.execute('''
    INSERT INTO jobs
    (command, timestamp, working_dir, was_run)
    VALUES
    (?, ?, ?, false)
    ''', (command, ts, working_dir))
    conn.commit()


def parse_time(time_str):
    # TODO: accept more formats
    dt = datetime.datetime.fromisoformat(time_str)
    if dt < datetime.datetime.now(tz=datetime.timezone.utc):
        raise ValueError("Scheduled time must be in the future")
    return int(dt.timestamp())


# TODO: is there a better way? Maybe have atd process write a pidfile?
def find_pid(sentinal):
    for proc_file in glob.glob('/proc/*/cmdline'):
        with open(proc_file, 'rb') as f:
            data = f.read()
        if sentinal in data:
            return int(proc_file.split('/')[2])
    raise ProcNotFound

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('time')
    args = parser.parse_args()
    cmd = sys.stdin.read()

    parsed_time = parse_time(args.time)

    try:
        atd_pid = find_pid(ATD_SENTINAL)
    except ProcNotFound:
        print("Couldn't find atd process")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    working_dir = os.getcwd()
    write_job_record(conn, cmd, parsed_time, working_dir)

    # notify atd
    os.kill(atd_pid, signal.SIGUSR1)
    print('Scheduled {} to run at {}'.format(cmd, args.time))
