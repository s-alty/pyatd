import collections
import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import time

# TODO: do we want to make this configurable?
DB_PATH = 'pyat.db'

Job = collections.namedtuple('Job', 'id, command, working_dir, environment')


class NoCommands(Exception):
    pass

class SentCommand(Exception):
    pass

def ddl(conn):
    c = conn.cursor()
    c.execute('''
         CREATE TABLE IF NOT EXISTS jobs
         (id INTEGER PRIMARY KEY, command TEXT, timestamp DATETIME, working_dir TEXT, environment TEXT, was_run BOOLEAN);
    ''')
    c.execute('''
        CREATE INDEX IF NOT EXISTS next_command_lookup
        ON jobs (timestamp) WHERE was_run = 0;
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS output
        (id INTEGER PRIMARY KEY, jobid INTEGER, timestamp DATETIME, line TEXT,
        FOREIGN KEY(jobid) REFERENCES jobs(id));
    ''')
    c.execute('''
        CREATE INDEX IF NOT EXISTS output_by_job_and_time
        ON output (jobid, timestamp)
    ''')
    conn.commit()

def get_next_command(conn):
    c = conn.cursor()
    c.execute('''
       SELECT id, command, timestamp, working_dir, environment
       FROM jobs
       WHERE was_run = 0
       ORDER BY timestamp
       LIMIT 1;
    ''')


    result = c.fetchone()
    if result is None:
        raise NoCommands

    id, command, timestamp, working_dir, env = tuple(result)
    environment = json.loads(env)

    difference = int(timestamp - time.time())
    time_to_wait = max(0, difference)
    return Job(id, command, working_dir, environment), time_to_wait

def execute_command(conn, job):
    # start the command in the background, then mark it done
    # this is a critical section so we block reciept of signals here
    logging.info('executing command: {}'.format(job.id))
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGUSR1})
    try:

        # double fork to daemonize it
        ret = os.fork()
        if ret == 0:
            # child should start the process then exist
            os.setsid()

            # create a pipe to redirect the output of the user's command to a second process
            # the second process reads from stdin and inserts a record into the sqlite db
            rfd, wfd = os.pipe()
            proc = subprocess.Popen(['/bin/sh'], stdin=subprocess.PIPE, stdout=wfd, stderr=subprocess.DEVNULL, cwd=job.working_dir, env=job.environment)

            sqlite_writer = subprocess.Popen(['./sqlite_writer.py', DB_PATH, 'output', 'jobid', str(job.id)], stdin=rfd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            proc.stdin.write(job.command.encode('utf-8'))
            proc.stdin.close()
            os._exit(os.EX_OK)
        else:
            # wait for child to exit signifying that the process has been started
            os.waitpid(ret, 0)

        c = conn.cursor()
        c.execute('''
            UPDATE jobs
            SET was_run = 1
            WHERE id = ?;
        ''', (job.id,))
        conn.commit()
        logging.info('Marking command run: {}'.format(job.id))
    finally:
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGUSR1})


def serve(conn):
    while True:
        try:
            try:
                job, time_to_wait = get_next_command(conn)
            except NoCommands:
                logging.debug("Got no commands")
                # It's fine to sleep for a long time, we'll get a signal when there's something to do
                time.sleep(60 * 60 * 30)
            else:
                logging.info("Sleeping for command: {}".format(job.id))
                time.sleep(time_to_wait)
                execute_command(conn, job)
        except SentCommand:
            # we were sent a new command, so restart the loop to check for the next command
            continue


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    conn = sqlite3.connect(DB_PATH)
    ddl(conn)

    def handle_notification(signum, stackframe):
        raise SentCommand()

    signal.signal(signal.SIGUSR1, handle_notification) # register a signal handler for sigusr1
    serve(conn)
