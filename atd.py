import logging
import os
import signal
import sqlite3
import subprocess
import sys
import time

# TODO: do we want to make this configurable?
DB_PATH = 'pyat.db'

class NoCommands(Exception):
    pass

class SentCommand(Exception):
    pass

def ddl(conn):
    c = conn.cursor()
    c.execute('''
         CREATE TABLE IF NOT EXISTS jobs
         (command TEXT, timestamp DATETIME, working_dir TEXT, was_run BOOLEAN);
    ''')
    c.execute('''
        CREATE INDEX IF NOT EXISTS next_command_lookup
        ON jobs (timestamp) WHERE was_run = 0;
    ''')
    conn.commit()

def get_next_command(conn):
    c = conn.cursor()
    c.execute('''
       SELECT rowid, command, timestamp, working_dir
       FROM jobs
       WHERE was_run = 0
       ORDER BY timestamp
       LIMIT 1;
    ''')


    result = c.fetchone()
    if result is None:
        raise NoCommands

    rowid, command, timestamp, working_dir = tuple(result)
    difference = int(timestamp - time.time())
    time_to_wait = max(0, difference)
    return rowid, command, time_to_wait, working_dir

def execute_command(conn, id, cmd, working_dir):
    # start the command in the background, then mark it done
    # this is a critical section so we block reciept of signals here
    logging.info('executing command: {}'.format(id))
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGUSR1})
    try:

        # double fork to daemonize it
        ret = os.fork()
        if ret == 0:
            # child should start the process then exist
            os.setsid()
            # TODO: Should we redirect stdout to a file? Should we add it to sqlite?
            # TODO: retain working directory and environment
            proc = subprocess.Popen(['/bin/sh'], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=working_dir)
            proc.stdin.write(cmd.encode('utf-8'))
            os._exit(os.EX_OK)
        else:
            # wait for child to exit signifying that the process has been started
            os.waitpid(ret, 0)

        c = conn.cursor()
        c.execute('''
            UPDATE jobs
            SET was_run = 1
            WHERE rowid = ?;
        ''', (id,))
        conn.commit()
        logging.info('Marking command run: {}'.format(id))
    finally:
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGUSR1})


def serve(conn):
    while True:
        try:
            try:
                id, command, time_to_wait, working_dir = get_next_command(conn)
            except NoCommands:
                logging.debug("Got no commands")
                # It's fine to sleep for a long time, we'll get a signal when there's something to do
                time.sleep(60 * 60 * 30)
            else:
                logging.info("Sleeping for command: {}".format(id))
                time.sleep(time_to_wait)
                execute_command(conn, id, command, working_dir)
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
