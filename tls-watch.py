#
# This was an attempt at parsing the mail logs, to automatically add to the
# TLS exception list hosts that retried. This was based on the mistaken belief
# that spammers who don't use TLS also don't retry, but an analysis of
# a few days' logs disproved it.
#
# The only piece that's done (but untested) is the overall logic to tail the
# log file, and handle file rotation, hopefully without race conditions...


import os
import click
import time

SLEEP_TIME = 0.5

@click.command()
@click.argument('logfile', type=click.Path(), help='Log file to tail')
@click.option('--exceptions-file', type=click.Path(), default='/tmp/tls_except'
              help='The exception file ')
def main(logfile):
    inode = None
    log_fh = None
    while True:
        try:
            if not log_fh:
                log_fh = open(logfile)
        except FileNotFoundError:
            inode = None
            time.sleep(SLEEP_TIME)
            continue

        while line := log_fh.readline():
            # read 'till EoF
            pass

        # end of file
        st = os.stat(log_fh)
        if inode and st.st_ino != inode: # File rotated
            # Process any last lines
            while line := log_fh.readline():
                # read 'till EoF
                pass
            log_fh.close()
            log_fh = None
            inode = st.st_ino

        time.sleep(SLEEP_TIME)

class TlsExceptions:
    def __init__():
        pass
