#
# Given a cyrus-imap server whose data store is in a dedicated LVM2 LV,
# shut down the server, take an LVM snapshot, restart cyrus, clear postfix queue,
# rsync the snapshot files to a remote host, remove the snapshot, and send
# a message via pushover (optionally).
#

import click
import os
from pathlib import Path
from pystemd.systemd1 import Unit
import requests
from subprocess import run, check_output, PIPE, STDOUT, DEVNULL, CompletedProcess
from syslog import openlog, syslog, LOG_ERR, LOG_MAIL
import time
import yaml

class LocalError(RuntimeError):
    pass

PUSHOVER_TOKEN = None
PUSHOVER_USER_KEY = None

@click.command()
@click.option('--lv-name', help='Name of logical volume to snapshot', required=True)
@click.option('--vg-name', help='Name of containing volume group', required=True)
@click.option('--rsync-host', '-h', help='rsync target host name', required=True)
@click.option('--pushover-yaml', help='Pushover file containing TOKEN and USER_KEY for alerting')
@click.option('--force/--no-force', default=False,
              help='Forcibly remove existing LVM2 snapshot and/or mountpoint')
def main(lv_name, vg_name, rsync_host, pushover_yaml, force):
    try:
        cyrus = None
        mount_point = f'/mnt/{lv_name}_bkup'
        backup_vol = f'{lv_name}_bkup'
        lv_full_name = f'{vg_name}/{lv_name}'
        bkup_lv_full_name = f'{vg_name}/{backup_vol}'
        validate(lv_name=lv_full_name, bkup_lv_name=bkup_lv_full_name, force=force,
                 mount_point=mount_point, pushover_yaml=pushover_yaml)

        # stop cyrus-imapd
        cyrus = Unit(b'cyrus-imapd.service')
        cyrus.load()
        cyrus.Stop(b'replace')
        time.sleep(5)
        if cyrus.SubState != b'dead':
            raise LocalError('Failed to stop cyrus-imapd. systemd unit is in state {cyrus.SubState}')

        # make snapshot
        proc = run(f'lvcreate --snapshot --name {backup_vol} --size 100M /dev/{lv_full_name}'.split(),
                   capture_output=True, text=True)
        check_proc(proc, 'Failed to create snapshot')

        # restart cyrus-imapd
        cyrus.Start(b'replace')

        # mount snapshot
        proc = run(f'mount /dev/{vg_name}/{backup_vol} {mount_point} -o ro'.split(),
                   capture_output=True, text=True)
        check_proc(proc, 'Failed to mount snapshot')

        # rsync to backup host
        rsync_cmd = ('rsync --archive --relative --sparse --hard-links --one-file-system --delete '
                     f'--numeric-ids --rsh=ssh --fake-super --numeric-ids {mount_point} '
                     f'{rsync_host}:{lv_name}')
        proc = run(rsync_cmd.split(), stdout=DEVNULL, stderr=PIPE, text=True)
        check_proc(proc, 'Failed to rsync to backup host')

        # unmount & remove snapshot
        proc = run(f'umount {mount_point}'.split(), text=True, capture_output=True)
        check_proc(proc, f'Failed to unmount {mount_point}')
        proc = run(f'lvremove --yes /dev/{vg_name}/{backup_vol}'.split(), capture_output=True,
                   text=True)
        check_proc(proc, 'Failed to remove logical volume {vg_name}/{backup_vol}')
        proc = run(f'rmdir {mount_point}'.split(), stdout=DEVNULL, stderr=PIPE, text=True)
        check_proc(proc, f'Failed to remove mountpoint directory {mount_point}')

        # Verify cyrus is running
        if not (cyrus.ActiveState == b'active' and cyrus.SubState == b'running'):
            raise LocalError('cyrus-imapd may be down after backup: ActiveState '
                             f'{cyrus.ActiveState} SubState{cyrus.SubState}')

        proc = run(f'postqueue -f'.split(), stdout=PIPE, stderr=STDOUT, text=True)
        if proc.returncode != 0:
            notify(pushover_yaml is not None, f'Failed to flush postfix queue: {proc.stdout}')

        notify(pushover_yaml is not None, 'Successfully backed up cyrus volume')

    except Exception as exc:
        notify(pushover_yaml is not None, repr(exc))
        raise exc

    finally:
        # Hail-Mary attempt to ensure cyrus-imapd is (re)started, no matter what.
        # It's a no-op if it's running already
        if cyrus:
            cyrus.Start(f'replace')

def check_proc(proc: CompletedProcess, err_msg: str):
    if proc.returncode == 0:
        if proc.stdout and proc.stdout.strip():
            print(proc.stdout.strip())
    else:
        raise LocalError(f'{err_msg}: {proc.stderr}')


def notify(pushover: bool, msg: str) -> None:
    if pushover:
        data = { "token"   : PUSHOVER_TOKEN,
                 "user"    : PUSHOVER_USER_KEY,
                 "message" : msg  }
        resp = requests.post('https://api.pushover.net/1/messages.json', data=data)
        if resp.status_code != 200:
            raise LocalError('Unable to send pushover notification: HTTP {resp.status_code}')
    else:
        openlog(facility=LOG_MAIL)
        syslog(LOG_ERR, 'msg')


def validate(lv_name=None, bkup_lv_name=None, force=False, mount_point=None, pushover_yaml=None):
    """
    Check that utilities exist, etc...
    Also, makes sure the mountpoint exists
    """
    out = run('lvs --options lv_full_name --noheadings'.split(), check=True, stdout=PIPE,
              stderr=None, text=True).stdout.strip()
    if lv_name not in out:
        raise LocalError(f'LVM volume {lv_name} not found')

    tests = (('rsync --version', 'protocol version'),
             ('lvcreate --version', 'LVM version'),
             ('lvremove --version', 'LVM version'),
             ('mount --version', 'mount from util-linux'),
             ('umount --version', 'umount from util-linux'),
             ('postqueue -p', ''))

    for (cmd, output) in tests:
        proc = run(cmd.split(), check=True, capture_output=True, text=True)
        if output not in proc.stdout:
            raise LocalError(f'Output from "{cmd}" ({proc.stdout.strip()})did not match expected "{output}"')

    if os.path.exists(mount_point):
        if force:
            mounted = check_output(['mount'], text=True)
            if f'on {mount_point} ' in mounted:
                proc = run(f'umount {mount_point}'.split(), capture_output=True, text=True)
                if proc.returncode != 0:
                    if proc.stderr:
                        raise LocalError(f'umount error: {proc.stderr.strip()}')
                    else:
                        raise LocalError(f'umount exited {proc.returncode}')
                mounted = check_output(['mount'], text=True)
                if f'on {mount_point} ' in mounted:
                    raise LocalError(f'Unable to unmount {mount_point}')
        else:
            raise LocalError(f'mount point {mount_point} exists')
    else:
        proc = run(f'mkdir {mount_point}'.split(), stdout=PIPE, stderr=STDOUT, text=True)
        if proc.returncode != 0:
            if proc.stdout:
                raise LocalError(f'mkdir failed: {proc.stdout.strip()}')
            else:
                raise LocalError(f'mkdir failed (exited {proc.returncode})')

    out = run('lvs --options lv_full_name --noheadings'.split(), check=True, stdout=PIPE,
              stderr=None, text=True).stdout.strip()
    if bkup_lv_name in out:
        if force:
            proc = run(f'lvremove --yes /dev/{bkup_lv_name}'.split())
            check_proc(proc, 'Failed to remove logical volume {bkup_lv_name}')
        else:
            raise LocalError(f'Backup LV present ({bkup_lv_name})')


    if pushover_yaml:
        pushover_yaml = Path(pushover_yaml)
        if not pushover_yaml.exists():
            raise LocalError(f'Pushover YAML file {pushover_yaml} does not exist')
        pushover_data = yaml.safe_load(pushover_yaml.open())
        if not 'user_key' in pushover_data \
           and 'MailBackup' in pushover_data \
           and 'token' in pushover_data['MailBackup']:
            raise LocalError('Structure of YAML pushover file incorrect')

        global PUSHOVER_USER_KEY, PUSHOVER_TOKEN
        PUSHOVER_USER_KEY = pushover_data['user_key']
        PUSHOVER_TOKEN = pushover_data['MailBackup']['token']

if __name__ == '__main__':
    main()
