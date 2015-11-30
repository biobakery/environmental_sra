from ftplib import FTP
from ftplib import all_errors as ftp_errors
from os import stat
from os.path import exists
from os.path import basename

def fsize(fname):
    return stat(fname).st_size

# can't draft this module too much yet because I don't have test
# credentials for ftp
def gen_uptodate(remote_srv, remote_path, user, ftp_pass):
    ftp = FTP(remote_srv, user, ftp_pass)
    ftp.cwd(remote_path)
    remote_fnames = dict()
    for n in ftp.nlst():
        try:
            remote_fnames[n] = ftp.size()
        except ftp_errors:
            pass
    def _utd(task):
        for f in task.file_dep:
            bn = basename(f)
            if bn not in remote_fnames:
                return False
            if fsize(f) == remote_fnames[bn]:
                return True
            return False
    return _utd
