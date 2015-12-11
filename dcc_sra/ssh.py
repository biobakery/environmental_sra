import os
import time
import socket
import pipes
from os.path import join, basename

import paramiko

class SSHConnection(object):
    def __init__(self, user, host, keyfile, remote_path):
        self.remote_path = remote_path
        self.key = paramiko.RSAKey.from_private_key_file(keyfile)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, 22))
        self.transport = paramiko.Transport(self.sock)
        self.transport.start_client()
        self.transport.auth_publickey(user, self.key)
        self.chan = self.transport.open_session()
        self.chan.get_pty()
        self.chan.invoke_shell()
        self._recvall()

    def _wait(self, tries=5):
        for i in range(tries):
            time.sleep(0.125)
            if self.chan.recv_ready():
                return True
        return False
        
    def _recvall(self):
        ret = str()
        while True:
            if self.chan.recv_ready():
                ret += self.chan.recv(1024)
            else:
                if not self._wait():
                    break
        return ret

    def execute(self, cmd, verbose=False):
        if not cmd.endswith("\n"):
            cmd += "\n"
        if verbose:
            print "sending `%s'"%(cmd)
        self.chan.send(cmd)
        return self._recvall()

    def fsize(self, fname):
        quoted = pipes.quote(fname)
        output = self.execute("ls -l "+quoted).split("\n")[1].strip()
        try:
            return int(output.split(None, 8)[4])
        except ValueError:
            return None

    def uptodate(self, task, values):
        for fname in task.file_dep:
            if not os.path.exists(fname):
                return False
            remote_fname = join(self.remote_path, basename(fname))
            return self.fsize(remote_fname) == os.stat(fname).st_size
                
    def files(self):
        return self.execute("ls "+self.remote_path).strip().split('\r\n')[1:-1]

    
