import os
import re
import sys
import json
import time
from os.path import join
from os.path import dirname
from os.path import basename
from os.path import exists
from urlparse import urlparse
from collections import defaultdict
import xml.etree.ElementTree as ET

from anadama.util import new_file
from anadama.util import matcher
from cutlass.aspera import aspera as asp

from . import ssh
from .serialize import indent
from .serialize import to_xml
from .util import reportnum
from .update import print_report

def fsize(fname):
    return os.stat(fname).st_size

def parse_fasp_url(u):
    parsed = urlparse(u)
    return parsed.netloc, parsed. path

identity = lambda x: x
def groupby(keyfunc=identity, seq=[]):
    grouped = defaultdict(list)
    for item in seq:
        grouped[keyfunc(item)].append(item)
    return grouped

def _sequences(sample_records):
    def _s():
        for sample in sample_records:
            for prep, seq in sample.prepseqs:
                yield seq

    grouped = groupby(lambda s: s.urls[0], _s())
    return [ grp[0] for grp in grouped.itervalues() ]


class Bag(object):
    pass

# def find_file(needle, haystack):
#     try:
#         it = (h for h in haystack if needle.split("_", 1)[0] in h)
#         return next(it)
#     except StopIteration:
#         return None

# def find_file(needle, haystack):
#     results = matcher.closest(needle, haystack, (2,3))
#     if len(results) > 1:
#         return None
#     return results[0][1]

_hash = lambda v: "{}{}".format(0 if v < 0 else 1, abs(hash(v)))

class MyDict(dict):
    id = None

def gen_samples_seqs(study, metadata, seqinfo, files):
    with open(metadata, 'r') as f:
        recs = json.load(f)
    with open(seqinfo, 'r') as f:
        seqinfo = json.load(f)
    samples_seqs = list()
    for rec in recs:
        seq = Bag()
        match = rec.get('filename', None)
        if not match:
            continue
        seq.path = os.path.abspath(match)
        seq.seq_model = seqinfo['seq_model']
        seq.lib_const = seqinfo['lib_const']
        seq.method = seqinfo['method']
        seq.id = study.id+":seq:"+_hash(basename(seq.path))
        sample = MyDict(rec)
        sample.id = study.id+":sample:"+_hash(rec['SampleID'])
        samples_seqs.append((sample, seq))
    return samples_seqs
        

def serialize(study_json, qiime_metadata, seqinfo_16s, files_16s,
              wgs_metadata, seqinfo_wgs, files_wgs, submission_fname,
              ready_fname, products_dir):

    def _write_xml():
        with open(study_json) as f:
            st = json.load(f)
        study = Bag()
        study.name = st['name']
        study.description = st['description']
        study.id = _hash(study.name)
        samples_seqs = list()
        if qiime_metadata:
            samples_seqs += gen_samples_seqs(study, qiime_metadata,
                                            seqinfo_16s, files_16s)
        if wgs_metadata:
            samples_seqs += gen_samples_seqs(study, wgs_metadata,
                                             seqinfo_wgs, files_wgs)
        xml = to_xml(study, samples_seqs)
        indent(xml)
        et = ET.ElementTree(xml)
        et.write(submission_fname)

    yield {
        "name": "serialize:xml: "+submission_fname,
        "actions": [_write_xml],
        "file_dep": filter(None, [qiime_metadata, wgs_metadata]),
        "targets": [submission_fname]
    }

    yield {
        "name": "serialize:ready_file: "+ready_fname,
        "actions": [lambda *a, **kw: open(ready_fname, 'w').close()],
        "file_dep": [],
        "targets": [ready_fname]
    }


def upload(files_16s, files_wgs, sub_fname, ready_fname, keyfile,
           remote_path, remote_srv, user, products_dir):
    """Upload raw sequence files and xml.

    :param keyfile: String; absolute filepath to private SSH keyfile for
    access to NCBI's submission server

    :param remote_path: String; the directory on the NCBI submission
    server where to upload data. If unset, the remote_path is
    automatically determined.

    :param remote_srv: String; TLD of NCBI's submission server

    :param user: String; username used to access NCBI's submission server

    """

    to_upload = [ f for f in list(files_16s)+list(files_wgs)
                  if not f.endswith(".complete") ]
    ssh_session = ssh.SSHConnection(user, remote_srv, keyfile, remote_path)
    uptodate = [ssh_session.uptodate]

    def _upload(local_fname, complete_fname, blithely=False):
        def _u():
            with open(sub_fname, 'r') as f:
                b = basename(local_fname)
                if b not in ("submission.xml", "submit.ready") \
                   and b not in f.read():
                    return
            ret = asp.upload_file(remote_srv, user, None, local_fname,
                                  remote_path, keyfile=keyfile)
            if blithely or ret:
                open(complete_fname, 'w').close()
            return blithely or ret # return True if blithely is True
        return _u

    complete_fnames = [f+".complete" for f in to_upload
                       if not f.endswith(".complete")]
    for f, complete_fname in zip(to_upload, complete_fnames):
        yield {
            "name": "upload: "+basename(f),
            "actions": [_upload(f, complete_fname)],
            "file_dep": [f],
            "uptodate": uptodate,
            "targets": [complete_fname]
        }

    yield {
        "name": "upload: "+basename(sub_fname),
        "actions": [_upload(sub_fname, sub_fname+".complete")],
        "file_dep": [],
        "targets": [sub_fname+".complete"]
    }

    yield {
        "name": "upload: "+basename(ready_fname),
        "actions": [_upload(ready_fname, ready_fname+".complete", True)],
        "file_dep": [],
        "targets": [ready_fname+".complete"]
    }


def report(ready_complete_fname, user, remote_srv, remote_path,
           keyfile):
    reports_dir = dirname(ready_complete_fname)
    def _download():
        c = ssh.SSHConnection(user, remote_srv, keyfile, remote_path)
        for _ in range(60*20*2): # 20 minutes in half-seconds
            report_fnames = [basename(n) for n in c.files()
                             if re.search(r'report\.[\d.]*xml', n)
                             and not exists(join(reports_dir, basename(n)))]
            if report_fnames:
                break
            else:
                time.sleep(.5)
        for n in report_fnames:
            if exists(join(reports_dir, n)):
                continue
            asp.download_file(remote_srv, user, None, join(remote_path, n),
                              reports_dir, keyfile=keyfile)
        if not report_fnames:
            print >> sys.stderr, "Timed out waiting for report xml files."
            return False
        most_recent_report = max(report_fnames, key=reportnum)
        print_report(join(reports_dir, most_recent_report))

    yield {
        "name": "report:get_reports",
        "actions": [_download],
        "file_dep": [ready_complete_fname],
        "uptodate": [False],
        "targets": [],
    }
    


