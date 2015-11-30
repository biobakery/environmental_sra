from collections import namedtuple

SubmitRecord = namedtuple("SubmitRecord", "sample prepseqs")
PrepSeq = namedtuple("PrepSeq", "prep seq")

from .pipeline import DCCSRAPipeline
