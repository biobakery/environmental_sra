from collections import namedtuple

class settings:
    keyfile = "/home/rschwager/test_data/broad_metadata/dcc_sra/iHMP_SRA_key"
    remote_path = "/submit/Test/"
    remote_srv = "upload.ncbi.nlm.nih.gov"
    user = "asp-hmp2"

SubmitRecord = namedtuple("SubmitRecord", "sample prepseqs")
PrepSeq = namedtuple("PrepSeq", "prep seq")

from .pipeline import DCCSRAPipeline
