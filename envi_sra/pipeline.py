import os

import anadama.pipelines

from . import workflows


class ENVISRAPipeline(anadama.pipelines.Pipeline):
    """Pipeline for submitting metadata to NCBI's SRA.

    Steps:

    1. Query local metadata for all samples, preps, and raw sequence
       sets for a given study

    2. For each raw sequence, download the raw sequence file if it's
    not available locally

    3. Serialize all metadata useful for SRA from OSDF into a
    submission.xml file

    4. Create an empty submit.empty file.

    5. Upload raw sequence files to SRA as necessary

    6. Upload submission.xml and submit.ready file

    Workflows used:

    * :py:func:`envi_sra.workflows.serialize`
    * :py:func:`envi_sra.workflows.upload`
    * :py:func:`envi_sra.workflows.report`

    """


    name = "ENVISRA"
    products = {
        "input_16s_files": list(),
        "input_wgs_files": list()
    }

    default_options = {
        "serialize": {
            "study_json": None,
            "qiime_metadata": None,
            "seqinfo_16s": None,
            "wgs_metadata": None,
            "seqinfo_wgs": None,
        },
        "upload": {
            "keyfile": "/home/rschwager/test_data/broad_metadata/dcc_sra/iHMP_SRA_key",
            "remote_path": "/submit/Test/",
            "remote_srv" : "upload.ncbi.nlm.nih.gov",
            "user": "asp-hmp2",
        },
        "report": {
            "products_dir": "reports"
        }
    }

    workflows = {
        "serialize": workflows.serialize,
        "upload": workflows.upload,
        "report": workflows.report
    }

    def __init__(self, input_16s_files=list(),
                 input_wgs_files=list(),
                 sample_metadata=list(),
                 products_dir=str(),
                 workflow_options=dict(),
                 *args, **kwargs):

        """Initialize the pipeline.

        :keyword input_16s_files: List of strings; raw 16S sequence
        files (fasta or fastq format) already downloaded.

        :keyword input_16s_files: List of strings;raw WGS sequence
        files (fasta or fastq format) already downloaded.

        :keyword products_dir: String; Directory path for where outputs will 
                               be saved.

        :keyword workflow_options: Dictionary; **opts to be fed into the 
                                   respective workflow functions.

        """

        super(ENVISRAPipeline, self).__init__(*args, **kwargs)

        self.options = self.default_options.copy()
        for k in self.options.iterkeys():
            self.options[k].update(workflow_options.get(k,{}))

        if not products_dir:
            products_dir = self.options['report']['products_dir']
        self.products_dir = os.path.abspath(products_dir)
        if not os.path.isdir(self.products_dir):
            os.mkdir(self.products_dir)

        for k in self.options['serialize']:
            if not self.options['serialize'].get(k, None):
                prompt = "Enter the path to the {}: ".format(k)
                self.options['serialize'][k] = raw_input(prompt)

        if not self.options['upload'].get('remote_path', None):
            prompt = "Enter the study name to submit: "
            self.options['upload']['remote_path'] = raw_input(prompt)

        if not self.options['upload']['remote_path'].endswith('/'):
            self.options['upload']['remote_path'] += '/'

        if not self.options['upload']['remote_path'].startswith('/submit/'):
            p = self.options['upload']['remote_path']
            self.options['upload']['remote_path'] = '/submit/' + p

        self.add_products(
            input_wgs_files = input_wgs_files,
            input_16s_files = input_16s_files,
        )


    def _configure(self):
        submission_file = os.path.join(self.products_dir, "submission.xml")
        ready_file = os.path.join(self.products_dir, "submit.ready")
        opts = self.options['serialize']
        yield workflows.serialize(opts.pop('study_json'),
                                  opts.pop('qiime_metadata'),
                                  opts.pop('seqinfo_16s'),
                                  self.input_16s_files,
                                  opts.pop('wgs_metadata'),
                                  opts.pop('seqinfo_wgs'),
                                  self.input_wgs_files,
                                  submission_file,
                                  ready_file,
                                  self.products_dir,
                                  **self.options['serialize'])

        yield workflows.upload(self.input_16s_files,
                               self.input_wgs_files,
                               submission_file, ready_file,
                               products_dir=self.products_dir,
                               **self.options['upload'])

        yield workflows.report(ready_file+".complete",
                               **self.options['upload'])
