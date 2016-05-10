from setuptools import setup, find_packages

setup(
    name='envi_sra',
    version='0.0.1',
    description=("Transfer sequence files and metadata from the iHMP DCC "
                 "to NCBI's SRA"),
    packages=["envi_sra"],
    zip_safe=False,
    install_requires=[
        'anadama',
        'osdf-python',
        'cutlass',
        'paramiko'
    ],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha"
    ],
    entry_points= {
        'anadama.pipeline': [
            ".envi_sra = envi_sra.pipeline:ENVISRAPipeline"
        ]
    }
)
