import os
import numpy as np
import shutil
import sys

from jupyterlab_h5web import H5Web
from pyiron_base import Project, GenericJob, DataContainer, state, Executable, ImportAlarm

def _pipe_output_to_file(filename):
    def _wrapper(method):
        """
        This is a temporary fix until paraprobe input outputs etc can be fixed
        """
        def change_stdout(self):
            orig_stdout = sys.stdout
            outfile = os.path.join(self.working_directory, filename)
            f = open(outfile, 'w')
            sys.stdout = f
            method(self)
            sys.stdout = orig_stdout
            f.close()
        return change_stdout
    return _wrapper

def _change_directory(method):
    """
    Temporarily switch directory
    """
    def change_dir(self):
        os.chdir(self.working_directory)
        method(self)
        os.chdir(self._current_dir)            
    return change_dir

    
class ParaprobeBase(GenericJob):
    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self.input = DataContainer(table_name="input")
        self.output = DataContainer(table_name="output")
        self.input.input_path = None
        #self.executable = f"mpiexec -n $1 paraprobe_ranger 636502001 {self.working_directory}/PARAPROBE.Ranger.Config.SimID.636502001.nxs;"
        self._executable = None
        self._executable_activate()
        state.publications.add(self.publication)
        self.jobid = 636502001
        self._pos_file = None
        self._rrng_file = None
        self._current_dir = os.getcwd()

    def _copy_file(self, filename):
        if os.path.exists(filename):
            shutil.copy(filename, self.working_directory)
            return os.path.basename(filename)
        else:
            raise FileNotFoundError(f"file {filename} not found")
            
    def _read_temporary_output_file(self, filename, clean=True):
        outfile = os.path.join(self.working_directory, filename)
        if clean:
            lines = []
            with open(outfile, "r") as fin:
                for line in fin:
                    line = line.strip().split()
                    lines.append(line)
        else:
            with open(outfile, "r") as fin:
                lines = fin.read()
        return lines

    @property
    def pos_file(self):
        return self._pos_file

    @pos_file.setter
    def pos_file(self, filename):
        self._pos_file = filename

    @property
    def rrng_file(self):
        return self._rrng_file
    
    @rrng_file.setter
    def rrng_file(self, filename):
        self._rrng_file = filename
        
    def to_hdf(self, hdf=None, group_name=None): 
        super().to_hdf(
            hdf=hdf,
            group_name=group_name
        )
        with self.project_hdf5.open("input") as h5in:
            self.input.to_hdf(h5in)

    def from_hdf(self, hdf=None, group_name=None): 
        super().from_hdf(
            hdf=hdf,
            group_name=group_name
        )
        with self.project_hdf5.open("input") as h5in:
            self.input.from_hdf(h5in)

    @property
    def publication(self):
        return {
            "paraprobe": [
                {
                    "title": "On Strong-Scaling and Open-Source Tools for High-Throughput Quantification of Material Point Cloud Data: Composition Gradients, Microstructural Object Reconstruction, and Spatial Correlations",
                    "journal": "arxiv",
                    "volume": "1",
                    "number": "1",
                    "year": "2022",
                    "doi": "10.48550/arXiv.2205.13510",
                    "url": "https://doi.org/10.48550/arXiv.2205.13510",
                    "author": ["M. Kühbach", "V. V. Rielli", 
                               "S. Primig", "A. Saxena", "D. Mayweg",
                               "B. Jenkins", "S. Antonov", "A. Reichmann",
                               "S. Kardos", "L. Romaner", "S. Brockhauser"],
                },
                {
                    "title": "On Strong Scaling Open Source Tools for Mining Atom Probe Tomography Data",
                    "journal": "Microscopy and Microanalysis",
                    "volume": "25",
                    "number": "1",
                    "year": "2019",
                    "doi": "10.1017/S1431927619002228",
                    "url": "https://doi.org/10.1017/S1431927619002228",
                    "author": ["M. Kühbach", "P. Bajaj", 
                               "A. Breen", "E. A. Jägle", "B. Gault"],
                }, 
                {
                    "title": "On strong-scaling and open-source tools for analyzing atom probe tomography data",
                    "journal": "npj Computational Materials",
                    "volume": "7",
                    "number": "21",
                    "year": "2021",
                    "doi": "10.1038/s41524-020-00486-1",
                    "url": "https://doi.org/10.1038/s41524-020-00486-1",
                    "author": ["M. Kühbach", "P. Bajaj", 
                               "H. Zhao", "M. H. Çelik", "E. A. Jägle",
                               "B. Gault"],
                }, 
                {
                    "title": "Community-Driven Methods for Open and Reproducible Software Tools for Analyzing Datasets from Atom Probe Microscopy",
                    "journal": "Microscopy and Microanalysis",
                    "volume": "1",
                    "number": "1",
                    "year": "2021",
                    "doi": "10.1017/S1431927621012241",
                    "url": "https://doi.org/10.1017/S1431927621012241",
                    "author": ["M. Kühbach", "A. J. London", 
                               "J. Wang", "D. K. Schreiber", "F. Mendez-Martin",
                               "I. Ghamarian", "H. Bilal", "A. V.Ceguerra"],
                }, 
                {
                    "title": "Open and strong-scaling tools for atom-probe crystallography: high-throughput methods for indexing crystal structure and orientation",
                    "journal": "Journal of Applied Crystallography",
                    "volume": "52",
                    "number": "1",
                    "year": "2021",
                    "doi": "10.1107/S1600576721008578",
                    "url": "https://doi.org/10.1107/S1600576721008578",
                    "author": ["M. Kühbach", "M. Kasemer", 
                               "A. Breen", "B. Gault"],
                }, 
            ]
        }