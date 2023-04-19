import os
import numpy as np
import shutil
import sys
import h5py
import matplotlib.pyplot as plt

from jupyterlab_h5web import H5Web
from pyiron_base import Project, GenericJob, DataContainer, state, Executable, ImportAlarm
from paraprobe_base_job import ParaprobeBase, _pipe_output_to_file, _change_directory
import paraprobe_autoreporter.metadata.h5tessellator as nx

def get_cell_volume(results_file, dataset_id, tessellation_task_id=None):
    h5r = h5py.File(results_file, 'r')
    grpnm = nx.MYTESS + str(dataset_id) \
        + nx.MYTESS_DATA_VORO_TSKS + '/' + str(tessellation_task_id)
    dsnm = grpnm + '/' + nx.MYTESS_DATA_VORO_TSKS_CVOL
    dsnm_con = grpnm + '/' + nx.MYTESS_DATA_VORO_TSKS_WALLCONTACT
    V = np.sort(np.asarray(h5r[dsnm][:, 0], np.float32), kind='mergesort')
    Wall = np.asarray(h5r[dsnm_con][:, 0], np.uint8)
    n = np.shape(V)[0]
    cdf = np.asarray(np.linspace(1./n, 1., n, endpoint=True), np.float32)
    return [V], [cdf]

with ImportAlarm(
    "paraprobe functionality requires the `paraprobe` module (and its dependencies) specified as extra"
    "requirements. Please install it and try again."
) as paraprobe_alarm:
    from paraprobe_parmsetup.tessellator_guru import ParmsetupTessellator
    from paraprobe_autoreporter.wizard.tessellator_report import AutoReporterTessellator

class ParaprobeTessellator(ParaprobeBase):
    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self.ranger_job = None
        self.distancer_job = None
        self._tessellator_config = None
        self._skip_copy_results = False
        
    def _copy_results(self):
        if self._skip_copy_results:
            return

        if self.ranger_job is None:
            raise ValueError("Needs a ranger job!")
        if self.distancer_job is None:
            raise ValueError("Needs a distancer job!")
        
        self._copy_file(os.path.join(self.ranger_job.working_directory, self.ranger_job._transcoder_config))
        self._copy_file(os.path.join(self.ranger_job.working_directory, self.ranger_job._transcoder_results))
        self._copy_file(os.path.join(self.ranger_job.working_directory, self.ranger_job._ranger_config))
        self._copy_file(os.path.join(self.ranger_job.working_directory, self.ranger_job._ranger_results))
        self._copy_file(os.path.join(self.distancer_job.working_directory, self.distancer_job._distancer_results))
    
    @_change_directory
    @_pipe_output_to_file("config_tessellator.log")
    def _configure_tessellator(self):
        tessellator = ParmsetupTessellator()
        self._tessellator_config = tessellator.compute_complete_voronoi_tessellation(self.working_directory, 
                                    transcoder_config_sim_id=self.jobid,
                                    transcoder_results_sim_id=self.jobid,
                                    ranger_results_sim_id=self.jobid,
                                    distancer_results_sim_id=self.jobid,
                                    tessellator_results_sim_id=self.jobid)
        
    def _executable_activate(self, enforce = False):
        if self._executable is None or enforce:
            self._executable = Executable(
                codename='paraprobe-tessellator',
                module='paraprobe-tessellator',
                path_binary_codes=state.settings.resource_paths
            )
            
    def write_input(self):
        if ((self.pos_file is None) or (self.rrng_file is None)):
            raise ValueError("Set files")

        self.pos_file = self._copy_file(self.pos_file)
        self.rrng_file = self._copy_file(self.rrng_file)
        self._copy_results()
        self._configure_tessellator()
    
    def collect_output(self):
        self._collect_tessellator_results()
        self._collect_logs()
    
    @_pipe_output_to_file("result_tessellator.log")
    def _collect_tessellator_results(self):
        self._tessellator_results = os.path.join(self.working_directory, f"PARAPROBE.Tessellator.Results.SimID.{self.jobid}.h5")
        v, cdf = get_cell_volume(self._tessellator_results, self.jobid, tessellation_task_id=0)
        self.output.v = v[0]
        self.output.cdf = cdf[0]
        
    def _collect_logs(self):
        config_tessellator_log = self._read_temporary_output_file("config_tessellator.log", clean=False)
        execute_tessellator_log = self._read_temporary_output_file("log.out", clean=False)
        self.output["log/configure/tessellator"] = config_tessellator_log
        self.output["log/execute/tessellator"] = execute_tessellator_log
    
    def plot(self):
        plt.plot(self.output.v, self.output.cdf)
        plt.xscale('log')
        plt.xlabel(r'Cell volume $({nm}^3)$')
        plt.ylabel(r'Cumulated fraction');