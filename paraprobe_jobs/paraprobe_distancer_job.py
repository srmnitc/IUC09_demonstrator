import os
import numpy as np
import shutil
import sys

from jupyterlab_h5web import H5Web
from pyiron_base import Project, GenericJob, DataContainer, state, Executable, ImportAlarm
from paraprobe_base_job import ParaprobeBase, _pipe_output_to_file, _change_directory

with ImportAlarm(
    "paraprobe functionality requires the `paraprobe` module (and its dependencies) specified as extra"
    "requirements. Please install it and try again."
) as paraprobe_alarm:
    from paraprobe_parmsetup.distancer_guru import ParmsetupDistancer
    from paraprobe_autoreporter.wizard.distancer_report import AutoReporterDistancer

class ParaprobeDistancer(ParaprobeBase):
    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self.ranger_job = None
        self.surfacer_job = None
        self._distancer_config = None
        self._skip_copy_results = False
        
    def _copy_results(self):
        if self._skip_copy_results:
            return

        if self.ranger_job is None:
            raise ValueError("Needs a ranger job!")
        
        a = self._copy_file(os.path.join(self.ranger_job.working_directory, self.ranger_job._transcoder_config))
        self._copy_file(os.path.join(self.ranger_job.working_directory, self.ranger_job._transcoder_results))
        self._copy_file(os.path.join(self.ranger_job.working_directory, self.ranger_job._ranger_config))
        self._copy_file(os.path.join(self.ranger_job.working_directory, self.ranger_job._ranger_results))
        self._copy_file(os.path.join(self.surfacer_job.working_directory, self.surfacer_job._surfacer_config))
        self._copy_file(os.path.join(self.surfacer_job.working_directory, self.surfacer_job._surfacer_results))
        
    
    @_change_directory
    @_pipe_output_to_file("config_distancer.log")
    def _configure_distancer(self):
        distancer = ParmsetupDistancer()
        self._distancer_config = distancer.compute_ion_to_edge_model_distances(self.working_directory, 
                                    transcoder_config_sim_id=self.jobid,
                                    transcoder_results_sim_id=self.jobid,
                                    ranger_results_sim_id=self.jobid,
                                    distancer_results_sim_id=self.jobid)
        
    def _executable_activate(self, enforce = False):
        if self._executable is None or enforce:
            self._executable = Executable(
                codename='paraprobe-distancer',
                module='paraprobe-distancer',
                path_binary_codes=state.settings.resource_paths
            )
            
    def write_input(self):
        if ((self.pos_file is None) or (self.rrng_file is None)):
            raise ValueError("Set files")

        self.pos_file = self._copy_file(self.pos_file)
        self.rrng_file = self._copy_file(self.rrng_file)
        self._copy_results()
        self._configure_distancer()
    
    def collect_output(self):
        self._collect_distancer_results()
        self._collect_logs()
    
    def _collect_distancer_results(self):
        self._distancer_results = os.path.join(self.working_directory, f"PARAPROBE.Distancer.Results.SimID.{self.jobid}.h5")
        #distancer_report = AutoReporterDistancer(self._distancer_results, self.jobid)
        #distancer_plot = distancer_report.get_ion2mesh_distance_cdf(distancing_task_id=0)
        
        
    def _collect_logs(self):
        config_distancer_log = self._read_temporary_output_file("config_distancer.log", clean=False)
        execute_distancer_log = self._read_temporary_output_file("log.out", clean=False)
        self.output["log/configure/distancer"] = config_distancer_log
        self.output["log/execute/distancer"] = execute_distancer_log