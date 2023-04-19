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
    from paraprobe_parmsetup.surfacer_guru import ParmsetupSurfacer
    

class ParaprobeSurfacer(ParaprobeBase):
    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self.ranger_job = None
        self._surfacer_config = None
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
    
    @_change_directory
    @_pipe_output_to_file("config_surfacer.log")
    def _configure_surfacer(self):
        surfacer = ParmsetupSurfacer()
        self._surfacer_config = surfacer.compute_convex_hull_edge_model(self.working_directory, 
                                    transcoder_config_sim_id=self.jobid,
                                    transcoder_results_sim_id=self.jobid,
                                    ranger_results_sim_id=self.jobid,
                                    surfacer_results_sim_id=self.jobid)
        
    def _executable_activate(self, enforce = False):
        if self._executable is None or enforce:
            self._executable = Executable(
                codename='paraprobe-surfacer',
                module='paraprobe-surfacer',
                path_binary_codes=state.settings.resource_paths
            )
            
    def write_input(self):
        if ((self.pos_file is None) or (self.rrng_file is None)):
            raise ValueError("Set files")

        self.pos_file = self._copy_file(self.pos_file)
        self.rrng_file = self._copy_file(self.rrng_file)
        self._copy_results()
        self._configure_surfacer()
    
    def collect_output(self):
        self._collect_surfacer_results()
        self._collect_logs()
    
    def _collect_surfacer_results(self):
        self._surfacer_results = os.path.join(self.working_directory, f"PARAPROBE.Surfacer.Results.SimID.{self.jobid}.h5")
        
    def _collect_logs(self):
        config_surfacer_log = self._read_temporary_output_file("config_surfacer.log", clean=False)
        execute_surfacer_log = self._read_temporary_output_file("log.out", clean=False)
        self.output["log/configure/surfacer"] = config_surfacer_log
        self.output["log/execute/surfacer"] = execute_surfacer_log