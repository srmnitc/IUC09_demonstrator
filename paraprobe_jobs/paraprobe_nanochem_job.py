import os
import numpy as np
import shutil
import sys
import h5py
import matplotlib.pyplot as plt

from jupyterlab_h5web import H5Web
from pyiron_base import Project, GenericJob, DataContainer, state, Executable, ImportAlarm
from paraprobe_base_job import ParaprobeBase, _pipe_output_to_file, _change_directory

with ImportAlarm(
    "paraprobe functionality requires the `paraprobe` module (and its dependencies) specified as extra"
    "requirements. Please install it and try again."
) as paraprobe_alarm:
    from paraprobe_parmsetup.nanochem_guru import ParmsetupNanochem, NanochemTask, Delocalization
    from paraprobe_autoreporter.wizard.nanochem_report import AutoReporterNanochem
    from paraprobe_parmsetup.utils.numerics import EPSILON
    
class ParaprobeNanochem(ParaprobeBase):
    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self.surfacer_job = None
        self.distancer_job = None
        self.ranger_job = None
        self._nanochem_config = None
        self._skip_copy_results = False
        
    def _copy_results(self):
        if self._skip_copy_results:
            return

        if self.ranger_job is None:
            raise ValueError("Needs a ranger job!")
        if self.surfacer_job is None:
            raise ValueError("Needs a surfacer job!")
        if self.distancer_job is None:
            raise ValueError("Needs a distancer job!")
        
        self._copy_file(os.path.join(self.ranger_job.working_directory, self.ranger_job._transcoder_config))
        self._copy_file(os.path.join(self.ranger_job.working_directory, self.ranger_job._transcoder_results))
        self._copy_file(os.path.join(self.ranger_job.working_directory, self.ranger_job._ranger_config))
        self._copy_file(os.path.join(self.ranger_job.working_directory, self.ranger_job._ranger_results))
        
        self._copy_file(os.path.join(self.surfacer_job.working_directory, self.surfacer_job._surfacer_results))
        self._copy_file(os.path.join(self.distancer_job.working_directory, self.distancer_job._distancer_results))
    
    @_change_directory
    @_pipe_output_to_file("config_nanochem.log")
    def _configure_nanochem(self):
        nanochem = ParmsetupNanochem()
        dataset = NanochemTask()
        dataset.load_reconstruction_and_ranging(
            ranging_applied=True,
            working_directory=self.working_directory,
            transcoder_config_sim_id=self.jobid,
            transcoder_results_sim_id=self.jobid,
            ranger_results_sim_id=self.jobid)
        dataset.load_edge_model(
                file_name=os.path.join(self.working_directory, 'PARAPROBE.Surfacer.Results.SimID.636502001.h5'),
                dataset_name_vertices='/entry/process0/point_set_wrapping0/alpha_complex/triangle_set/triangles/vertices',
                dataset_name_facet_indices='/entry/process0/point_set_wrapping0/alpha_complex/triangle_set/triangles/faces')    
        dataset.load_ion_to_edge_distances(
                file_name=os.path.join(self.working_directory, 'PARAPROBE.Distancer.Results.SimID.636502001.h5'),
                dataset_name='/entry/process0/point_to_triangle_set/distance')        
        
        task = Delocalization()
        task.set_delocalization_input(source='default')
        task.set_delocalization_normalization(method='composition')
        task.set_delocalization_elements(['Y', 'Ti', 'O'])
        task.set_delocalization_gridresolutions(length=[1.])
        task.set_delocalization_kernel(sigma=[1.0], size=3)
        task.set_delocalization_isosurfaces(phi=np.linspace(start=0.01, stop=0.21, num=21, endpoint=True))
        task.set_delocalization_edge_handling(method='default')
        task.set_delocalization_edge_threshold(EPSILON)
        task.report_fields_and_gradients(True)
        task.report_triangle_soup(True)
        task.report_objects(True)
        task.report_objects_properties(True)
        task.report_objects_geometry(True)
        task.report_objects_optimal_bounding_box(True)
        task.report_objects_ions(True)
        task.report_objects_edge_contact(True)
        task.report_proxies(False)
        task.report_proxies_properties(False)
        task.report_proxies_geometry(False)
        task.report_proxies_optimal_bounding_box(False)
        task.report_proxies_ions(False)

        nanochem.add_task(dataset, task)
        self._nanochem_config = nanochem.configure(self.jobid)

    def _executable_activate(self, enforce = False):
        if self._executable is None or enforce:
            self._executable = Executable(
                codename='paraprobe-nanochem',
                module='paraprobe-nanochem',
                path_binary_codes=state.settings.resource_paths
            )
            
    def write_input(self):
        if ((self.pos_file is None) or (self.rrng_file is None)):
            raise ValueError("Set files")

        self.pos_file = self._copy_file(self.pos_file)
        self.rrng_file = self._copy_file(self.rrng_file)
        self._copy_results()
        self._configure_nanochem()

    def collect_output(self):
        self._collect_nanochem_results()
        self._collect_logs()
        
    @_pipe_output_to_file("result_nanochem.log")
    def _collect_nanochem_results(self):
        self._nanochem_results = os.path.join(self.working_directory, f"PARAPROBE.Nanochem.Results.SimID.{self.jobid}.h5")
        nanochem_report = AutoReporterNanochem(self._nanochem_results, dataset_id=0)
        nanochem_report.get_delocalization(delocalization_task_id=0)
        nanochem_report.get_isosurface_objects_volume_and_number_over_isovalue(delocalization_task_id=0)

    def _collect_logs(self):
        config_nanochem_log = self._read_temporary_output_file("config_nanochem.log", clean=False)
        execute_nanochem_log = self._read_temporary_output_file("log.out", clean=False)
        self.output["log/configure/nanochem"] = config_nanochem_log
        self.output["log/execute/nanochem"] = execute_nanochem_log