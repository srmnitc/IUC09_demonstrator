import os
import numpy as np
import shutil
import sys

from jupyterlab_h5web import H5Web
from pyiron_base import Project, GenericJob, DataContainer, state, Executable, ImportAlarm
from paraprobe_base_job import ParaprobeBase, _pipe_output_to_file, _change_directory
from paraprobe_ranger_job import ParaprobeRanger
from paraprobe_surfacer_job import ParaprobeSurfacer
from paraprobe_distancer_job import ParaprobeDistancer
from paraprobe_tessellator_job import ParaprobeTessellator
from paraprobe_nanochem_job import ParaprobeNanochem


class ParaprobeJob(ParaprobeBase):
    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self._analyse_ranger = False
        self._analyse_surfacer = False
        self._analyse_distancer = False
        self._analyse_tessellator = False
        self._analyse_nanochem = False
        self._ranger_job = None
        self._surfacer_job = None
        self._distancer_job = None
        self._tessellator_job = None
        self._nanochem_job = None
    
    def analyse_ranger(self):
        self._analyse_ranger = True

    def analyse_surfacer(self):
        self._analyse_ranger = True
        self._analyse_surfacer = True
        
    def analyse_distancer(self):
        self._analyse_ranger = True
        self._analyse_surfacer = True
        self._analyse_distancer = True
        
    def analyse_tessellator(self):
        self._analyse_ranger = True
        self._analyse_distancer = True
        self._analyse_tessellator = True

    def analyse_nanochem(self):
        self._analyse_ranger = True
        self._analyse_surfacer = True
        self._analyse_distancer = True
        self._analyse_nanochem = True
    
    
    def run_static(self):
        self.status.running = True
        if self._analyse_ranger:
            self._ranger_job = self.project.create_job(job_type=ParaprobeRanger, 
                                                       job_name=f'{self.name}_ranger', 
                    delete_existing_job=True)
            self._ranger_job.pos_file = self.pos_file
            self._ranger_job.rrng_file = self.rrng_file
            self._ranger_job.run()
            
        if self._analyse_surfacer:
            self._surfacer_job = self.project.create_job(job_type=ParaprobeSurfacer, 
                                                       job_name=f'{self.name}_surfacer', 
                    delete_existing_job=True)
            self._surfacer_job.pos_file = self.pos_file
            self._surfacer_job.rrng_file = self.rrng_file
            self._surfacer_job.ranger_job = self._ranger_job
            self._surfacer_job.run()

        if self._analyse_distancer:
            self._distancer_job = self.project.create_job(job_type=ParaprobeDistancer, 
                                                       job_name=f'{self.name}_distancer', 
                    delete_existing_job=True)
            self._distancer_job.pos_file = self.pos_file
            self._distancer_job.rrng_file = self.rrng_file
            self._distancer_job.ranger_job = self._ranger_job
            self._distancer_job.surfacer_job = self._surfacer_job
            self._distancer_job.run()

        if self._analyse_tessellator:
            self._tessellator_job = self.project.create_job(job_type=ParaprobeTessellator, 
                                                       job_name=f'{self.name}_tessellator', 
                    delete_existing_job=True)
            self._tessellator_job.pos_file = self.pos_file
            self._tessellator_job.rrng_file = self.rrng_file
            self._tessellator_job.ranger_job = self._ranger_job
            self._tessellator_job.surfacer_job = self._surfacer_job
            self._tessellator_job.run()

        if self._analyse_nanochem:
            self._nanochem_job = self.project.create_job(job_type=ParaprobeNanochem, 
                                                       job_name=f'{self.name}_nanochem', 
                    delete_existing_job=True)
            self._nanochem_job.pos_file = self.pos_file
            self._nanochem_job.rrng_file = self.rrng_file
            self._nanochem_job.ranger_job = self._ranger_job
            self._nanochem_job.surfacer_job = self._surfacer_job
            self._nanochem_job.distancer_job = self._distancer_job
            self._nanochem_job.run()
            
        self.status.collect = True
        self.collect_output()
    
    def plot_tessellator_results(self):
        if self._tessellator_job is not None:
            self._tessellator_job.plot()
            
    def _collect_logs(self):
        if self._analyse_ranger:
            self.output["log/configure/transcoder"] = self._ranger_job.output["log/configure/transcoder"]
            self.output["log/execute/transcoder"] = self._ranger_job.output["log/execute/transcoder"]
            self.output["log/configure/ranger"] = self._ranger_job.output["log/configure/ranger"]
            self.output["log/execute/ranger"] = self._ranger_job.output["log/execute/ranger"]
        
        if self._analyse_distancer:
            self.output["log/configure/distancer"] = self._distancer_job.output["log/configure/distancer"]
            self.output["log/execute/distancer"] = self._distancer_job.output["log/execute/distancer"]
        
        if self._analyse_surfacer:
            self.output["log/configure/surfacer"] = self._surfacer_job.output["log/configure/surfacer"]
            self.output["log/execute/surfacer"] = self._surfacer_job.output["log/execute/surfacer"]

        if self._analyse_tessellator:
            self.output["log/configure/tessellator"] = self._tessellator_job.output["log/configure/tessellator"]
            self.output["log/execute/tessellator"] = self._tessellator_job.output["log/execute/tessellator"]
        
        if self._analyse_nanochem:
            self.output["log/configure/nanochem"] = self._nanochem_job.output["log/configure/nanochem"]
            self.output["log/execute/nanochem"] = self._nanochem_job.output["log/execute/nanochem"]     
    
    def _collect_results(self):
        if self._analyse_ranger:
            self.output["ranger"] = self._ranger_job.output["ranger"]
    
    def collect_output(self):
        self._collect_logs()
        self._collect_results()
            
            
    
    
    
    
    
    
    
        