"""Module containing JobTask classes. These are used to create additional for runner jobs"""

import logging
import os
from typing import List, Tuple

from atsim.pro_fit.exceptions import ConfigException
from atsim.pro_fit.jobfactories import Job
from atsim.potentials.config import Configuration


class PotableJobTask:
    """JobTask that uses atsim.potentials to create potential tabulations"""

    def __init__(self, name: str, input_filename: str, output_filename: str):
        """Create JobTask for generating potential tabulation files.

        Args:
            name (str): Name of task.
            input_filename (str): Name of potable input file.
            output_filename (str): File in which tabulation should be created (should be a relative path).
        """

        self.name = name
        self.input_filename = input_filename
        self.output_filename = output_filename

    def beforeRun(self, job: Job):
        logger = logging.getLogger(__name__).getChild(
            "PotableJobTask.beforeRun")

        input_filename = self.input_filename
        if not os.path.isabs(input_filename):
            input_filename = os.path.abspath(
                os.path.join(job.path, "job_files", self.input_filename))
        output_filename = os.path.abspath(
            os.path.join(job.path, "job_files", self.output_filename))

        logger.debug("Executing potable for job '%s', using input file: '%s', tabulating into '%s'",
                     job.name, input_filename, output_filename)
        asp_cfg = Configuration()
        with open(input_filename) as infile:
            tabulation = asp_cfg.read(infile)

        with open(output_filename, 'w') as outfile:
            tabulation.write(outfile)

    def afterRun(self, job: Job):
        pass

    @classmethod
    def _is_relative(cls, job_path: str, filename: str) -> bool:
        jp = os.path.abspath(job_path)
        p = os.path.join(jp, filename)
        p = os.path.normpath(p)
        cp = os.path.commonprefix([p, jp])
        return cp == jp

    @classmethod
    def createFromConfig(cls, name: str, job_path: str, cfg_items: List[Tuple[str, str]]):
        cfgdict = dict(cfg_items)

        del cfgdict["type"]

        required_keys = set(["input_filename", "output_filename"])
        actual_keys = set(cfgdict.keys())

        for k in (actual_keys - required_keys):
            raise ConfigException(
                "Unknown configuration directive: {} found for Potable job task".format(k))

        if "input_filename" not in cfgdict:
            raise ConfigException(
                "Reqired configuration itme 'input_filename' not found for Potable job task")
        if "output_filename" not in cfgdict:
            raise ConfigException(
                "Reqired configuration itme 'output_filename' not found for Potable job task")

        input_filename = cfgdict['input_filename']
        output_filename = cfgdict['output_filename']

        if not cls._is_relative(job_path, output_filename):
            raise ConfigException(
                "Potable job task configuration item 'output_filename' should be a relative path: '{}'".format(output_filename))

        job_task = cls(name, input_filename, output_filename)
        return job_task
