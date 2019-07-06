class Missing_Distribution_Exception(Exception):
    """Exception raised by Variable_Distributions if a Variable_Distribution
    hasn't been specified for a particular fitting variable"""

    pass


class Candidate_Length_Exception(Exception):
    """Exception raised by Variable_Distributions when a list provided
    for normalisation has the wrong number of entries"""

    pass
