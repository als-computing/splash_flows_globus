from orchestration.flows.bl832.job_controller import TomographyHPCController


class OLCFTomographyHPCController(TomographyHPCController):
    """
    Implementation of TomographyHPCController for OLCF.

    Args:
        TomographyHPCController (ABC): Abstract class for tomography HPC controllers.
    """

    def __init__(self) -> None:
        pass

    def reconstruct(
        self,
        file_path: str = "",
    ) -> bool:
        # TODO: Implement tomography reconstruction at OLCF
        # https://docs.olcf.ornl.gov/ace_testbed/defiant_quick_start_guide.html#running-jobs
        pass

    def build_multi_resolution(
        self,
        file_path: str = "",
    ) -> bool:
        # TODO: Implement building multi-resolution datasets at OLCF
        pass
