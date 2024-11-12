from orchestration.flows.bl832.nersc import launch_nersc_jobs_tomo_recon

def test_launch_nersc_jobs_tomography():
    launch_nersc_jobs_tomo_recon()
    assert True

if __name__ == "__main__":
    test_launch_nersc_jobs_tomography()