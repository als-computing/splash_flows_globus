import h5py
import json
import numpy as np
import pytest

from orchestration.flows.scicat.utils import NPArrayEncoder

from orchestration.flows.scicat.utils import (
    build_search_terms,
    calculate_access_controls,
)


@pytest.fixture
def sample_file(tmp_path):
    # data = np.empty((num_frames_primary, 5, 5))
    file = h5py.File(tmp_path / "test.hdf5", "w")
    file.create_dataset("/measurement/sample/name", data=b"my sample", dtype="|S256")
    yield file
    print("closing file")
    file.close()


def add_mock_requests(mock_request):
    mock_request.post("http://localhost:3000/api/v3/Users/login", json={"id": "foobar"})
    mock_request.post(
        "http://localhost:3000/api/v3/Samples", json={"sampleId": "dataset_id"}
    )
    mock_request.post(
        "http://localhost:3000/api/v3/RawDatasets/replaceOrCreate", json={"pid": "42"}
    )
    mock_request.post(
        "http://localhost:3000/api/v3/RawDatasets/42/origdatablocks",
        json={"response": "random"},
    )


def test_np_encoder():
    test_dict = {"dont_panic": np.array([1, 2, 3], dtype=np.int8)}
    assert json.dumps(test_dict, cls=NPArrayEncoder) 

 
    test_dict = {"dont_panic": np.array([1, 2, 3], dtype=np.float32)}
    assert json.dumps(test_dict, cls=NPArrayEncoder)

    test_dict = {"dont_panic": np.full((1, 1), np.inf)}
    
    # requests doesn't allow strings  that have np.inf or np.nan
    # so the NPArrayEncoder needs to return both as None
    encoded_np = json.loads(
            json.dumps(test_dict, cls=NPArrayEncoder)
    )
    assert json.dumps(encoded_np, allow_nan=False)
 

def test_build_search_terms():
    terms = build_search_terms("Time-is_an illusion. Lunchtime/2x\\so.")
    assert "time" in terms
    assert "is" in terms
    assert "an" in terms
    assert "illusion" in terms
    assert "lunchtime" in terms
    assert "2x" in terms
    assert "so" in terms


def test_access_controls():
    # no propsal, no beamline
    username = "slartibartfast"
    access_controls = calculate_access_controls(username, None, None)
    assert access_controls["owner_group"] == "slartibartfast"
    assert access_controls["access_groups"] == []

    # propoosal and no beamline
    access_controls = calculate_access_controls(username, None, "42")
    assert access_controls["owner_group"] == "42"

    # no propoosal and beamline
    access_controls = calculate_access_controls(username, "10.3.1", None)
    assert access_controls["owner_group"] == "slartibartfast"
    assert "10.3.1" in access_controls["access_groups"]
    assert "slartibartfast" in access_controls["access_groups"]

    # proposal and beamline
    access_controls = calculate_access_controls(username, "10.3.1", "42")
    assert access_controls["owner_group"] == "42"
    assert "10.3.1" in access_controls["access_groups"]

    # special 8.3.2 mapping
    access_controls = calculate_access_controls(username, "bl832", "42")
    assert access_controls["owner_group"] == "42"
    assert "8.3.2" in access_controls["access_groups"]
    assert "bl832" in access_controls["access_groups"]