import time

from globus_sdk import TransferData
from pytest import MonkeyPatch

from ..flows.bl832 import move
from orchestration.flows.bl832.move import process_new_832_file


class MockTransferClient:
    transfer_data: TransferData

    move_to_data832_was_called = False
    move_to_nersc_was_called = False

    def submit_transfer(self, transfer_data: TransferData):
        self.transfer_data = transfer_data
        return {"task_id": "12345"}

    def get_submission_id(self):
        return {"value": "42"}

    def task_wait(self, task_id, polling_interval=1, timeout=1):
        time.sleep(polling_interval)
        return True

    def get_task(self, task_id):
        return {"task_id": task_id, "status": "SUCCEEDED"}


# def test_parameters(monkeypatch):
#     import orchestration.flows.bl832.config
#     monkeypatch.setattr("orchestration.flows.bl832.config", MockTransferClient())
#     transfer_client = MockTransferClient()
#     process_new_832_file("foo/bar.h5")


#     assert result
