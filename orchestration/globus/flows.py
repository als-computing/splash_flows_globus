from dotenv import load_dotenv
import os
import globus_sdk
from globus_sdk import (
    ClientCredentialsAuthorizer,
    ConfidentialAppAuthClient,
    SpecificFlowClient
)
from globus_sdk.scopes import TransferScopes, GCSCollectionScopeBuilder, MutableScope
from globus_sdk.tokenstorage import SimpleJSONFileAdapter
from pprint import pprint
from prefect.blocks.system import Secret

MY_FILE_ADAPTER = SimpleJSONFileAdapter(os.path.expanduser("~/.sdk-manage-flow.json"))

TRANSFER_ACTION_PROVIDER_SCOPE_STRING = (
    "https://auth.globus.org/scopes/actions.globus.org/transfer/transfer"
)

dotenv_file = load_dotenv()

# GLOBUS_CLIENT_ID = os.getenv("GLOBUS_CLIENT_ID")
# GLOBUS_CLIENT_SECRET = os.getenv("GLOBUS_CLIENT_SECRET")

GLOBUS_CLIENT_ID = Secret.load("globus-client-id")
GLOBUS_CLIENT_SECRET = Secret.load("globus-client-secret")


def do_login_flow(scopes, native_client):
    native_client.oauth2_start_flow(requested_scopes=scopes,
                                    refresh_tokens=True)
    authorize_url = native_client.oauth2_get_authorize_url()
    print(f"Please go to this URL and login:\n\n{authorize_url}\n")
    auth_code = input("Please enter the code here: ").strip()
    tokens = native_client.oauth2_exchange_code_for_tokens(auth_code)
    return tokens


def get_manage_flow_authorizer(client_id):
    # native_client = globus_sdk.NativeAppAuthClient(client_id)
    confidential_client = globus_sdk.ConfidentialAppAuthClient(
        client_id=GLOBUS_CLIENT_ID.get(), client_secret=GLOBUS_CLIENT_SECRET.get()
    )
    # resource_server = globus_sdk.FlowsClient.resource_server
    all_scopes = [
        globus_sdk.FlowsClient.scopes.manage_flows,
        globus_sdk.FlowsClient.scopes.run_status,
    ]
    return globus_sdk.ClientCredentialsAuthorizer(
        confidential_client,
        all_scopes)


def get_flows_client():
    # native_client = globus_sdk.NativeAppAuthClient(client_id)
    confidential_client = globus_sdk.ConfidentialAppAuthClient(
        client_id=GLOBUS_CLIENT_ID.get(), client_secret=GLOBUS_CLIENT_SECRET.get()
    )

    # resource_server = globus_sdk.FlowsClient.resource_server
    all_scopes = [
        globus_sdk.FlowsClient.scopes.manage_flows,
        globus_sdk.FlowsClient.scopes.run_status,
    ]
    authorizer = globus_sdk.ClientCredentialsAuthorizer(
        confidential_client,
        all_scopes)
    return globus_sdk.FlowsClient(authorizer=authorizer)


def get_specific_flow_client(flow_id, collection_ids=None):
    confidential_client = ConfidentialAppAuthClient(
        client_id=GLOBUS_CLIENT_ID.get(), client_secret=GLOBUS_CLIENT_SECRET.get()
    )

    assert collection_ids, "Why don't we have a collection id??"

    # Request token for Globus Flows scopes
    flow_scopes = [
        globus_sdk.FlowsClient.scopes.manage_flows,
        globus_sdk.FlowsClient.scopes.run_status,
        globus_sdk.SpecificFlowClient(flow_id).scopes]
    flow_scopes = flow_scopes[2].make_mutable("user")

    flows_authorizer = ClientCredentialsAuthorizer(confidential_client, flow_scopes)
    # could monkeypatch the init function for ClientCredentialsAuthorizer with my own function
    flow_client = SpecificFlowClient(flow_id, authorizer=flows_authorizer)

    # Request token for Transfer scopes
    transfer_action_provider_scope = MutableScope(
        "https://auth.globus.org/scopes/actions.globus.org/transfer/transfer"
    )
    transfer_scope = TransferScopes.make_mutable("all")

    for collection_id in collection_ids:
        gcs_data_access_scope = GCSCollectionScopeBuilder(collection_id).make_mutable(
            "data_access", optional=True
        )
        transfer_action_provider_scope.add_dependency(gcs_data_access_scope)
        transfer_action_provider_scope.add_dependency(transfer_scope)

    transfer_action_provider_scope.add_dependency(transfer_scope)
    transfer_scopes = [
        "urn:globus:auth:scope:transfer.api.globus.org:all",
        transfer_action_provider_scope
    ]
    pprint(transfer_scope)
    pprint(transfer_scopes)
    pprint(flow_client)
    return flow_client
