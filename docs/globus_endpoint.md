# Notes about Globus Endpoints

## Configuring an endpoint
As of globus server v5, the configuration of a globus endpoint for data transfer via API has changed a bit.

The data orchestration code has a ("config.yml")[../config.yml"] file that contains configuration for endpoints.

 > Note: A "Guest Collection" is created and your application is assigned to it and your client application is given permission to it. Before performing this step, have your Globus Application configurated.

To setup a new endpoint,
* Create a collection for transfer.
  * In the globus web app, find the endpoint and click on the vertical ellipse to view the endpoint details.
  * Click on the "Collections" tab
  * Click on "Add Guest Collection"
  * Authenticate to your endpoint
  * Select the root of your collection. This is relative to the directory configrured in your endpoint.
  * Create the endpoint
  * Click "Add Permissions"
  * Click "Add Permission"
  * in the "User Name or Email" field, past your application's ClientId. Wait while the UI finds your account. If you do this correctly, it will resolve your application with and email like "ALS Transfer Client Credentials App (xxxxxxx@clients.auth.globus.org)"
  * Add an email
  * Click "Add Permission"

  Now that you have collection for sharing, the "Endpoint UUID" of this collection is your new endpoint UUID. Set it in ("config.yml")[../config.yml"]




