# Globus

[​Globus](https://www.globus.org/) is a file transport service, developed and operated as not-for-profit by the University of Chicago.  There is also [​Globus Compute](https://www.globus.org/compute), which uses similar infrastructure to link compute services.

The UoC operates the server, and we deploy client software to make "endpoints" on our machines and move files between them.

## How to set up a Globus endpoint

As of globus server v5, the configuration of a globus endpoint for data transfer via API has changed a bit.

The data orchestration code has a ["config.yml"](../config.yml") file that contains configuration for endpoints.

In the following steps, a "Guest Collection" is created, your application is assigned to it, and your client application is given permission to it. 

 > Note: Before performing these steps, have your Globus Application configurated.

To set up a new endpoint,
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

  Now that you have collection for sharing, the "Endpoint UUID" of this collection is your new endpoint UUID. Set it in ["config.yml"]("../config.yml")




