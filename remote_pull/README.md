
# App Engine Python Pull Queue REST API Example #

This app shows an example of using the Pull Queue REST API via another app engine app, by using a service account.

App configuration requires specifying a developer key and service account info from an APIs
console project, and configuring the provider app to allow the service account address to access the relevant
pull queue.

It also requires installation of the python api client libraries, e.g.
		easy_install --upgrade google-api-python-client
then run
	  enable-app-engine-project <project_dir>
for your app engine project directory.

More detailed documentation TBD (this will be a GDA class).
