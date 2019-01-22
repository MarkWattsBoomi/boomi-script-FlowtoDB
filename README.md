# FLOW to DB

A generic script for use in Boomi Flow Services which query a database.

Flow's query XML structure varies in form and is difficult to handle in Boomi.

This script performs the DB query using java code rather than the DB connector.

It's totally generic and respects both the where and order elements sent from Flow.

The script takes it's operational parameters from "Dynamic Process Properties" in the boomi process.

## Setup
You need to set "Dynamic Process Properties" in boomi for the operational parameters.  

Do this by first creating a Process Properties component and giving it these attributes: -

DatabaseDriverClass - The java class name of the driver to use e.g. org.postgresql.Driver for postgres
UserName            - The user name for the connection authentication
Password            - The password for the user name
Protocol            - The protocol element of the jdbc connection string e.g. jdbc:postgresql://
Host                - The URI of the database e.g. ec2-54-xxx-246-59.eu-west-1.compute.amazonaws.com
Port                - The listener port of the DB e,g, 5432 for postgres
Database            - the name of the database
TableName           - The table name to query against (include the schema e.g. myschem.tablename)

Then in the process itself map these parameters to dynamic process properties using a "Set Properties" shape.  
You can hard code at this stage if you prefer and not use the Process Properties component at all or mix and match.

