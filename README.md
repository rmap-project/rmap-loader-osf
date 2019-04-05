# RMap OSF Loader
The RMap OSF Loader is a command line utility for extracting `nodes`, `registrations`, and `users` from the [OSF API (v2)](https://api.osf.io/v2), converting them to [RMap DiSCOs](https://github.com/rmap-project/rmap-documentation/blob/master/api/discos/disco-media-type.md), and then depositing them into to [RMap](https://rmap-hub.org).   Some examples of these can be found on the [RMap sandbox](https://test.rmap-hub.org) . The utility can be run at regular intervals using a scheduler, and includes built in features to avoid duplicates, create new versions of a DiSCO where the graph has changed, retry on failure, and much more.

The loader process has 3 parts that can be completed all at once or as separate processes, providing a chance to review the queued tasks in between each stage. The stages are:
1. Given some filters, identify and queue the records that will be transformed
2. Read from the transform queue, convert each ID listed into a DiSCO and add the result to the ingest queue
3. Read from the ingest queue and POST each DiSCO to RMap. Store the resulting DiSCO ID.

## Prerequisites
The following are required to run this tool:
* Java 8
* MySQL (5.5 or later) or MariaDB (2.0.3 or later). PostgreSQL is not yet supported. 
* Apache ActiveMQ. 
* An RMap API key

## Build / Installation
There is a dependency that is not yet in the dataconservancy maven repository, so for now, to run this first download and build (using `mvn clean install`) the [RMap OSF Client Extension](https://github.com/rmap-project/rmap-osf-client-extension) and make sure it is on the classpath. Then download and build this `rmap-loader-osf` repository using `mvn clean install`. In the `rmap-loader-osf/rmap-loader-osf-service/target` folder there should be a CLI jar that can be used for the following commands.

## Using the Loader

### Environment variables
The RMap OSF Loader depends on a number of environment variables being set.  The required variables are as follows:

* `osf.client.conf` - Described in the [OSF Client documentation](https://github.com/DataConservancy/dcs-packaging-osf/blob/master/osf-client/README.md), this is the file path for the OSF client configuration file that will indicate the API access information. The file should look something like this:
	```
	{
	  "osf": {
	    "v2": {
	      "host": "api.osf.io",
	      "port": "443",
	      "basePath": "/v2/",
	      "authHeader": "Basic ZW138fTnZXJAZ21haWwu98wIOmZvb2JuU43heg==",
	      "scheme": "https"
	    }
	  },
	  "wb": {
	    "v1": {
	      "host": "files.osf.io",
	      "port": "443",
	      "basePath": "/v1/",
	      "scheme": "https"
	    }
	  },
	}
	```
* `jdbc.url` - The JDBC URL for the loader log database. 
* `jdbc.driver` - The classpath of the driver that will be used for the JDBC connection e.g. `com.mysql.jdbc.Driver` 
* `jdbc.username` - The user name for the JDBC connection
* `jdbc.password` - The password for the JDBC connection 
* `rmap.api.baseuri` - The URL for the root of the RMap API that will be used e.g. "https://test.rmap-hub.org/api/" 
* `rmap.api.auth.token` - The key:secret combination downloaded from the user management part of the RMap GUI 
* `jms.brokerUrl` - the URL used for the application to communicate with ActiveMQ e.g. "tcp://localhost:61616" 
* `jms.username` - The ActiveMQ user name
* `jms.password` - The ActiveMQ password

### Options
As mentioned previously, the loader runs in 3 stages - identify, transform, ingest. The options are used to control which stages are run and what records are included.  The options for the OSF Loader are as follows:
#### type (`-t`)
Type defines what type of material you will harvest from OSF. Only one type of material can be harvested in a single run. The options are:
*  "user" - an OSF user record including its linked profile IDs.
*  "node" - a non-registered OSF project or sub project
* "registration" - a registered OSF project 
The default is  "node".

#### filters (`-f`)
The filters option is appended as a querystring to the OSF API request for the type specified. It is only used in the "identify" process since this process is selecting which IDs to process and putting them in a JMS queue. Filters should be formatted according to the [OSF guidelines](https://developer.osf.io/#tag/Filtering) with multiple filters separated by an `&`.  Some examples:
	`filter[date_modified]=2017-05-05`
	`filter[id]=kjd2d`
If no date filters are applied, the loader will by default limit the harvest to collect everything since the last recorded load date (this is recorded in the SQL database), or if there was no previous load it will take items that were modified on the previous day only. This prevents unintentionally harvesting the entire OSF database on the first load!

#### process (`-p`)
The process option indicates which part of the ETL you would like the OSF loader to perform. The following options are available:
 * "identify" - this searches for all records matching the type selected with the filters applied and puts the record into a transform JMS queue
 * "transform" - this inspects the transform queues for newly queued IDs, if it finds any it will harvest the record from OSF and put the resulting DiSCO in the ingest JMS queue. Note that it checks "queues" (plural). In addition to a primary transform queue, there are several "retry" queues where records that fail to transform are placed. The order of the queue processing is such that the transform of each record will be attempted twice back to back, and then once again the next time the process is run. If it continues to fail it will be moved to a transform fail queue for manual inspection.
 * "ingest" - this looks at the ingest queues for newly transformed DiSCOs. If it finds any that are ready for ingest, it looks up the OSF ID in the SQL database, which stores a record of DiSCO URIs for each OSF ID processed. If it does not find a previous version of the DiSCO for that ID, a new DiSCO is created and the resulting OSF ID and DiSCO URI combination is recorded in the SQL database. If an existing DiSCO URI _is_ found, the loader retrieves it and does a triple-by-triple comparison to see if anything has changed since the last version. If nothing has changed, the message is removed from the JMS queue without action. If anything _has_ changed, a new version of the DiSCO is created and linked to the previous version. Similar to the transform queue management, the process allows for several retries before placing a message in the fail queue for manual evaluation.
 * "all" - this process will complete the identify, transform then ingest process back to back.
 * "requeuefails" - if failures are the result of an outage and you suspect a rerun will fix the issue, simply call this process in order for all JMS messages in the transform or ingest fail queues to be moved back to the first transform queue for reprocessing.

#### help (-h)
Prints out the options.

## Example
Putting this all together, a script to run the app might look like this:
```
> java -jar 
	-Dosf.client.conf="file:///home/osfloader/osf-config.json" 
	-Djdbc.url="jdbc:mysql://localhost:3306/osf_loader_log" 
	-Djdbc.driver="com.mysql.jdbc.Driver" 
	-Djdbc.username="osfloader" 
	-Djdbc.password="osfloader" 
	-Drmap.api.baseuri="https://test.rmap-hub.org/api/" 
	-Drmap.api.auth.token="key:secret" 
	-Djms.brokerUrl="tcp://localhost:61616" 
	-Djms.username="admin" 
	-Djms.password="adminpass" 
   /home/osfloader/rmap-loader-osf.jar -t registration -p all -f "filter[date_modified]=2019-01-10
```
This will harvest, transform, and load all registrations that were modified on January 10, 2019.