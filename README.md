# alt-fh-db

This module is aimed to provide an alternative implementation to the database functionality of the [FeedHenry MBaaS API](https://github.com/feedhenry/fh-mbaas-api) (upstream of RHMAP - Red Hat Mobile Application Platform).

## Documentation

The module replicates the `$fh.db` interface (where `$fh` is an instance of `fh-mbaas-api`) documented in the RHMAP documentation on the Red Hat Customer Portal: https://access.redhat.com/documentation/en-us/red_hat_mobile_application_platform_hosted/3/html/cloud_api/fh-db

## Usage

The module is used as follows:

```
var mongoClient = require('alt-fh-db').client;

mongoClient.db({
  act: 'list',
  type: 'Users',
  skip: 20,
  limit: 10
}, function (err, data){
  if (err) {
    console.error("Error " + err);
  } else {
    console.log(JSON.stringify(data));
  }
});
```

If you are migrating from the `$fh.db`, you just need to replace:

```
var $fh = require('fh-mbaas-api');

$fh.db(...);
```

with

```
var mongoClient = require('alt-fh-db').client;
mongoClient.db(...);
```

The MongoDB instance is configured by the environment variable `MONGODB_CONN_URL` - a connection string in the [standard MongoDB format](https://docs.mongodb.com/manual/reference/connection-string/#connections-standard-connection-string-format).

So, when migrating from `$fh.db`, replace `FH_MONGODB_CONN_URL` with `MONGODB_CONN_URL`.

## Testing

Run the tests by executing:

```
MONGODB_CONN_URL=<mongo_url> npm test
```