var MongoClient = require('mongodb').MongoClient;
var ObjectId = require('mongodb').ObjectId;

var MAX_COLLECTION_NAME = 70;

var mongoClientOptions = {
  useNewUrlParser: true,
  useUnifiedTopology: true
};

var MongoDbClient = function (options) {
  var localMongoUrl = process.env.MONGODB_CONN_URL || 'mongodb://localhost:27017/db'
 
  var localDb = null;
  var localClient = null;

  this.connect = function (callback) {
    // console.log('MongoDbClient prototype.mongoConnect ' + this.getMongoUrl()); // move to 'debug' level

    var self = this;

    // Check DB connection is already open or not
    if (self.getDb() === null) {
      // console.log('Creating a new DB connection'); // move to 'debug' level

      MongoClient.connect(self.getMongoUrl(), options, function (err, client) {
        if (err) {
          console.log('Error connecting to MongoDB');
          return callback(err);
        }

        localClient = client;

        // Use the database name from the connection string
        var database = client.db();
        self.setDb(database);
        callback(null, self.getDb());
      });
    } else {
      // console.log('Reusing DB connection'); // move to 'debug' level
      callback(null, self.getDb());
    }
  };

  this.close = function(callback) {
    var client = this.getClient();
    if (client !== null) {
      return client.close(false, callback);
    } else {
      if (!callback || typeof callback !== 'function'){
        return Promise.resolve();
      }
      else {
        return callback();
      }
    }
  }

  /**
   * Get the mongo DB URL
   */
  this.getMongoUrl = function() {
    return localMongoUrl;
  };

  /**
   * Returns the DB instance that was stored from the initial Mongo connect
   */
  this.getDb = function() {
    return localDb;
  };

  /**
   * Sets the DB instance that was stored from the initial Mongo connect
   */
  this.setDb = function setDb (db) {
    localDb = db;
  };

  /**
   * Returns the Mongo client instance
   */
  this.getClient = function getClient () {
    return localClient;
  };
};


/**
 * Helper object for building queries for list call
 * Taken from fh_ditch
 */
var critOps = {
  eq: function (query, fields) {
    var field;
    if (fields !== null) {
      for (field in fields) {
        if (fields.hasOwnProperty(field)) {
          query[field] = fields[field];
        }
      }
    }
  },
  ne: function (query, fields) {
    buildQuery(query, fields, '$ne');
  },
  lt: function (query, fields) {
    buildQuery(query, fields, '$lt');
  },
  le: function (query, fields) {
    buildQuery(query, fields, '$lte');
  },
  gt: function (query, fields) {
    buildQuery(query, fields, '$gt');
  },
  ge: function (query, fields) {
    buildQuery(query, fields, '$gte');
  },
  like: function (query, fields) {
    buildQuery(query, fields, '$regex');
  },
  'in': function (query, fields) {
    buildQuery(query, fields, '$in');
  },
  geo: function (query, fields) {
    if (fields !== null) {
      var field;
      var earthRadius = 6378; // km
      for (field in fields) {
        if (fields.hasOwnProperty(field)) {
          var queryField = {};
          if (typeof query[field] !== 'undefined') {
            queryField = query[field];
          }
          queryField['$within'] = {
            '$centerSphere': [ // supported by mongodb V1.8 & above
              fields[field].center,
              fields[field].radius / earthRadius
            ]
          };
          query[field] = queryField;
        }
      }
    }
  }
};

/**
 * Helper method for building queries for list call
 * Taken from fh_ditch
 * @param {*} query
 * @param {*} fields
 * @param {*} expression
 */
function buildQuery (query, fields, expression) {
  var field;
  if (fields !== null) {
    for (field in fields) {
      if (fields.hasOwnProperty(field)) {
        var queryField = {};
        if (typeof query[field] !== 'undefined') {
          queryField = query[field];
        }
        queryField[expression] = fields[field];
        query[field] = queryField;
      }
    }
  }
}

/**
 * Helper method for returning the correct response
 * Taken from fh_ditch
 * @param {*} document
 * @param {*} type
 */
function generateReturn (document, type) {
  var retDoc = {};
  if (document !== null && typeof (document) !== 'undefined') {
    if (document._id) {
      retDoc.type = type;
      retDoc.guid = document._id.toString(); // switched from .toHexString() to prevent failing when _id is not ObjecdId
    }
    var i = 0;
    for (var field in document) {
      if (field !== '_id') {
        if (i === 0) {
          retDoc.fields = {};
          i = 1;
        }
        retDoc.fields[field] = document[field];
      }
    }
  }
  return retDoc;
}

function createObjectIdFromHexString (str) {
  var response;
  try {
    response = ObjectId.createFromHexString(str);
    return response;
  } catch (err) {
    // console.log('Caught error converting ID', str, ': ', err); // move to 'debug' level
    return str;
  }
};

/**
 * Mimic the create action from fh.db
 * Inserts one or many records in a collection
 */
MongoDbClient.prototype.create = function (db, options, callback) {
  var fields = options.fields;

  if (!fields || typeof fields !== 'object' || (Array.isArray(fields) && fields.length === 0)) {
    return callback(new Error("Fields need to be set as an object or array for '" + options.act + "' action."));
  }

  if (!Array.isArray(fields)) {
    fields = [fields];
  }
  fields.forEach(function(doc){
    if (doc.hasOwnProperty('_id') && doc._id.length === 24){
      doc._id = createObjectIdFromHexString(doc._id);
    }
  });

  db.collection(options.type).insertMany(fields, {}, function (err, result) {
    if (null !== err) {
      console.log("Error in MongoDB create:", err);
      return callback(err);
    }
    var count = result.insertedCount;
    var ret;
    if (count === 1) {
      ret = generateReturn(result.ops[0], options.type);
    } else {
      ret = {
        'Status': 'OK',
        'Count': count
      };
    }
    callback(undefined, ret);
  });
};


/**
 * Mimic the read action from fh.db
 * Reads a single row from the collection
 */
MongoDbClient.prototype.read = function (db, options, callback) {
    var guid = options.guid;
    if (!guid) {
      // fh.db actually queries the DB with empty guid and formats the null response as {}, but we'll just use shortcut
      return callback(undefined, {});
    }

    var query = {'_id': createObjectIdFromHexString(guid)};

    var mongoOptions = {};
    if (options.fields) {
      var fields = {};
      for (var i = 0; i < options.fields.length; i += 1) {
        fields[options.fields[i]] = 1;
      }
      mongoOptions.projection = fields;
    }

    db.collection(options.type).findOne(query, mongoOptions, function (err, result) {
      if (null !== err) {
        console.log("Error in MongoDB read:", err);
        return callback(err);
      }
      var ret = generateReturn(result, options.type);
      callback(undefined, ret);
    });
};

/**
 * Mimic the list action from fh.db
 * Reads multiple rows from the collection
 */
MongoDbClient.prototype.list = function (db, options, callback) {
  var query = {};
  for (var op in critOps) {
    var fieldsValues = options[op];
    if (fieldsValues) {
      critOps[op](query, fieldsValues);
    }
  }

  var mongoOptions = {};

  if (options.fields) {
    var fields = {};
    for (var i = 0; i < options.fields.length; i += 1) {
      fields[options.fields[i]] = 1;
    }
    mongoOptions.projection = fields;
  }
  
  if (options.skip && typeof options.skip === 'number' && options.skip >= 0) {
    mongoOptions.skip = options.skip;
  }
  if (options.limit && typeof options.limit === 'number' && options.limit > 0) {
    mongoOptions.limit = options.limit;
  }
  if (options.sort && typeof options.sort === 'object') {
    mongoOptions.sort = options.sort;
  }

  db.collection(options.type).find(query, mongoOptions).toArray(function (err, docs) {
    if (null !== err) {
      console.log("Error in MongoDB list:", err);
      return callback(err);
    }
    var retDocs = [];

    for (var i = 0; i < docs.length; i += 1) {
      retDocs.push(generateReturn(docs[i], options.type));
    }
    var listResp = {count: retDocs.length, list: retDocs};
    callback(undefined, listResp);
  });
};

/**
 * Mimic the update action from fh.db
 * Reads multiple rows from the collection
 */
MongoDbClient.prototype.update = function (db, options, callback) {
  var self = this;
  var guid = options.guid;
  var fields = options.fields;

  if (!fields) {
    return callback(new Error("Invalid Params - 'fields' object required"));
  }

  if (!guid) {
    return callback(new Error("Invalid Params - 'guid' is required"));
  }

  var query = {'_id': createObjectIdFromHexString(guid)};

  db.collection(options.type).findOneAndReplace(query, fields, {}, function (err, result) {
    if (null !== err) {
      console.log("Error in MongoDB update:", err);
      return callback(err);
    }
    // TODO process the response of the update operation instead of making another read operation
    self.read(db, options, callback);
  });
};

/**
 * Mimic the delete action from fh.db
 * Delete a row from the collection
 */
MongoDbClient.prototype.delete = function (db, options, callback) {
  var guid = options.guid;

  if (!guid) {
    // fh.db actually tries to delete from the DB with empty guid and formats the null response as {}, but we'll just use shortcut
    return callback(undefined, {});
  }

  var query = {'_id': createObjectIdFromHexString(guid)};

  db.collection(options.type).findOneAndDelete(query, {}, function (err, result) {
    if (null !== err) {
      console.log("Error in MongoDB delete:", err);
      return callback(err);
    }
    var ret = generateReturn(result.value, options.type);
    callback(undefined, ret);
  });
};

/**
 * Mimic the deleteall action from fh.db
 * Delete all collection entries
 */
MongoDbClient.prototype.deleteall = function (db, options, callback) {
  db.collection(options.type).deleteMany({}, {}, function (err, result) {
    if (null !== err) {
      console.log("Error in MongoDB deleteall:", err);
      return callback(err);
    }
    // TODO: check if result.result.n returns the same
    var status = { status: 'ok', count: result.deletedCount };
    callback(undefined, status);
  });
};

/**
 * Mimic the 'index' action from fh.db
 * Add an index to  field/fields
 */
MongoDbClient.prototype.index = function (db, options, callback) {
  var indexes = options.index;
  if (typeof indexes === "undefined") {
    return callback(new Error("Invalid Params - 'index' object required for " + options.act + " action."));
  }

  var mapObj = {
    "ASC": 1,
    "DESC": -1,
    "2D": "2d"
  };
  for (var indx in indexes) {
    var type = indexes[indx].toString().toUpperCase();
    var mongoType = mapObj[type] || 1;
    indexes[indx] = mongoType;
  }

  db.collection(options.type, function (err, collection) {
    if (null === collection) return callback(new Error("Collection doesn't exist"));
    if (err) {
      return callback(err);
    }
    collection.createIndex(indexes, function (err, indexName) {
      if (err) return callback(err);
      callback(undefined, {"status": "OK", "indexName": indexName});
    });
  });
};


/**
 * Check fh.db options are valid before processing the DB action
 */
MongoDbClient.prototype.validateOptions = function (options, callback) {
  if (!options.act) {
    throw new Error("'act' undefined in params");
  }
  if (!(options.type) && ['close', 'list', 'export', 'import'].indexOf(options.act) === -1) {
    throw new Error("'type' undefined in params");
  }
  if (options.type && options.type.length > MAX_COLLECTION_NAME) {
    return callback("Error: 'type' name too long: '" + options.type + "'. Collection name cannot be greater than: " + MAX_COLLECTION_NAME);
  }
};

MongoDbClient.prototype.processAction = function (options, callback) {
  this.validateOptions(options, callback);

  var action = this[options.act];
  // Check that a function passed in 'action' exists
  if (typeof action !== 'function') {
    return callback(new Error("Unknown fh.db action"));
  } else {
    action = action.bind(this);
  }

  this.connect(function(err, db) {
    if (null !== err) {
      console.log('Error connecting to DB:', err);
      return callback(err);
    } else {
      action(db, options, callback);
    }
  });
};

/**
 * Sets the db method on MongoDbClient which will allow for easy swap of fh.db
 * Allows for promise or callback
 */
MongoDbClient.prototype.db = function (options, callback) {
  var self = this;
  if (callback && typeof callback === 'function'){
    return self.processAction(options, callback);
  }
  else {
    return new Promise(function(resolve, reject) {
      try {
        self.processAction(options, function(err, result){
          if (err) {
            return reject(err);
          }
          resolve(result);
        })
      } catch (error) {
        reject(error);
      }
    });
  }
};

exports.client = new MongoDbClient(mongoClientOptions);
