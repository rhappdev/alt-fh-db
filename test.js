const mongoUrl = process.env.MONGODB_CONN_URL || 'mongodb://localhost:27017/db';
process.env.FH_MONGODB_CONN_URL = mongoUrl;

const expect = require('chai').expect;
const mongo = require('./index.js').client;

const mongodb = require('mongodb').MongoClient;

const testCollection = 'TEST';
const mongoOptions = { useNewUrlParser: true };

const emptyFunc = function() {};

/* 
 * Helper functions to prepare the dateabase
 */
const insertDoc = function(doc, callback) {
  let mongoClient;
  return mongodb.connect(mongoUrl, mongoOptions)
  .then(client => {
    mongoClient = client;
    let db = client.db();
    return db.collection(testCollection).insertOne(doc);
  })
  .then(result => {
    mongoClient.close();
    return result;
  })
  .then(result => {
    callback(null, result.ops[0]);
  })
  .catch(error => {
    callback(error);
  });
};

const insertNDocs = function(numDocs, callback) {
  let mongoClient;
  return mongodb.connect(mongoUrl, mongoOptions)
  .then(client => {
    mongoClient = client;
    let docs = [];
    let strings = ['some', 'random', 'strings', 'to', 'be', 'used', 'for', 'testing'];
    for (let i = 0; i < numDocs; i++) {
      let str = strings[i] || null;
      docs.push({[i.toString()]: i, num: i, str: str});
    }
    let db = client.db();
    return db.collection(testCollection).insertMany(docs);
  })
  .then(result => {
    mongoClient.close();
    return result;
  })
  .then(result => {
    callback(null, result.ops);
  })
  .catch(error => {
    callback(error);
  });
};

const countDocs = function(callback) {
  let mongoClient;
  return mongodb.connect(mongoUrl, mongoOptions)
  .then(client => {
    mongoClient = client;
    let db = client.db();
    return db.collection(testCollection).countDocuments();
  })
  .then(result => {
    mongoClient.close();
    return result;
  })
  .then(result => {
    callback(null, result);
  })
  .catch(error => {
    callback(error);
  });
};

const deleteAll = function(callback) {
  let db;
  let mongoClient;
  return mongodb.connect(mongoUrl, mongoOptions)
  .then(client => {
    mongoClient = client;
    db = client.db();
    return db.collection(testCollection).estimatedDocumentCount()
  })
  .then(result => {
    if (result > 0) {
      // console.log(`Collection ${testCollection} has ${result} documents, cleaning up...`);
      return db.collection(testCollection).deleteMany()
    }
    else {
      // console.log(`Collection ${testCollection} is empty`);
      return true;
    }
  })
  .then(result => {
    mongoClient.close();
    return result;
  })
  .then((result) => {
    if (result.deletedCount) {
      // console.log(`${result.deletedCount} documents deleted from collection ${testCollection}`);
    }
    callback(null, true);
  })
  .catch(error => {
    console.log(error);
  });
}

const filterObjectKeep = function(object, keysToKeep) {
  return Object.keys(object)
  .filter(key => keysToKeep.includes(key))
  .reduce((obj, key) => {
    return {
      ...obj,
      [key]: object[key]
    };
  }, {});
};

const formatDocs = function(docs) {
  return docs.map(doc => {
    let id = doc._id.toHexString();
    delete doc._id;
    return { type: testCollection, guid: id, fields: doc };
  });
}

// See https://access.redhat.com/documentation/en-us/red_hat_mobile_application_platform_hosted/3/html/cloud_api/fh-db for the behavior of fh-db

describe('mongoDBClient', function() {

  context('using callbacks', function() {

    describe('db', function() {
      it("should throw an error if the 'act' option is not present", function(done){
        expect(() => mongo.db({
          type: 'some-type',
          fields: {}
        }, emptyFunc)).to.throw("'act' undefined in params");
        done();
      });

      it("should return an error if the 'act' option is not correct", function(done){
          mongo.db({
          act: 'invalid',
          type: 'some-type',
          fields: {}
        }, function(err, result){
          expect(err).not.null;
          expect(err.toString()).to.contain('Unknown fh.db action');
          expect(result).to.be.undefined;
          done();
        });
      });

      describe("action 'create'", function() {
        it('should create a single document and return the result', function(done) {
          const type = testCollection;
          let doc = {
            one: 1,
            two: 2
          };
          mongo.db({
            act: 'create',
            type: type,
            fields: doc
          }, function(err, result){
            if (err) return done(err);
            expect(err).to.be.undefined;
            expect(result.type).to.equal(type);
            // Delete the _id because the mongo driver adds one
            delete doc._id;
            expect(result.fields).to.deep.equal(doc);
            done();
          });
        });

        it('should create multiple recods in one call and return the status', function(done) {
          var type = testCollection;
          var docs = [{
            one: 1,
            two: 2
          },{
            three: 3,
            four: 4
          }];
          mongo.db({
            act: 'create',
            type: type,
            fields: docs
          }, function(err, result){
            if (err) return done(err);
            expect(err).to.be.undefined;
            expect(result).to.have.keys(['Status', 'Count']);
            expect(result.Status).to.equal('OK');
            expect(result.Count).to.equal(docs.length);
            done();
          });
        });

        it("should throw an error if 'type' is not provided", function(done) {
          expect(() => mongo.db({
            act: 'create',
            fields: {}
          }, emptyFunc)).to.throw("'type' undefined in params");
          done();
        });
      });

      describe("action 'read'", function() {
        it('should read a single document and return the result', function(done) {
          const type = testCollection;
          let doc = {
            one: 1,
            two: 2
          };
          insertDoc(doc, function(err, result){
            if (err) return done(err);
            let id = result._id.toHexString();
            mongo.db({
              act: 'read',
              type: type,
              guid: id
            }, function(err, result){
              if (err) return done(err);
              expect(err).to.be.undefined;
              expect(result.type).to.equal(type);
              expect(result.guid).to.equal(id);
              // Delete the _id because the mongo driver adds one
              delete doc._id;
              expect(result.fields).to.deep.equal(doc);
              done();
            });
          })
        });

        it("should read a single document and return filtered results if 'fields' option is provided", function(done) {
          let type = testCollection;
          let doc = {
            one: 1,
            two: 2,
            three: 3
          };
          let fields = ['one', 'three'];
          insertDoc(doc, function(err, result){
            if (err) return done(err);
            let id = result._id.toHexString();
            mongo.db({
              act: 'read',
              type: type,
              guid: id,
              fields: fields
            }, function(err, result){
              if (err) return done(err);
              expect(err).to.be.undefined;
              expect(result.type).to.equal(type);
              expect(result.guid).to.equal(id);
              let filtered = filterObjectKeep(doc, fields);
              expect(result.fields).to.deep.equal(filtered);
              done();
            });
          })
        });

        it("should throw an error if 'type' is not provided", function(done) {
          expect(() => mongo.db({
            act: 'read',
            fields: {}
          }, emptyFunc)).to.throw("'type' undefined in params");
          done();
        });

        it("should return an empty object if 'guid' is not provided", function(done) {
          let type = testCollection;
          mongo.db({
            act: 'read',
            type: type
          }, function(err, result){
            if (err) return done(err);
            expect(err).to.be.undefined;
            expect(result).to.be.instanceof(Object);
            expect(result).to.be.empty;
            done();
          });
        });

        it("should return an emtpy object if the collection doesn't exist", function(done) {
          mongo.db({
            act: 'read',
            type: 'SOME-COLLECTION',
            guid: 'some-guid'
          }, function(err, result){
            if (err) return done(err);
            expect(err).to.be.undefined;
            expect(result).to.be.instanceof(Object);
            expect(result).to.be.empty;
            done();
          });
        });
      });

      describe("action 'update'", function() {
        it('should update an entire document and return the result', function(done) {
          const type = testCollection;
          let doc = {
            one: 1,
            two: 2
          };
          let newDoc = {
            three: 3,
            four: 4,
            five: 'five'
          }
          insertDoc(doc, function(err, result){
            if (err) return done(err);
            let id = result._id.toHexString();
            mongo.db({
              act: 'update',
              type: type,
              guid: id,
              fields: newDoc
            }, function(err, result){
              if (err) return done(err);
              expect(err).to.be.undefined;
              expect(result.type).to.equal(type);
              expect(result.guid).to.equal(id);
              // Delete the _id because the mongo driver adds one
              delete doc._id;
              expect(result.fields).to.deep.equal(newDoc);
              done();
            });
          })
        });

        it("should throw an error if 'type' is not provided", function(done) {
          expect(() => mongo.db({
            act: 'update',
            fields: {}
          }, emptyFunc)).to.throw("'type' undefined in params");
          done();
        });

        it("should return an error if 'fields' is not provided", function(done) {
          let type = testCollection;
          mongo.db({
            act: 'update',
            type: type,
            guid: 'some-guid'
          }, function(err, result){
            expect(err).not.null;
            expect(err.toString()).to.contain("Invalid Params - 'fields' object required");
            expect(result).to.be.undefined;
            done();
          });
        });

        it("should return an error if 'guid' is not provided (failure in FeedHenry is expected)", function(done) {
          let type = testCollection;
          mongo.db({
            act: 'update',
            type: type,
            fields: {}
          }, function(err, result){
            expect(err).not.null;
            // NOTE: fh.db actually throws an unhandled exception here: `Uncaught TypeError: Cannot read property 'toString' of undefined`
            // This is not really nice, so changing this to a callback error
            expect(err.toString()).to.contain("Invalid Params - 'guid' is required");
            expect(result).to.be.undefined;
            done();
          });
        });
      });

      describe("action 'delete'", function() {
        it('should delete an entry and return the original document', function(done) {
          const type = testCollection;
          let doc = {
            one: 1,
            two: 2
          };
          insertDoc(doc, function(err, insertedDoc){
            if (err) return done(err);
            let id = insertedDoc._id.toHexString();
            mongo.db({
              act: 'delete',
              type: type,
              guid: id
            }, function(err, result){
              if (err) return done(err);
              expect(err).to.be.undefined;
              expect(result.guid).to.equal(id);
              expect(result.type).to.equal(type);
              // Delete the _id because the mongo driver adds one
              delete doc._id;
              expect(result.fields).to.deep.equal(doc);
              done();
            });
          })
        });
      
        it("should throw an error if 'type' is not provided", function(done) {
          expect(() => mongo.db({
            act: 'delete',
            fields: {}
          }, emptyFunc)).to.throw("'type' undefined in params");
          done();
        });
      
        it("should return an empty object if 'guid' is not provided", function(done) {
          let type = testCollection;
          mongo.db({
            act: 'delete',
            type: type
          }, function(err, result){
            if (err) return done(err);
            expect(err).to.be.undefined;
            expect(result).to.be.instanceof(Object);
            expect(result).to.be.empty;
            done();
          });
        });
      });

      describe("action 'deleteall'", function() {
        it('should delete all documents in the collection', function(done) {
          const type = testCollection;
          countDocs(function(err, numDocs){
            if (err) return done(err);
            mongo.db({
              act: 'deleteall',
              type: type
            }, function(err, result){
              if (err) return done(err);
              expect(err).to.be.undefined;
              expect(result.status).to.equal('ok');
              expect(result.count).to.equal(numDocs);
              done();
            });
          });
        });
      
        it("should throw an error if 'type' is not provided", function(done) {
          expect(() => mongo.db({
            act: 'deleteall'
          }, emptyFunc)).to.throw("'type' undefined in params");
          done();
        });
      });

      describe("action 'list'", function() {
        it('should list all documents in the collection', function(done) {
          const type = testCollection;
          deleteAll(function(err, status){
            if (err) return done(err);
            let numDocs = 5;
            insertNDocs(5, function(err, insertedDocs) {
              mongo.db({
                act: 'list',
                type: type
              }, function(err, result){
                if (err) return done(err);
                let expectedDocs = formatDocs(insertedDocs);
                expect(err).to.be.undefined;
                expect(result.count).to.equal(numDocs);
                expect(result.list).to.deep.equal(expectedDocs);
                done();
              });
            });
          });
        });
      
        it("should list all documents and sort them", function(done) {
          const type = testCollection;
          deleteAll(function(err, status){
            if (err) return done(err);
            let numDocs = 5;
            insertNDocs(5, function(err, insertedDocs) {
              mongo.db({
                act: 'list',
                type: type,
                sort: {
                  str: 1
                }
              }, function(err, result){
                if (err) return done(err);
                let expectedDocs = formatDocs(insertedDocs);
                // sort by 'fields.str'
                expectedDocs = expectedDocs.sort((a,b) => a.fields.str.localeCompare(b.fields.str));
                expect(err).to.be.undefined;
                expect(result.count).to.equal(numDocs);
                expect(result.list).to.deep.equal(expectedDocs);
                done();
              });
            });
          });
        });

        it("should list all documents and sort them in reverse order", function(done) {
          const type = testCollection;
          deleteAll(function(err, status){
            if (err) return done(err);
            let numDocs = 5;
            insertNDocs(5, function(err, insertedDocs) {
              mongo.db({
                act: 'list',
                type: type,
                sort: {
                  str: -1
                }
              }, function(err, result){
                if (err) return done(err);
                let expectedDocs = formatDocs(insertedDocs);
                // sort by 'fields.str', reverse
                expectedDocs = expectedDocs.sort((a,b) => b.fields.str.localeCompare(a.fields.str));
                expect(err).to.be.undefined;
                expect(result.count).to.equal(numDocs);
                expect(result.list).to.deep.equal(expectedDocs);
                done();
              });
            });
          });
        });

        it("should list all documents with pagination", function(done) {
          const type = testCollection;
          deleteAll(function(err, status){
            if (err) return done(err);
            let options = {
              skip: 2,
              limit: 3
            };
            insertNDocs(12, function(err, insertedDocs) {
              mongo.db({
                act: 'list',
                type: type,
                ...options
              }, function(err, result){
                if (err) return done(err);
                let expectedDocs = formatDocs(insertedDocs);
                // skip the first elements and take only 'limit' elements
                expectedDocs = expectedDocs.slice(options.skip, options.skip+options.limit);
                expect(err).to.be.undefined;
                expect(result.count).to.equal(expectedDocs.length);
                expect(result.list).to.deep.equal(expectedDocs);
                done();
              });
            });
          });
        });

        it("should list all documents with multiple restrictions", function(done) {
          const type = testCollection;
          deleteAll(function(err, status){
            if (err) return done(err);
            let query = {
              eq: { str: 'some' },
              ne: { num: 10 },
              in: { '0': [0, 1] }
            };
            insertNDocs(5, function(err, insertedDocs) {
              mongo.db({
                act: 'list',
                type: type,
                ...query
              }, function(err, result){
                if (err) return done(err);
                let expectedDocs = formatDocs(insertedDocs);
                // skip the first elements and take only 'limit' elements
                expect(err).to.be.undefined;
                expect(result.count).to.equal(1);
                expect(result.list[0]).to.deep.equal(expectedDocs[0]);
                done();
              });
            });
          });
        });

        it("should list all documents with multiple restrictions, and limited fields", function(done) {
          const type = testCollection;
          deleteAll(function(err, status){
            if (err) return done(err);
            let query = {
              eq: { str: 'some' },
              ne: { num: 10 },
              in: { '0': [0, 1] }
            };
            let fields = ['str'];
            insertNDocs(5, function(err, insertedDocs) {
              mongo.db({
                act: 'list',
                type: type,
                fields: fields,
                ...query
              }, function(err, result){
                if (err) return done(err);
                let expectedDocs = formatDocs(insertedDocs);
                expectedDocs.forEach(function(doc){
                  doc.fields = filterObjectKeep(doc.fields, fields);
                });
                expect(err).to.be.undefined;
                expect(result.count).to.equal(1);
                expect(result.list[0]).to.deep.equal(expectedDocs[0]);
                done();
              });
            });
          });
        });

        it("should list all documents filtered by regex matching", function(done) {
          const type = testCollection;
          let doc = { username: 'this-is-username', password: 'abc' };
          insertDoc(doc, function(err){
            if (err) return done(err);
            let query = {
              like: { username: new RegExp('user', 'i') },
              eq: { password: 'abc' }
            };
            mongo.db({
              act: 'list',
              type: type,
              ...query
            }, function(err, result){
              if (err) return done(err);
              expect(err).to.be.undefined;
              expect(result.count).to.equal(1);
              delete doc._id; // mongodb driver adds the _id to the original object
              expect(result.list[0].fields).to.deep.equal(doc);
              done();
            });
          });
        });
        // TODO: list with Geo search
      });

      describe("action 'index'", function() {
        it('should delete all documents in the collection', function(done) {
          mongo.db({
            act: 'index',
            type: testCollection,
            index: {
              location: "2D",
              str: "ASC"
            }
          }, function(err, result){
            if (err) return done(err);
            expect(err).to.be.undefined;
            expect(result.status).to.equal('OK');
            expect(result.indexName).to.equal('location_2d_str_1');
            done();
          });
        });
      
        it("should return an error if '2d' index is not first", function(done) {
          mongo.db({
            act: 'index',
            type: testCollection,
            index: {
              str: "ASC",
              location: "2D"
            }
          }, function(err, result){
            expect(err).not.null;
            expect(err.toString()).to.contain('2d has to be first in index');
            expect(result).to.be.undefined;
            done();
          });
        });

        // TODO: test for creating the same index twice
      });
    });

  });

  // the 'mongo' instance does not belong to 'fh-mbaas-api'
  if (!mongo.mbaasExpress) {
    context('using promises', () => {

      describe('db', () => {

        it("should return a promise", () => {
          let p = mongo.db({
            act: 'read',
            type: 'test',
            guid: 'fake'
          });
          expect(p).to.be.a('promise');
          return p;
        });

        it("reject promise with an error if the 'act' option is not present", () => {
          return mongo.db({
            type: 'some-type',
            fields: {}
          })
          .then(() => {throw new Error('unexpected promise resolution');})
          .catch(err => {
            expect(err).to.be.an('Error');
            expect(err.message).to.equal("'act' undefined in params");
          });
        });

        it("reject promise with an error if the 'act'option is not correct", () => {
          return mongo.db({
            act: 'invalid',
            type: 'some-type',
            fields: {}
          })
          .then(() => {throw new Error('unexpected promise resolution');})
          .catch(err => {
            expect(err).to.be.an('Error');
            expect(err.message).to.equal('Unknown fh.db action');
          });
        });

        describe("action 'create'", () => {
          it('should create a single document and resolve the promise with the result', () => {
            const type = testCollection;
            let doc = {
              one: 1,
              two: 2
            };
            return mongo.db({
              act: 'create',
              type: type,
              fields: doc
            })
            .then((result) => {
              expect(result.type).to.equal(type);
              // Delete the _id because the mongo driver adds one
              delete doc._id;
              expect(result.fields).to.deep.equal(doc);
            })
            .catch(err => {
              throw(new Error(`unexpected promise rejection with error: ${err}`));
            });
          });
        });
      });
    });
  }

  // TODO: add test for connection close

});

/*
 * Clean up the database collection before starting the test suite
 */
before(function() {
  deleteAll(function(err){
    if (err) {
      console.log(err);
    }
    else {
      console.log(`Collection ${testCollection} initialized`);
    }
  });
});


function dropCollection(db, collectionName) {
  return db.collection(collectionName).drop() 
  .then(result => {
    if (result) {
      console.log(`Collection ${testCollection} dropped`);
    }
  })
  .catch(error => {
    if (error.codeName === 'NamespaceNotFound') {
      console.log(`Collection ${testCollection} doesn't exist, ignoring the error`);
    }
  });
}

/*
 * Drop the database collection before starting the test suite
 */
after(function() {
  let mongoClient;
  mongodb.connect(mongoUrl, mongoOptions)
  .then(client => {
    mongoClient = client;
    return dropCollection;
  })
  .then(() => {
    return mongoClient.close();
  })
  .then(() => {
    console.log('MongoDB connection closed');
    return mongo.close();
  })
  .catch(error => {
    console.log(`Unexpected error error: ${error}`);
  })
});
