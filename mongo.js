const MongoClient = require('mongodb').MongoClient
const ObjectId = require('mongodb').ObjectId
const BaseStore = require('./base')
const assert = require('assert')
const Collection = require('./collection')

// Importing typedef will help JS doc to generate and reference global type definitions
require('./typedef')

/**
 * @module mongod-store
 */

/**
 * The MongoStore class
 * @class
 */
class MongoStore extends BaseStore {
  /**
   * Create an instance of BaseStore class
   * @param {Object} config - the mongo store config
   * @param {string} config.url - the mongo url store config
   * @param {Object} [config.dbName] - the mongo db name
   * @param {Object} [config.opts] - the mongo connection options
   */
  constructor (config) {
    config.opts = Object.assign({ useUnifiedTopology: true }, config.opts || {})
    super(config)
    /** @member {boolean} */
    this.ready = false
    MongoClient.connect(config.url, config.opts, (err, client) => {
      assert.strictEqual(null, err)
      this.db = client.db(config.dbName)
      this.ready = true
    })
  }

  /**
   * resolving the collection name with the prefix
   */
  getCollection (name) {
    return this.db.collection(this.dbprefix + name)
  }

  /**
   * generate a random id
   * @param {string} [id] optional id
   *
   * @return {EntityId} id - return random id
   */
  static genId (id) {
    return (new ObjectId(id))
  }

  /**
   * find one entry
   * @param {string} coll - the collection, that should be read
   * @param {Object} filter - the filter to apply
   * @param {Object} options - the options to list the collection
   * @return {Object} return record
   */
  async findOne (coll, filter, options = {}) {
    this.getCollection(coll).findOne(filter, options)
  }

  /**
   * list all the entries
   * @param {string} coll - the collection, that should be read
   * @param {Object} filter - the filter to apply
   * @param {Object} options - the options to list the collection
   * @return {ListOfRecords} promise - return a promise
   */
  async list (coll, filter, options) {
    const { sort, skip = 0, limit = 10, count, projection } = options || {}
    let total = 0
    const collection = this.getCollection(coll)
    if (collection) {
      let cursor = collection.find(filter, { projection })
      if (sort) cursor = cursor.sort(sort)
      let records = (await cursor.skip(skip).limit(limit).toArray())
      if (!projection) {
        records = records.map(doc => doc._id)
      }
      if (count) {
        total = await collection.estimatedDocumentCount(filter)
        return { records, total }
      }
      return { records }
    }
    return { records: [] }
  }

  /**
   * list all the collections.
   * @return {} promise - return a promise
   */
  listcolls () {
    return Object.keys(this.db)
  }

  /**
   * create or update a document.
   * @param {string} coll - the collection, that should be read
   * @param {EntityId|Object} [id] - the doc id / filter at which, that should be created/updated
   * @param {Object} data - the content in to write
   * @param {Object} options - the options to save record
   *
   * @return {EntityId} id - return an id
   */
  async save (coll, id, data, options = {}) {
    if (id) {
      if (options.upsert === undefined) options.upsert = true
      return this.getCollection(coll).findOneAndUpdate(MongoStore.genId(id).isValid() ? { _id: id } : id, { $set: data }, options)
    } else {
      return this.getCollection(coll).insertOne(data, options)
    }
  }

  /**
   * delete a document.
   * @param {string} coll - the collection, that should be read
   * @param {EntityId|Object} id - the doc id / filter at which, that should be deleted
   * @param {Object} options - the options to delete record
   *
   * @return {Promise} promise - return a promise
   */
  async delete (coll, id, options) {
    return this.getCollection(coll).findOneAndDelete((id && MongoStore.genId(id).isValid()) ? { _id: id } : id || {}, options)
  }

  /**
   * create a collection.
   * @param {string} coll - the new collection, that should be created
   * @param {Object} options - the options to create indexes
   * @return {Collection} return collection instance
   */
  mkcoll (coll, options, indexes = []) {
    MongoStore.waitFor(() => this.db !== undefined, (er) => {
      if (er) throw er
      const col = this.db.createCollection(coll)
      if (indexes.length) {
        col.createIndexes(indexes, options).catch(er => {
          throw er
        })
      }
    }, 'Could not connect to mongodb.')
    return new Collection(this, coll)
  }

  /**
   * a sleep while loop
   */
  static waitFor (cond, fn, err = 'waiting_timed_out', interval = 1000, count = 60) {
    if (count < 0) fn(new Error(err))
    else if (cond()) fn()
    else setTimeout(MongoStore.waitFor.bind(undefined, cond, fn, err, interval, --count), interval)
  }

  /**
   * remove a collection.
   * @param {string} coll - the collection name that should be deleted
   */
  rmcoll (coll) {
    this.db.dropCollection(coll).catch(er => {
      throw er
    })
  }
}

module.exports = MongoStore
