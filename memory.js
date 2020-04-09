const jsonpath = require('jsonpath')
const BaseStore = require('./base')
const Collection = require('./collection')

// Importing typedef will help JS doc to generate and reference global type definitions
require('./typedef')

/**
 * @module memory-store
 */

/**
 * The MemoryStore class
 * @class
 */
class MemoryStore extends BaseStore {
  /**
   * Create an instance of BaseStore class
   * @param {Object} config - the store config config
   */
  constructor (config) {
    super(config)
    /** @member {Object} */
    this.data = {}
    this.dataObjects = {}
    this.dataIndexes = {}
  }

  /**
   * resolving the collection name with the prefix
   */
  getCollection (name) {
    return this.dbprefix + name
  }

  /**
   * generate a random id
   * @return {EntityId} id - return random id
   */
  static genId () {
    return Date.now()
  }

  /**
   * query json with jsonpath
   * @param {*} data - the input data
   * @param {string} path - the path to query for
   *
   * @return {*} return any value
   */
  static jsonquery (data, path) {
    let res = data
    if (path.indexOf('LEN()<') === 0) {
      return jsonpath.query(data, path.substring(6)).length
    } else if (typeof path === 'string' && path.indexOf('TYPEOF<') === 0) {
      return (typeof jsonpath.query(data, path.substring(7))[0])
    } else if (path.indexOf('ARRAY<') === 0) {
      return jsonpath.query(data, path.substring(6))
    } else if (path.indexOf('<') === 5) {
      const count = parseInt(path.substr(0, 5), 10)
      if (!isNaN(count)) {
        return jsonpath.query(data, path.substring(6), count)
      }
    }
    if (data instanceof Object && typeof path === 'string' && path) {
      res = jsonpath.query(data, path, 1)
    }
    res = (Array.isArray(res) && res.length < 2) ? res[0] : res
    return res
  }

  /**
   * query json with jsonpath
   * @param {*} filter - the filter
   * @param {Object} ob - the source object of comparision
   * @param {string} ky - the key to match with
   *
   * @return {boolean} return true if matched
   */
  static matchWithFilter (filter, ob, ky) {
    if (typeof filter[ky] === 'object') {
      const kys = Object.keys(filter[ky])
      for (let i = 0; i < kys.length; i++) {
        const kky = kys[i]
        if (kky.charAt(0) === '$') {
          switch (kky) {
            case '$lt':
              if (ob[ky] >= filter[ky][kky]) return false
              break
            case '$lte':
              if (ob[ky] > filter[ky][kky]) return false
              break
            case '$gt':
              if (ob[ky] <= filter[ky][kky]) return false
              break
            case '$gte':
              if (ob[ky] < filter[ky][kky]) return false
              break
          }
        } else if (ob[ky] !== filter[ky][kky]) return false
      }
      return true
    } else {
      return filter[ky] === ob[ky]
    }
  }

  /**
   * find one entry
   * @param {string} coll - the collection, that should be read
   * @param {Object} filter - the filter to apply
   * @param {Object} options - the options to list the collection
   * @return {Object} return record
   */
  async findOne (coll, filter, options = {}) {
    const ind = await this.findIndex(coll, filter)
    if (ind === undefined || ind === -1) {
      return (await this.list(coll, filter, Object.assign(options, { limit: 1 }))).records.shift()
    }
    return this.data[coll][ind]
  }

  /**
   * list all the entries
   * @param {string} coll - the collection, that should be read
   * @param {Object} filter - the filter to apply
   * @param {Object} options - the options to list the collection
   * @return {ListOfRecords} promise - return a promise
   */
  async list (coll, filter, options) {
    const skip = options.skip || 0
    const limit = options.limit || this.defaultPageSize
    let records = this.data[coll].filter((a) => {
      return Object.keys(filter).reduce((bool, ky) => {
        if (options.jsonquery === true) {
          return MemoryStore.jsonquery(a, ky) === filter[ky]
        } else {
          return MemoryStore.matchWithFilter(filter, a, ky)
        }
      }, true)
    })
    if (options.sort) {
      const srt = options.sort
      const sortKeys = Object.keys(srt)
      for (let z = 0; z < sortKeys.length; z++) {
        const ky = sortKeys[z]
        records = records.sort((a, b) => srt[ky] === -1 ? (a[ky] > b[ky] ? -1 : 1) : (a[ky] < b[ky] ? -1 : 1))
      }
    }
    return {
      records: records.slice(skip, skip + limit),
      total: records.length
    }
  }

  /**
   * list all the collections.
   * @return {} promise - return a promise
   */
  listcolls () {
    return Object.keys(this.data)
  }

  /**
   * find index based on filter
   * @param {string} coll - the collection, that should be read
   * @param {EntityId|Object} filter - the filter to apply
   *
   * @return {number} the index at which object is stored
   */
  async findIndex (coll, filter) {
    if (!filter) return -1
    if (typeof filter !== 'object') {
      return this.dataObjects[coll][filter]
    }
    if (Object.hasOwnProperty.call(filter, 'id')) {
      return this.dataObjects[coll][filter.id]
    }
    const kyl = Object.keys(filter)
    const kys = kyl.join(MemoryStore.INDEX_SPLITTER)
    if (Object.hasOwnProperty.call(this.dataIndexes[coll], kys)) {
      const sps = kyl.map(k => filter[k]).join(MemoryStore.INDEX_SPLITTER)
      if (Object.hasOwnProperty.call(this.dataIndexes[coll][kys], sps)) {
        return this.dataIndexes[coll][kys][sps]
      }
    }
    return -1
  }

  /**
   * create or update a document.
   * @param {string} coll - the collection, that should be read
   * @param {EntityId|Object} [id] - the doc id / filter at which, that should be created/updated
   * @param {Object} data - the content in to write
   *
   * @return {EntityId} id - return an id
   */
  async save (coll, id, data) {
    const ind = await this.findIndex(coll, id)
    if (ind !== -1) {
      Object.assign(this.data[coll][ind], data)
      return id
    }
    data.id = MemoryStore.genId()
    this.dataObjects[coll][data.id] = this.data[coll].length
    Object.keys(this.dataIndexes[coll]).forEach((ky) => {
      const sps = ky.split(MemoryStore.INDEX_SPLITTER).map(k => data[k]).join(MemoryStore.INDEX_SPLITTER)
      this.dataIndexes[coll][ky][sps] = this.dataObjects[coll][data.id]
    })
    this.data[coll].push(data)
    return data.id
  }

  /**
   * delete a document.
   * @param {string} coll - the collection, that should be read
   * @param {EntityId|Object} id - the doc id / filter at which, that should be deleted
   * @return {Promise} promise - return a promise
   */
  async delete (coll, id) {
    const ind = this.findIndex(coll, id)
    if (ind && ind !== -1) {
      const deleted = this.data[coll].splice(this.data[coll][ind], 1).pop()
      Object.keys(this.dataIndexes[coll]).forEach((ky) => {
        const sps = ky.split(MemoryStore.INDEX_SPLITTER).map(k => deleted[k]).join(MemoryStore.INDEX_SPLITTER)
        delete this.dataIndexes[coll][ky][sps]
      })
      delete this.dataObjects[coll][id]
    } else {
      throw new Error('Record not found.')
    }
  }

  /**
   * create a collection.
   * @param {string} coll - the new collection, that should be created
   * @return {Collection} return collection instance
   */
  mkcoll (coll, indexes) {
    const collection = new Collection(this, coll)
    this.data[coll] = []
    this.dataObjects[coll] = {}
    this.dataIndexes[coll] = {}
    indexes.forEach((ind) => {
      this.dataIndexes[coll][Object.keys(ind).join(MemoryStore.INDEX_SPLITTER)] = {}
    })
    return collection
  }

  /**
   * remove a collection.
   * @param {string} coll - the collection name that should be deleted
   */
  rmcoll (coll) {
    delete this.data[coll]
    delete this.dataObjects[coll]
    delete this.dataIndexes[coll]
  }
}

/** the splitter for indexes
 * @static
 * @member {string} */
MemoryStore.INDEX_SPLITTER = '<+>'
module.exports = MemoryStore
