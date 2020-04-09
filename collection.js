const BaseDriver = require('./base')
const assert = require('assert')

// Importing typedef will help JS doc to generate and reference global type definitions
require('./typedef')

/**
 * @module collection
 */

/**
 * The Collection class
 * @class
 */
class Collection {
  /**
   * Create an instance of Collection class
   * @param {BaseDriver} driver - the driver for collection
   * @param {string} string - the collection name
   * @param {Object} [config] - the collection config
   * @param {Object} [config.indexes] - the collection indexes
   */
  constructor (driver, name, config = {}) {
    assert.ok(driver instanceof BaseDriver, 'driver must be instanceof BaseDriver')
    /** @member {BaseDriver} */
    this.driver = driver
    /** @member {string} */
    this.name = name
  }

  /*
   * Save or update record to database
   * @param {EntityId} [id] - the doc id at which, that should be created/updated
   * @param {object} data - the content in to write
   * @returns {EntityId} return record id, if saved
   */
  save (id, data) {
    return this.driver.save(this.name, id, data)
  }

  /*
   * delete record from database
   * @param {EntityId} [id] - the doc id at which, that should be created/updated
   * @return {Promise} promise - return a promise
   */
  delete (id) {
    return this.driver.delete(this.name, id)
  }

  /**
   * find one entry
   * @param {Object} filter - the filter to apply
   * @param {Object} options - the options to list the collection
   * @return {Object} return record
   */
  async findOne (filter, options = {}) {
    return this.driver.findOne(this.name, filter, options)
  }

  /**
   * list all the entries
   * @param {Object} filter - the filter to apply
   * @param {Object} [options] - the options to list the collection
   * @return {ListOfRecords} list of records
   */
  async listAll (filter, options) {
    const coll = this.name
    const result = { total: 0 }
    const firstResult = await this.driver.list(coll, filter, Object.assign({}, options, {
      limit: this.driver.defaultPageSize,
      count: true
    }))
    result.records = new Array(firstResult.total)
    result.total = firstResult.total
    Object.assign(result.records, firstResult.records)
    const restOfResults = []
    for (let point = result.records.length; point < result.total; point += this.driver.defaultPageSize) {
      restOfResults.push(this.driver.list(coll, filter, Object.assign({}, options, {
        limit: this.driver.defaultPageSize,
        skip: point,
        count: false
      })))
    }
    (await Promise.all(restOfResults)).forEach((res, ind) => {
      let point = (ind + 1) * this.driver.defaultPageSize
      res.records.forEach((rc) => {
        result.records[point] = rc
        point += 1
      })
    })
    if (result.records.length !== result.total) {
      result.warning = 'Records count were changed while fetching the records.'
    }
    return result
  }

  /**
   * get a previous a document based on options.
   * @param {string} coll - the collection, that should be read
   * @param {EntityId} id - the doc id
   * @param {Object} options - the options to list the collection
   * @return {Promise} promise - return a promise
   */
  async getPrevDoc (coll, id, options) {
    let prevData
    if (options && options.prevData) {
      prevData = await this.driver.read(this.name, id)
      if (!prevData) throw new Error(`Record not exists with id ${id}`)
    }
    return prevData
  }
}

module.exports = Collection
