const EventEmitter = require('events')

// Importing typedef will help JS doc to generate and reference global type definitions
require('./typedef')

/**
 * @module base-store
 */

/**
 * The BaseStore class
 * @class
 */
class BaseStore extends EventEmitter {
  /**
   * Create an instance of BaseStore class
   * @param {Object} config - the store config
   */
  constructor ({ url, prefix, defaultPageSize = 10 }) {
    super()
    this.dburl = url
    this.defaultPageSize = defaultPageSize
    this.dbprefix = prefix || 'app_'
  }

  /**
   * Emitting the db event, this will help to setup custom hooks
   * @param {String} event - the event string
   * @param {Object} content - the content of emitted document
   */
  emitDbEvent (event, content) {
    const evs = event.split(':')
    this.emit(evs[0], ...(evs.slice(1).concat(content)))
    this.emit(evs.slice(0, 2).join(':'), ...(evs.slice(2).concat(content)))
    this.emit(event, content)
  }
}

module.exports = BaseStore
