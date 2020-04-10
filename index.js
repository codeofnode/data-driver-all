const MemoryStore = require('./memory')
const MongoStore = require('./mongo')

/**
 * @module store
 */

/**
 * The Store class
 * @class
 */
class Store {
  /*
   * Initialize static config
   * @param {Object} [config] - the config for store prototype
   */
  static init (config = {}) {
    /** the store types
     * @static
     * @member {Object} */
    Store.Types = {
      MEMORY: MemoryStore,
      MONGO: MongoStore
    }
    /** the store configs
     * @static
     * @member {Object} */
    Store.Configs = {
    }
    /** the store instances
     * @static
     * @member {Object} */
    Store.Instances = {
    }
    /** the default config
     * @static
     * @member {string} */
    Store.defaultType = config.defaultType
    delete config.defaultType
    Object.assign(Store.Configs, config)
  }

  /**
   * return instance of store
   * @param {string} [dType] - the db type
   */
  static getIns (dType) {
    dType = dType || Store.defaultType
    if (!Object.prototype.hasOwnProperty.call(Store.Types, dType)) {
      throw new Error('DB Type not supported.')
    }
    if (Object.prototype.hasOwnProperty.call(Store.Instances, dType)) {
      return Store.Instances[dType]
    }
    const ins = new Store.Types[dType](Store.Configs[dType] || {})
    Store.Instances[dType] = ins
    return ins
  }

  /**
   * is instances ready
   * @return {boolean} - whether all ready or not
   */
  static isReady () {
    const instances = Object.keys(Store.Instances)
    const l = instances.length
    for (let k = 0; k < l; k++) {
      if (!Store.Instances[instances[k]].ready) {
        return false
      }
    }
    return true
  }
}

Store.waitFor = MongoStore.waitFor
module.exports = Store
