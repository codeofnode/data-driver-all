/**
 * Entity id, that can be mongo id or string or number etc
  @typedef EntityId
*/

/**
 * the config for storage
  @typedef StoreConfig
  @property {string} collectionName - the name of collection
  @property {Object[]} collectionIndexes - list of indexes
*/

/**
 * the list record response
  @typedef ListOfRecords
  @property {Object[]} records - list of records
  @property {number} total - the total number of records in filter
  @property {string} [warning] - the warning if any
*/
