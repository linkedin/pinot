import Ember from 'ember';
import _ from 'lodash';
import moment from 'moment';

export default Ember.Component.extend({
  columns: null, // []

  entities: null, // {}

  selectedUrns: null, // Set

  onSelection: null, // function (e)

  /**
   * Returns a list of record objects to display in the table
   * Sets the 'isSelected' property to respond to table selection
   * @type {Object} - object with keys as urns and values as entities
   */
  records: Ember.computed(
    'entities',
    'selectedUrns',
    function () {
      const { entities, selectedUrns } = this.getProperties('entities', 'selectedUrns');

      const records = _.cloneDeep(entities);
      Object.keys(records).forEach(urn => records[urn].isSelected = selectedUrns.has(urn));
      return records;
    }
  ),

  /**
   * Returns a list of values to display in the rootcause table
   * @type {Array[Objects]} - array of entities
   */
  data: Ember.computed(
    'records',
    function () {
      let values = Object.values(this.get('records'));
      values.forEach((value) => {
        value.start = moment(value.start).format('LL');
        value.end = moment(value.end).format('LL');
      });
      return values;
    }
  ),

  /**
   * Keeps track of items that are selected in the table
   * @type {Array}
   */
  preselectedItems: Ember.computed(
    'records',
    'selectedUrns',
    function () {
      const { records, selectedUrns } = this.getProperties('records', 'selectedUrns');
      const selectedEntities = [...selectedUrns].filter(urn => records[urn]).map(urn => records[urn]);
      return selectedEntities;
    }
  ),

  actions: {
    /**
     * Updates the currently selected urns based on user selection on the table
     * @param {Object} e
     */
    displayDataChanged (e) {
      const { records, selectedUrns, onSelection } = this.getProperties('records', 'selectedUrns', 'onSelection');
      if (onSelection) {
        const table = new Set(e.selectedItems.map(e => e.urn));
        const added = [...table].filter(urn => !selectedUrns.has(urn));
        const removed = [...selectedUrns].filter(urn => records[urn] && !table.has(urn));

        const updates = {};
        added.forEach(urn => updates[urn] = true);
        removed.forEach(urn => updates[urn] = false);

        onSelection(updates);
      }
    }
  }
});
