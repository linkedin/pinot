import Ember from 'ember';
import moment from 'moment';
import fetch from 'fetch';

const generateRandomData = (num, min=0, max=100) => {
  const diff = max - min;
  const dataPoints = [];
  for (let i = 0; i < num; i++) {
    dataPoints.push(Math.random() * diff + min);
  }
  return dataPoints;
};

const generateSequentialData = (num, min=0, max=100) => {
  const diff = max - min;
  const dataPoints = [];
  for (let i = 0; i < num; i++) {
    dataPoints.push(min + diff * (i / num));
  }
  return dataPoints;
};

export default Ember.Controller.extend({

  series: Ember.computed(
    'model.data',
    function() {
      console.log('series()');

      const data = this.get('model.data') || {};

      console.log('data', data);

      const series = {};

      Object.keys(data).forEach(range =>
        Object.keys(data[range]).filter(s => s != 'timestamp').forEach(mid => {
          const sid = range + '-' + mid;
          series[sid] = {
            timestamps: data[range]['timestamp'],
            values: data[range][mid],
            type: 'line',
            axis: 'y'
          };
      }));

      console.log('series', series);
      return series;
    }
  ),

  actions: {
    addSeries() {
      console.log('addSeries()');

      const data = this.get('model.data');

      data['myrange'] = {
        "a": generateRandomData(100, 0, 20000),
        "timestamp": generateSequentialData(100, 1508454000000, 1508543940000)
      };

      console.log('data', data);

      this.set('model.data', data);
      this.notifyPropertyChange('model');
    },

    removeSeries() {
      console.log('removeSeries()');

      const data = this.get('model.data');

      delete data['myrange'];

      console.log('data', data);

      this.set('model.data', data);
      this.notifyPropertyChange('model');
    }
  }

});
