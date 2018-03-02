import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';
import { module, test } from 'qunit';

module('Unit | Utility | float to percent');

// Replace this with your real tests.
test('it works', function(assert) {
  assert.equal(floatToPercent(0), '0.00%');
  assert.equal(floatToPercent(0.55), '55.00%');
  assert.equal(floatToPercent(0.123456), '12.35%');
});
