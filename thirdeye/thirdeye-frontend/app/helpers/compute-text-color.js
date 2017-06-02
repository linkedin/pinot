import Ember from 'ember';

/**
 * Template helper that computes the background color
 * for the contribution map
 * @param {Number} [value=0] Contribution change in percentage
 * @return {String}          Text color (HEX)
 */
export function computeTextColor([value = 0]) {
  const opacity = Math.abs(value/25);
  if (opacity < 0.5) {
    return "#000000";
  } else{
    return "#ffffff" ;
  }
}

export default Ember.Helper.helper(computeTextColor);
