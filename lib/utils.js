// parse sort string, e.g. '-value'
exports.parseSort = function(sortString) {
  if (!sortString) return {};
  var sortProp = sortString;
  var sortOrder = 1;
  if (sortProp && sortProp[0] === '-') {
    sortOrder = -1;
    sortProp = sortProp.substr(1);
  }
  return {
    sortProp: sortProp,
    sortOrder: sortOrder
  };
};
