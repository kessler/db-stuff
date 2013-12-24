module.exports = function (start, end) {
    var range = end - start;
    return Math.floor((Math.random() * range) + start);
};