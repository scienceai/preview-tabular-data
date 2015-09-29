var util = require('util')
  , stream = require('stream');

function ObjectStreamTruncator(options) {
  options = options || {};
  this.nHead = (options.nHead == null || options.nHead <0) ? Infinity: options.nHead;
  this.nSample = (options.nSample == null || options.nSample <0) ? Infinity: options.nSample;

  stream.Transform.call(this, { objectMode : true });

  this.head = [];
  this.sampled = 0;
};

util.inherits(ObjectStreamTruncator, stream.Transform);

ObjectStreamTruncator.prototype._transform = function(row, encoding, done) {

  if (this.sampled < this.nSample) {
    this.sampled++;

    if (this.head.length < this.nHead) {
      this.head.push(row);
    }

    this.push(row);
    if (this.sampled >= this.nSample) {
      this.end();
    }
  }

  done();
};


/**
 * Don't even bother transforming if we're over the limit
 * Just return false.
 */
ObjectStreamTruncator.prototype.write = function(chunk, encoding, cb) {
  var ret;

  if (this.sampled >= this.nSample)
    ret = false;
  else
    ret = stream.Transform.prototype.write.apply(this, arguments);

  return ret;
};


module.exports = ObjectStreamTruncator;
