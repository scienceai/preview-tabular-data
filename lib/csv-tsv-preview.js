var util = require('util')
  , stream = require('stream');

function CsvTsvPreview(parser, options) {
  options = options || {};
  this.nPreview = options.nPreview || 50;
  this.nSample = options.nSample || options.nPreview;

  stream.Transform.call(this, options);

  this._writableState.objectMode = false;
  this._readableState.objectMode = true;

  this.parser = parser;
  this.preview = [];


  this.sampled = 0;
};

util.inherits(CsvTsvPreview, stream.Transform);

CsvTsvPreview.prototype._transform = function(chunk, encoding, done){

  if(this.sampled < this.nSample){

    var row = this.parser.line(chunk).map(function(cell){
      return this.parser.cell(cell).toString();
    }, this);

    this.sampled++;

    if(this.preview.length < this.nPreview){
      this.preview.push(row);
    }

    if(this.nSample === 1){
      this.end();
    }

    if(this.preview.length > 1){
      var obj = {};
      for(var i=0; i<row.length; i++){
        obj[this.preview[0][i]] = row[i];
      }

      this.push(obj);

      if(this.sampled >= this.nSample){
        this.end();
      }

    }
  }

  done();
};


/**
 * Don't even both transforming if we're over the limit
 * Just return false.
 */
CsvTsvPreview.prototype.write = function (chunk, encoding, cb) {
  var ret

  if (this.sampled >= this.nSample)
    ret = false;
  else
    ret = stream.Transform.prototype.write.apply (this, arguments);

  return ret;
};


module.exports = CsvTsvPreview;
