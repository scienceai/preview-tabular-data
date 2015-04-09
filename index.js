var path = require('path')
  , csvParser = require('csv-parser')
  , util = require('util')
  , zlib = require('zlib')
  , stream = require('stream')
  , xlsx = require('xlsx')
  , once = require('once')
  , ObjectStreamTruncator = require('./lib/object-stream-truncator')
  , jsonLdContextInfer = require('jsonld-context-infer')
  , split2 = require('split2')
  , concat = require('concat-stream');

function csvTsvHead(readable, contentType, opts, callback){

  if (!callback) {
    callback = opts;
    opts = {};
  }

  callback = once(callback);

  var headers;

  var parser = csvParser({separator: (contentType === 'text/csv') ? ',' : '\t' });

  var sampler = new ObjectStreamTruncator(opts);

  var s =  readable
        .pipe(parser)
        .pipe(sampler)
        .on('error', callback)
        .on('headers', function(data) {
          console.log(data);
          headers = data;
        })
        .on('finish', function() {
          try {
            readable.end();
            readable.destroy();
          } catch(e){}
        });

  jsonLdContextInfer(s, {nSample: opts.nSample || Infinity}, function(err, schema, scores){
    if (err) {
      return callback(err);
    }

    callback(null, s.head, jsonLdContextInfer.about(schema, headers));
  });

};

function xlsxHead(readable, contentType, opts, callback){

  if (!callback) {
    callback = opts;
    opts = {};
  }

  callback = once(callback);

  readable.pipe(concat(function(data){

    var workbook;

    try{
      workbook = xlsx.read(data.toString('binary'), {type: 'binary'});
    } catch(e){
      return callback(e);
    }

    if (!workbook) {
      return callback(new Error('could not parse spreadsheat'));
    }

    if (workbook.SheetNames.length>1) {
      console.error('multiple sheets in a workbook');
    }

    var sheet = workbook.Sheets[workbook.SheetNames[0]];

    try {
      var csv = xlsx.utils.sheet_to_csv(sheet);
    } catch(e){
      return callback(e);
    }

    var rs = new stream.Readable();
    rs.push(csv);
    rs.push(null);

    csvTsvHead(rs, 'text/csv', opts, callback);
  }));

};

function ndJsonHead(readable, contentType, opts, callback){

  if (!callback) {
    callback = opts;
    opts = {};
  }

  callback = once(callback);

  var parser = split2(JSON.parse);
  var sampler = new ObjectStreamTruncator(opts);

  var s = readable
        .pipe(parser)
        .pipe(sampler)
        .on('error', callback)
        .on('finish', function() {
          try {
            readable.end();
            readable.destroy();
          } catch(e){}
        });

  jsonLdContextInfer(s, {nSample: opts.nSample || Infinity}, function(err, schema, scores){
    if(err) return callback(err);
    callback(null, s.head, jsonLdContextInfer.about(schema));
  });

};


function head(readable, contentType, opts, callback){
  opts = opts || {nHead: Infinity, nSample: Infinity};

  contentType = contentType.split(';')[0].trim();

  if (contentType === 'application/x-ndjson') {
    ndJsonHead(readable, contentType, opts, callback);
  } else if (contentType === 'application/vnd.ms-excel' || contentType === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet') {
    xlsxHead(readable, contentType, opts, callback);
  } else if(contentType === 'text/csv' || contentType === 'text/tab-separated-values' ) {
    csvTsvHead(readable, contentType, opts, callback);
  } else {
    callback(new Error('no preview available for ' + contentType));
  }

};

exports.head = head;
exports.csvTsv = csvTsvHead;
exports.ndjson = ndJsonHead;
exports.xlsx = xlsxHead;
