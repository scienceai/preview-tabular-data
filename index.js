var path = require('path')
  , binaryCSV = require('binary-csv')
  , util = require('util')
  , Trunc = require('truncating-stream')
  , zlib = require('zlib')
  , stream = require('stream')
  , xlsx = require('xlsx')
  , xls = require('xlsjs')
  , once = require('once')
  , CsvTsvPreview = require('./lib/csv-tsv-preview')
  , jsonLdContextInfer = require('jsonld-context-infer')
  , split = require('split')
  , concat = require('concat-stream');

function previewCsvTsv(readable, contentType, contentLength, opts, callback){

  if(arguments.length === 4){
    callback = opts;
    opts = {};
  }

  callback = once(callback);

  var parser = binaryCSV({separator: (contentType === 'text/csv') ? ',': '\t'});
  var p = new CsvTsvPreview(parser, opts);

  var s =  readable.pipe(parser).pipe(p);
  s.on('error', callback);
  s.on('finish', function(){
    try{
      readable.end();
      readable.destroy();
    } catch(e){}

    if(!opts.nSample){
      callback(null, s.preview);
    }
  });

  if(opts.nSample){
    jsonLdContextInfer(s, {nSample: opts.nSample}, function(err, schema, scores){
      if(err){
        return  callback(err);
      }

      callback(null, s.preview, jsonLdContextInfer.about(schema, s.preview[0]));
    });
  }

};

function previewXls(readable, contentType, contentLength, opts, callback){

  if(arguments.length === 4){
    callback = opts;
    opts = {};
  }

  callback = once(callback);

  var nPreview = opts.nPreview || 10;

  readable.pipe(concat(function(data){

    var parser, workbook;
    if(contentType === 'application/vnd.ms-excel'){
      parser = xls;
      workbook = parser.read(data.toString('binary'), {type: 'binary'});
    } else {
      parser = xlsx;
      workbook = parser.read(data, {type: 'binary'});
    }

    if(workbook.SheetNames.length>1){
      console.error('multiple sheets in a workbook');
    }

    var sheet = workbook.Sheets[workbook.SheetNames[0]];

    try {
      var csv = parser.utils.sheet_to_csv(sheet);
    } catch(e){
      return callback(e);
    }


    var rs = new stream.Readable();
    rs.push(csv);
    rs.push(null);

    previewCsvTsv(rs, 'text/csv', Buffer.byteLength(csv), opts, callback);

  }));

};

function previewLdJson(readable, contentType, contentLength, opts, callback){

  if(arguments.length === 4){
    callback = opts;
    opts = {};
  }

  callback = once(callback);

  var maxSize = opts.maxSize || 1024000;
  var nPreview = opts.nPreview || 10;

  var s;

  if(!opts.nSample && (contentLength > maxSize)){
    var t = new Trunc({ limit : maxSize });
    s = readable.pipe(t);
    t.on('finish', function(){
      try{
        readable.end();
        readable.destroy();
      } catch(e){}
    });
  } else {
    s = readable;
  }

  var preview = [];

  s = s.pipe(split(function(row){
    if(row) {
      var prow = JSON.parse(row);
      if(preview.length < nPreview){
        preview.push(prow);
      }
      return prow;
    }
  }));

  s.on('error', callback);

  if(!opts.nSample){
    s.on('end', function(){
      callback(null, preview);
    });
  } else {
    jsonLdContextInfer(s, {nSample: opts.nSample}, function(err, schema, scores){
      if(err) return callback(err);
      callback(null, preview, jsonLdContextInfer.about(schema));
    });
  }
};


function preview(readable, headers, opts, callback){

  var encoding = headers['content-encoding'] || 'identity';
  var decompress, dataStream;

  if (encoding.match(/\bdeflate\b/)) {
    decompress = zlib.createInflate();
  } else if (encoding.match(/\bgzip\b/)) {
    decompress = zlib.createGunzip();
  }

  if (decompress) {
    dataStream = readable.pipe(decompress);
  } else {
    dataStream = readable;
  }

  if(headers['content-type'] === 'application/x-ldjson'){
    previewLdJson(dataStream, headers['content-type'], headers['content-length'], opts, callback);
  } else if (headers['content-type'] === 'application/vnd.ms-excel' || headers['content-type'] === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'){
    previewXls(dataStream, headers['content-type'], headers['content-length'], opts, callback);
  } else if(headers['content-type'] === 'text/csv' || headers['content-type'] === 'text/tab-separated-values' ){
    previewCsvTsv(dataStream, headers['content-type'], headers['content-length'], opts, callback);
  } else {
    callback(new Error('no preview available for ' + headers['content-type']));
  }

};

exports.preview = preview;
exports.csvTsv = previewCsvTsv;
exports.ldjson = previewLdJson;
exports.xls = previewXls;
