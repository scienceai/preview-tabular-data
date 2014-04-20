var path = require('path')
  , binaryCSV = require('binary-csv')
  , util = require('util')
  , Trunc = require('truncating-stream')
  , stream = require('stream')
  , xlsx = require('xlsx')
  , xls = require('xlsjs')
  , once = require('once')
  , CsvTsvPreview = require('./lib/csv-tsv-preview')
  , split = require('split')
  , concat = require('concat-stream');

function previewCsvTsv(readable, contentType, contentLength, opts, callback){

  if(arguments.length === 4){
    callback = opts;
    opts = {};
  }

  callback = once(callback);

  var nPreview = opts.nPreview || 10;

  var parser = binaryCSV({separator: (contentType === 'text/csv') ? ',': '\t'});
  var p = new CsvTsvPreview(parser, {nPreview: nPreview});

  var s =  readable.pipe(parser).pipe(p);
  s.on('error', callback);
  s.on('finish', function(){
    try{
      readable.end();
      readable.destroy();
    } catch(e){}

    callback(null, s.preview);
  });

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
      var ldjson = parser.utils.sheet_to_row_object_array(sheet);
    } catch(e){
      return callback(e);
    }

    var preview = [ Object.keys(ldjson[0]) ];
    for(var i=0, l = Math.min(ldjson.length, nPreview); i<(l-1); i++){
      var arr = [] //push only hasOwnproperty
      var row = ldjson[i];

      preview[0].forEach(function(key){
        arr.push(row[key]);
      });
      preview.push(arr);
    }


    callback(null, preview);

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

  if(contentLength > maxSize){
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
      return JSON.parse(row);
    }
  }));

  s.on('data', function(data){
    if(preview.length < nPreview){
      preview.push(data);
    }
  });
  s.on('error', callback);
  s.on('end', function(){
    callback(null, preview);
  });

};


function preview(readable, contentType, contentLength, opts, callback){

  if(contentType === 'application/x-ldjson'){
    previewLdJson(readable, contentType, contentLength, opts, callback);
  } else if (contentType === 'application/vnd.ms-excel' || contentType === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'){
    previewXls(readable, contentType, contentLength, opts, callback);
  } else if(contentType === 'text/csv' || contentType === 'text/tab-separated-values' ){
    previewCsvTsv(readable, contentType, contentLength, opts, callback);
  } else {
    callback(new Error('no preview available for ' + contentType));
  }

};

exports.preview = preview;
exports.csvTsv = previewCsvTsv;
exports.ldjson = previewLdJson;
exports.xls = previewXls;
