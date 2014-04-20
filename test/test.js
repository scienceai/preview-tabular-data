var util = require('util')
  , fs = require('fs')
  , path = require('path')
  , assert = require('assert')
  , preview = require('..').preview;

var root = path.dirname(__filename);

describe('preview', function(){

  it('should preview csv', function(done){
    fs.stat(path.join(root, 'fixtures', 'data.csv'), function(err, stats){
      preview(fs.createReadStream(path.join(root, 'fixtures', 'data.csv')), { 'content-type': 'text/csv', 'content-length': stats.size }, {nPreview: 2}, function(err, preview, about){
        assert.equal(typeof about, 'undefined');
        assert.deepEqual(preview, [["a","b","c"], [1,2,3]]);
        done();
      });
    });
  });

  it('should preview csv and give a about context', function(done){
    fs.stat(path.join(root, 'fixtures', 'data.csv'), function(err, stats){
      preview(fs.createReadStream(path.join(root, 'fixtures', 'data.csv')), { 'content-type': 'text/csv', 'content-length': stats.size }, {nPreview: 2, nSample:10}, function(err, preview, about){
        assert.deepEqual(preview, [["a","b","c"], [1,2,3]]);
        assert.deepEqual(about, [ { name: 'a', valueType: 'xsd:integer' }, { name: 'b', valueType: 'xsd:integer' }, { name: 'c', valueType: 'xsd:integer' } ]);
        done();
      });
    });
  });


  it('should preview xlsx and generate about context', function(done){
    fs.stat(path.join(root, 'fixtures', 'data.xlsx'), function(err, stats){
      preview(fs.createReadStream(path.join(root, 'fixtures', 'data.xlsx')), { 'content-type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'content-length': stats.size }, {nPreview: 2, nSample: 100}, function(err, preview, about){
        assert.deepEqual(preview, [["Non-wrinkled","Wrinkled"],["1.00","1.25"]] );
        assert.deepEqual(about, [{ name: 'Non-wrinkled', valueType: 'xsd:double' }, { name: 'Wrinkled', valueType: 'xsd:double' }]);
        done();
      });
    });
  });

  it('should preview xls', function(done){
    fs.stat(path.join(root, 'fixtures', 'data.xls'), function(err, stats){
      preview(fs.createReadStream(path.join(root, 'fixtures', 'data.xls')), { 'content-type': 'application/vnd.ms-excel', 'content-length': stats.size }, {nPreview: 2}, function(err, preview, about){
        assert.equal(typeof about, 'undefined');
        assert.deepEqual(preview[0][0], 'Source Category Code' );
        done();
      });
    });
  });


  it('should preview ldjson', function(done){
    fs.stat(path.join(root, 'fixtures', 'data.ldjson'), function(err, stats){
      preview(fs.createReadStream(path.join(root, 'fixtures', 'data.ldjson')), { 'content-type': 'application/x-ldjson', 'content-length': stats.size }, {nPreview: 2}, function(err, preview, about){
        assert.equal(typeof about, 'undefined');
        assert.deepEqual(preview, [{"a":1,"b":"a string","c":41}, {"a":2,"b":"a string","c":42}]);
        done();
      });
    });
  });

  it('should preview ldjson and generate about context', function(done){
    fs.stat(path.join(root, 'fixtures', 'data.ldjson'), function(err, stats){
      preview(fs.createReadStream(path.join(root, 'fixtures', 'data.ldjson')), { 'content-type': 'application/x-ldjson', 'content-length': stats.size }, {nPreview: 2, nSample: 10}, function(err, preview, about){
        assert.deepEqual(preview, [{"a":1,"b":"a string","c":41}, {"a":2,"b":"a string","c":42}]);
        assert.deepEqual(about, [ { name: 'a', valueType: 'xsd:integer' }, { name: 'b', valueType: 'xsd:string' }, { name: 'c', valueType: 'xsd:integer' } ]);
        done();
      });
    });
  });

});
