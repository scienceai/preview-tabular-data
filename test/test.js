var util = require('util')
  , fs = require('fs')
  , path = require('path')
  , assert = require('assert')
  , preview = require('..').preview;

var root = path.dirname(__filename);

describe('preview', function(){

  it('should preview csv', function(done){
    fs.stat(path.join(root, 'fixtures', 'data.csv'), function(err, stats){
      preview(fs.createReadStream(path.join(root, 'fixtures', 'data.csv')), 'text/csv', stats.size, {nPreview: 2}, function(err, preview){
        assert.deepEqual(preview, [["a","b","c"], [1,2,3]]);
        done();
      });
    });
  });

  it('should preview xlsx', function(done){
    fs.stat(path.join(root, 'fixtures', 'data.xlsx'), function(err, stats){
      preview(fs.createReadStream(path.join(root, 'fixtures', 'data.xlsx')), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', stats.size, {nPreview: 2}, function(err, preview){
        assert.deepEqual(preview, [["Non-wrinkled","Wrinkled"],["1.00","1.25"]] );
        done();
      });
    });
  });

  it('should preview xls', function(done){
    fs.stat(path.join(root, 'fixtures', 'data.xls'), function(err, stats){
      preview(fs.createReadStream(path.join(root, 'fixtures', 'data.xls')), 'application/vnd.ms-excel', stats.size, {nPreview: 2}, function(err, preview){
        assert.deepEqual(preview[0][0], '1962' );
        done();
      });
    });
  });

  it('should preview ldjson', function(done){
    fs.stat(path.join(root, 'fixtures', 'data.ldjson'), function(err, stats){
      preview(fs.createReadStream(path.join(root, 'fixtures', 'data.ldjson')), 'application/x-ldjson', stats.size, {nPreview: 2}, function(err, preview){
        assert.deepEqual(preview, [{"a":1,"b":3,"c":41}, {"a":2,"b":3,"c":42}]);
        done();
      });
    });
  });

});
