var util = require('util')
  , fs = require('fs')
  , path = require('path')
  , assert = require('assert')
  , head = require('..').head;

var root = path.dirname(__filename);

describe('head', function(){

  it('should head csv and give a columns context', function(done){
    head(fs.createReadStream(path.join(root, 'fixtures', 'data.csv')), 'text/csv', {nHead: 2, nSample:10}, function(err, head, columns){
      assert.deepEqual(head,  [ { a: '1', b: '2', c: '3' }, { a: '4', b: '5', c: '6' } ]);
      assert.deepEqual(columns, [ { name: 'a', datatype: 'integer' }, { name: 'b', datatype: 'integer' }, { name: 'c', datatype: 'integer' } ]);
      done();
    });
  });


  it('should head xlsx and generate columns context', function(done){
    head(fs.createReadStream(path.join(root, 'fixtures', 'data.xlsx')), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', {nHead: 2, nSample: 100}, function(err, head, columns){
      assert.deepEqual(head, [{ 'Non-wrinkled': '1.00', Wrinkled: '1.25' }, { 'Non-wrinkled': '1.00', Wrinkled: '1.25' }]);
      assert.deepEqual(columns, [{ name: 'Non-wrinkled', datatype: 'double' }, { name: 'Wrinkled', datatype: 'double' }]);
      done();
    });
  });

  it('should head xls', function(done){
    head(fs.createReadStream(path.join(root, 'fixtures', 'data.xls')), 'application/vnd.ms-excel', {nHead: 1, nSample: Infinity}, function(err, head, columns) {
      assert.equal(head[0]['Source category name'], 'Individual Income Taxes' );
      done();
    });
  });


  it('should head ndjson and generate columns context', function(done){
    head(fs.createReadStream(path.join(root, 'fixtures', 'data.ndjson')), 'application/x-ndjson', {nHead: 2, nSample: 10}, function(err, head, columns){
      assert.deepEqual(head, [{"a":1,"b":"a string","c":41}, {"a":2,"b":"a string","c":42}]);
      assert.deepEqual(columns, [ { name: 'a', datatype: 'integer' }, { name: 'b', datatype: 'string' }, { name: 'c', datatype: 'integer' } ]);
      done();
    });
  });

  it("should give an error but not crash in case of rt mismatch", function(done){
    head(fs.createReadStream(path.join(root, 'fixtures', 'head.xls')), 'application/vnd.ms-excel', {nHead: 2}, function(err, head, columns){
      assert(err);
      done();
    });
  });

});
