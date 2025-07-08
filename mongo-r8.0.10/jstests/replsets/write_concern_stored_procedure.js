/**
 * Tests running stored procedures do not wait for write concern (which would trigger assertion
 * while holding global lock) when it iterates system.js collection using DBDirectClient.
 */

const rst = new ReplSetTest({name: jsTestName(), nodes: 2});
rst.startSet();
rst.initiate();
const db = rst.getPrimary().getDB(jsTestName());

for (var i = 0; i < 3000; i++) {
    db.system.js.insertOne({
        _id: "test" + i,
        value: function(x) {
            return x;
        }
    });
}
assert.commandWorked(db.coll.insertOne({x: 1}));
assert.commandWorked(db.coll.mapReduce("function() { return test0(this.x); }" /* map */,
                                       "function(a, b) { return a + b; }" /* reduce */,
                                       {out: "coll_out"}));

rst.stopSet();
