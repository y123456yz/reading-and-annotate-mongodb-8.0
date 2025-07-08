/*
 * Test the $_internalListCollections stage by comparing its output with the listCollections command
 * response.
 *
 * @tags: [
 *    requires_fcv_80,
 *    # $_internalListCollections only supports local read concern
 *    assumes_read_concern_unchanged,
 *    # There is no need to support multitenancy, as it has been canceled and was never in
 *    # production (see SERVER-97215 for more information)
 *    command_not_supported_in_serverless,
 * ]
 */

import {FixtureHelpers} from "jstests/libs/fixture_helpers.js";

const dbTest1 = db.getSiblingDB(jsTestName() + "1");
const dbTest2 = db.getSiblingDB(jsTestName() + "2");
const adminDB = db.getSiblingDB("admin");
const configDB = db.getSiblingDB("config");

const collectionPlacementIsUnstable =
    (TestData.runningWithBalancer === true || TestData.shardsAddedRemoved);

function removeUuidField(listOfCollections) {
    let listResult = listOfCollections.map((entry) => {
        const {info: {uuid: uuid, ...infoWithoutUuid}, ...entryWithoutInfo} = entry;
        return {info: infoWithoutUuid, ...entryWithoutInfo};
    });
    return listResult;
}

function removePrimaryField(listOfCollections) {
    let listResult = listOfCollections.map((entry) => {
        const {primary: primary, ...entryWithoutPrimary} = entry;
        return {...entryWithoutPrimary};
    });
    return listResult;
}

function compareInternalListCollectionsStageAgainstListCollections(dbTest,
                                                                   expectedNumOfCollections) {
    // Fetch all the collections for the `dbTest` using the `listCollections` command and transform
    // them to the same format used by `$_internalListCollections`.
    let listCollectionsResponse = dbTest.getCollectionInfos().map(entry => {
        const {name: name, ...entryWithoutName} = entry;
        return {ns: dbTest.getName() + "." + name, db: dbTest.getName(), ...entryWithoutName};
    });
    if (collectionPlacementIsUnstable) {
        // The uuid may change if there are moveCollection operations on the background, therefore
        // we don't check it.
        listCollectionsResponse = removeUuidField(listCollectionsResponse);

        // There may exist temporal collection called '<db>.system.resharding.*' or
        // '<db>.system.buckets.resharding.*' if there are moveCollection operations on the
        // background. Remove them to pass the following checks.
        listCollectionsResponse = listCollectionsResponse.filter((collEntry) => {
            return !collEntry['ns'].includes("resharding");
        });
    }
    assert.eq(expectedNumOfCollections, listCollectionsResponse.length, listCollectionsResponse);

    // Check that all the collections returned by `listCollections` are also returned by
    // `$_internalListCollections`.
    let internalStageResponseAgainstDbTest =
        dbTest.aggregate([{$_internalListCollections: {}}, {$match: {ns: {$not: /resharding/}}}])
            .toArray();
    assert.eq(expectedNumOfCollections,
              internalStageResponseAgainstDbTest.length,
              internalStageResponseAgainstDbTest);
    if (collectionPlacementIsUnstable) {
        // The uuid may change if there are moveCollection operations on the background, therefore
        // we don't check it.
        internalStageResponseAgainstDbTest = removeUuidField(internalStageResponseAgainstDbTest);
    }
    internalStageResponseAgainstDbTest = removePrimaryField(internalStageResponseAgainstDbTest);

    assert.sameMembers(
        listCollectionsResponse,
        internalStageResponseAgainstDbTest,
        "listCollectionsResponse: " + tojson(listCollectionsResponse) +
            ", $_internalListCollectionsResponse: " + tojson(internalStageResponseAgainstDbTest));

    // Check that the collections returned by listCollections are also returned by
    // $_internalListCollections when it runs against the 'admin' db.
    let stageResponseAgainstAdminDb =
        dbTest.getSiblingDB("admin").aggregate([{$_internalListCollections: {}}]).toArray();
    if (collectionPlacementIsUnstable) {
        stageResponseAgainstAdminDb = removeUuidField(stageResponseAgainstAdminDb);
    }
    stageResponseAgainstAdminDb = removePrimaryField(stageResponseAgainstAdminDb);

    listCollectionsResponse.forEach((entry) => {
        assert.contains(entry,
                        stageResponseAgainstAdminDb,
                        "The listCollections entry " + tojson(entry) +
                            " hasn't been found on the $_internalListCollections output " +
                            tojson(stageResponseAgainstAdminDb));
    });
}

function runTestOnDb(dbTest) {
    jsTestLog("Going to run the test on " + dbTest.getName());

    let numCollections = 0;

    // Non-existing db
    compareInternalListCollectionsStageAgainstListCollections(dbTest, numCollections);
    compareInternalListCollectionsStageAgainstListCollections(db.getSiblingDB("non-exising-db"),
                                                              numCollections);

    // Unsharded standard collection
    assert.commandWorked(dbTest.createCollection("coll1"));
    compareInternalListCollectionsStageAgainstListCollections(dbTest, ++numCollections);

    assert.commandWorked(dbTest.createCollection("coll2"));
    compareInternalListCollectionsStageAgainstListCollections(dbTest, ++numCollections);

    // Views
    assert.commandWorked(dbTest.createView("view1", "coll1", []));
    ++numCollections;  // because of `<db>.system.views`
    compareInternalListCollectionsStageAgainstListCollections(dbTest, ++numCollections);

    assert.commandWorked(dbTest.createView("view2", "coll2", []));
    compareInternalListCollectionsStageAgainstListCollections(dbTest, ++numCollections);

    // Sharded collections
    if (FixtureHelpers.isMongos(dbTest)) {
        assert.commandWorked(dbTest.adminCommand(
            {shardCollection: dbTest.getName() + ".collSharded1", key: {x: 1}}));
        compareInternalListCollectionsStageAgainstListCollections(dbTest, ++numCollections);

        assert.commandWorked(dbTest.adminCommand(
            {shardCollection: dbTest.getName() + ".collSharded2", key: {x: 1}}));
        compareInternalListCollectionsStageAgainstListCollections(dbTest, ++numCollections);

        // Timeseries-sharded collection
        assert.commandWorked(dbTest.adminCommand({
            shardCollection: dbTest.getName() + ".collShardedTim1",
            key: {t: 1},
            timeseries: {timeField: "t"}
        }));
        // We'll see two collections per every timeseries created collection: the main one
        // and the buckets collection called `<db>.system.timeseries.<collName>`
        numCollections += 2;
        compareInternalListCollectionsStageAgainstListCollections(dbTest, numCollections);
    }

    // Timeseries collections
    assert.commandWorked(dbTest.createCollection("collTim1", {timeseries: {timeField: "t"}}));
    numCollections += 2;
    compareInternalListCollectionsStageAgainstListCollections(dbTest, numCollections);

    assert.commandWorked(dbTest.createCollection("collTim2", {timeseries: {timeField: "t"}}));
    numCollections += 2;
    compareInternalListCollectionsStageAgainstListCollections(dbTest, numCollections);
}

function runInternalCollectionsTest(dbTest) {
    // Check that $_internalListCollections returns collections for "admin" db.
    assert.soon(
        () => {
            const adminCollsListCollections = adminDB.getCollectionInfos();
            const adminCollsInternalListCollections =
                adminDB.aggregate([{$_internalListCollections: {}}, {$match: {db: "admin"}}])
                    .toArray();

            if (adminCollsListCollections.length != adminCollsInternalListCollections.length) {
                jsTestLog(
                    "The collections of the 'admin' db returned by listCollections don't match " +
                    "with the collections returned by $_internalListCollections. listCollections " +
                    "response: " + tojson(adminCollsListCollections) +
                    ", $_internalListCollections response: " +
                    tojson(adminCollsInternalListCollections) +
                    ". Going to retry the comparison check.");
                return false;
            }
            return true;
        },
        "The collections of the 'admin' db returned by listCollections don't match with the " +
            "collections returned by $_internalListCollections.");

    // Check that $_internalListCollections returns "config" collections if called against "admin"
    // db.
    assert.soon(
        () => {
            const configCollsListCollections = configDB.getCollectionInfos();
            const configCollsInternalListCollections =
                adminDB.aggregate([{$_internalListCollections: {}}, {$match: {db: "config"}}])
                    .toArray();

            if (configCollsListCollections.length != configCollsInternalListCollections.length) {
                jsTestLog(
                    "The collections of the 'config' db returned by listCollections don't match " +
                    "with the collections returned by $_internalListCollections when targeting " +
                    "the 'admin' database. listCollections response: " +
                    tojson(configCollsListCollections) + ", $_internalListCollections response: " +
                    tojson(configCollsInternalListCollections) +
                    ". Going to retry the comparison check.");
                return false;
            }
            return true;
        },
        "The collections of the 'config' db returned by listCollections don't match with the " +
            "collections returned by $_internalListCollections when targeting the 'admin' database.");

    // Check that $_internalListCollections returns "config" collections if called against "config"
    // db.
    assert.soon(
        () => {
            const configCollsListCollections = configDB.getCollectionInfos();
            const configCollsInternalListCollections =
                configDB.aggregate([{$_internalListCollections: {}}]).toArray();

            if (configCollsListCollections.length != configCollsInternalListCollections.length) {
                jsTestLog(
                    "The collections of the 'config' db returned by listCollections don't match " +
                    "with the collections returned by $_internalListCollections when targeting " +
                    "the 'config' database. listCollections response: " +
                    tojson(configCollsListCollections) + ", $_internalListCollections response: " +
                    tojson(configCollsInternalListCollections) +
                    ". Going to retry the comparison check.");
                return false;
            }
            return true;
        },
        "The collections of the 'config' db returned by listCollections don't match with the " +
            "collections returned by $_internalListCollections when targeting the 'config' database.");
}

assert.commandWorked(dbTest1.dropDatabase());
assert.commandWorked(dbTest2.dropDatabase());

runTestOnDb(dbTest1);
runTestOnDb(dbTest2);

runInternalCollectionsTest(dbTest1);
