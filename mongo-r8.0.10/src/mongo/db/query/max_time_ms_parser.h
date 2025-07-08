/**
 *    Copyright (C) 2021-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */


#pragma once

#include <cstdint>

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobj.h"

namespace mongo {

/**
 * Parses maxTimeMS from the BSONElement containing its value.
 *
 * Ensures the value is within the range [0, maxValue].
 */
StatusWith<int> parseMaxTimeMS(BSONElement maxTimeMSElt, long long maxValue = INT_MAX);

/**
 * Parses maxTimeMSOpOnly from the BSONElement containing its value.
 *
 * 'maxTimeMSOpOnly' needs a slightly higher max value than regular 'maxTimeMS' to account for
 * the case where a user provides the max possible value for 'maxTimeMS' to one server process
 * (mongod or mongos), then that server process passes the max time on to another server as
 * 'maxTimeMSOpOnly', but after adding a small amount to the max time to account for clock
 * precision.  This can push the 'maxTimeMSOpOnly' sent to the mongod over the max value allowed
 * for users to provide. This is safe because 'maxTimeMSOpOnly' is only allowed to be provided
 * for internal intra-cluster requests.
 *
 * Thus, this method ensures the value is within the range [0, INT_MAX+kMaxTimeMSOpOnlyMaxPadding]
 */
StatusWith<int> parseMaxTimeMSOpOnly(BSONElement maxTimeMSElt);

/**
 * Returns the provided, valid maxTimeMS or throws.
 */
int parseAndThrowMaxTimeMS(BSONElement maxTimeMSElt);

/**
 * Returns the provided, valid maxTimeMSOpOnly or throws.
 */
int parseAndThrowMaxTimeMSOpOnly(BSONElement maxTimeMSElt);

}  // namespace mongo
