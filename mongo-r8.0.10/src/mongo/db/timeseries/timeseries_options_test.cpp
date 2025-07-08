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

#include <string>
#include <tuple>
#include <vector>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/time_parsers.hpp>
#include <boost/move/utility_core.hpp>

#include "mongo/base/string_data.h"
#include "mongo/db/timeseries/timeseries_options.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/framework.h"
#include "mongo/util/time_support.h"


namespace mongo {

auto createTimeseriesOptionsWithGranularity(BucketGranularityEnum granularity) {
    auto options = TimeseriesOptions{};
    options.setGranularity(granularity);
    return options;
}

TEST(TimeseriesOptionsTest, RoundTimestampToGranularity) {
    TimeseriesOptions optionsSeconds =
        createTimeseriesOptionsWithGranularity(BucketGranularityEnum::Seconds);
    TimeseriesOptions optionsMinutes =
        createTimeseriesOptionsWithGranularity(BucketGranularityEnum::Minutes);
    TimeseriesOptions optionsHours =
        createTimeseriesOptionsWithGranularity(BucketGranularityEnum::Hours);
    std::vector<std::tuple<TimeseriesOptions, std::string, std::string>> testCases{
        {optionsSeconds, "2021-01-01T00:00:15.555Z", "2021-01-01T00:00:00.000Z"},
        {optionsSeconds, "2021-01-01T00:00:30.555Z", "2021-01-01T00:00:00.000Z"},
        {optionsSeconds, "2021-01-01T00:00:45.555Z", "2021-01-01T00:00:00.000Z"},

        {optionsMinutes, "2021-01-01T00:15:00.000Z", "2021-01-01T00:00:00.000Z"},
        {optionsMinutes, "2021-01-01T00:30:00.000Z", "2021-01-01T00:00:00.000Z"},
        {optionsMinutes, "2021-01-01T00:45:00.000Z", "2021-01-01T00:00:00.000Z"},

        {optionsHours, "2021-01-01T06:00:00.000Z", "2021-01-01T00:00:00.000Z"},
        {optionsHours, "2021-01-01T12:00:00.000Z", "2021-01-01T00:00:00.000Z"},
        {optionsHours, "2021-01-01T18:00:00.000Z", "2021-01-01T00:00:00.000Z"},
    };

    for (const auto& [granularity, input, expectedOutput] : testCases) {
        auto inputDate = dateFromISOString(input);
        ASSERT_OK(inputDate);
        auto roundedDate =
            timeseries::roundTimestampToGranularity(inputDate.getValue(), granularity);
        ASSERT_EQ(dateToISOStringUTC(roundedDate), expectedOutput);
    }
}

TEST(TimeseriesOptionsTest, RoundTimestampBySeconds) {
    std::vector<std::tuple<long long, std::string, std::string>> testCases{
        {60l, "2024-08-08T00:00:00.000Z", "2024-08-08T00:00:00.000Z"},
        {60l, "2024-08-08T00:00:00.001Z", "2024-08-08T00:00:00.000Z"},
        {60l, "2024-08-08T00:00:15.555Z", "2024-08-08T00:00:00.000Z"},
        {60l, "2024-08-08T00:00:30.555Z", "2024-08-08T00:00:00.000Z"},
        {60l, "2024-08-08T00:00:45.555Z", "2024-08-08T00:00:00.000Z"},
        {60l, "2024-08-08T00:00:59.999Z", "2024-08-08T00:00:00.000Z"},

        {60l, "2024-08-08T05:04:00.000Z", "2024-08-08T05:04:00.000Z"},
        {60l, "2024-08-08T05:04:00.001Z", "2024-08-08T05:04:00.000Z"},
        {60l, "2024-08-08T05:04:15.555Z", "2024-08-08T05:04:00.000Z"},
        {60l, "2024-08-08T05:04:30.555Z", "2024-08-08T05:04:00.000Z"},
        {60l, "2024-08-08T05:04:45.555Z", "2024-08-08T05:04:00.000Z"},
        {60l, "2024-08-08T05:04:59.999Z", "2024-08-08T05:04:00.000Z"},

        {3600l, "2024-08-08T00:00:00.000Z", "2024-08-08T00:00:00.000Z"},
        {3600l, "2024-08-08T00:00:00.001Z", "2024-08-08T00:00:00.000Z"},
        {3600l, "2024-08-08T00:15:00.000Z", "2024-08-08T00:00:00.000Z"},
        {3600l, "2024-08-08T00:30:00.000Z", "2024-08-08T00:00:00.000Z"},
        {3600l, "2024-08-08T00:45:00.000Z", "2024-08-08T00:00:00.000Z"},
        {3600l, "2024-08-08T00:59:59.999Z", "2024-08-08T00:00:00.000Z"},

        {86400l, "2024-08-08T00:00:00.000Z", "2024-08-08T00:00:00.000Z"},
        {86400l, "2024-08-08T00:00:00.001Z", "2024-08-08T00:00:00.000Z"},
        {86400l, "2024-08-08T06:00:00.000Z", "2024-08-08T00:00:00.000Z"},
        {86400l, "2024-08-08T12:00:00.000Z", "2024-08-08T00:00:00.000Z"},
        {86400l, "2024-08-08T18:00:00.000Z", "2024-08-08T00:00:00.000Z"},
        {86400l, "2024-08-08T23:59:59.999Z", "2024-08-08T00:00:00.000Z"},
    };

    for (const auto& [roundingSeconds, input, expectedOutput] : testCases) {
        auto inputDate = dateFromISOString(input);
        ASSERT_OK(inputDate);
        auto roundedDate =
            timeseries::roundTimestampBySeconds(inputDate.getValue(), roundingSeconds);
        ASSERT_EQ(dateToISOStringUTC(roundedDate), expectedOutput);
    }
}

TEST(TimeseriesOptionsTest, ExtendedRangeRoundTimestamp) {
    std::vector<std::tuple<long long, std::string, std::string>> testCases{
        {60l, "1901-01-01T00:00:12.345", "1901-01-01T00:00:00"},
        {60l, "1901-01-01T00:04:12.345", "1901-01-01T00:04:00"},
        {60l, "1901-01-01T02:04:12.345", "1901-01-01T02:04:00"},

        {60l, "1969-01-01T00:00:12.345", "1969-01-01T00:00:00"},
        {60l, "1969-01-01T00:04:12.345", "1969-01-01T00:04:00"},
        {60l, "1969-01-01T02:04:12.345", "1969-01-01T02:04:00"},

        {60l, "2040-01-01T00:00:12.345", "2040-01-01T00:00:00"},
        {60l, "2040-01-01T00:04:12.345", "2040-01-01T00:04:00"},
        {60l, "2040-01-01T02:04:12.345", "2040-01-01T02:04:00"},

        {60l, "2108-01-01T00:00:12.345", "2108-01-01T00:00:00"},
        {60l, "2108-01-01T00:04:12.345", "2108-01-01T00:04:00"},
        {60l, "2108-01-01T02:04:12.345", "2108-01-01T02:04:00"},

        {3600l, "1901-01-01T00:00:12.345", "1901-01-01T00:00:00"},
        {3600l, "1901-01-01T00:04:12.345", "1901-01-01T00:00:00"},
        {3600l, "1901-01-01T02:04:12.345", "1901-01-01T02:00:00"},

        {3600l, "1969-01-01T00:00:12.345", "1969-01-01T00:00:00"},
        {3600l, "1969-01-01T00:04:12.345", "1969-01-01T00:00:00"},
        {3600l, "1969-01-01T02:04:12.345", "1969-01-01T02:00:00"},

        {3600l, "2040-01-01T00:00:12.345", "2040-01-01T00:00:00"},
        {3600l, "2040-01-01T00:04:12.345", "2040-01-01T00:00:00"},
        {3600l, "2040-01-01T02:04:12.345", "2040-01-01T02:00:00"},

        {3600l, "2108-01-01T00:00:12.345", "2108-01-01T00:00:00"},
        {3600l, "2108-01-01T00:04:12.345", "2108-01-01T00:00:00"},
        {3600l, "2108-01-01T02:04:12.345", "2108-01-01T02:00:00"},

        {86400l, "1901-01-01T00:00:12.345", "1901-01-01T00:00:00"},
        {86400l, "1901-01-01T00:04:12.345", "1901-01-01T00:00:00"},
        {86400l, "1901-01-01T02:04:12.345", "1901-01-01T00:00:00"},

        {86400l, "1969-01-01T00:00:12.345", "1969-01-01T00:00:00"},
        {86400l, "1969-01-01T00:04:12.345", "1969-01-01T00:00:00"},
        {86400l, "1969-01-01T02:04:12.345", "1969-01-01T00:00:00"},

        {86400l, "2040-01-01T00:00:12.345", "2040-01-01T00:00:00"},
        {86400l, "2040-01-01T00:04:12.345", "2040-01-01T00:00:00"},
        {86400l, "2040-01-01T02:04:12.345", "2040-01-01T00:00:00"},

        {86400l, "2108-01-01T00:00:12.345", "2108-01-01T00:00:00"},
        {86400l, "2108-01-01T00:04:12.345", "2108-01-01T00:00:00"},
        {86400l, "2108-01-01T02:04:12.345", "2108-01-01T00:00:00"},
    };

    // TODO SERVER-94228: Support ISO 8601 date parsing and formatting of dates prior to 1970.
    static constexpr auto epoch = boost::posix_time::ptime(boost::gregorian::date(1970, 1, 1));
    auto parse = [](const std::string& input) {
        auto ptime = boost::posix_time::from_iso_extended_string(input);
        return Date_t::fromMillisSinceEpoch((ptime - epoch).total_milliseconds());
    };
    auto format = [](Date_t date) {
        boost::posix_time::milliseconds ms(date.toMillisSinceEpoch());
        return boost::posix_time::to_iso_extended_string(epoch + ms);
    };

    for (const auto& [roundingSeconds, input, expectedOutput] : testCases) {
        Date_t inputDate = parse(input);
        auto roundedDate = timeseries::roundTimestampBySeconds(inputDate, roundingSeconds);
        // We should always round down
        ASSERT_LTE(roundedDate, inputDate);
        // The rounding amount should be less than the rounding seconds
        ASSERT_LT((inputDate - roundedDate).count(), roundingSeconds * 1000);
        // Ensure that we've rounded to an even number according to our rounding seconds
        ASSERT_EQ(durationCount<Seconds>(roundedDate.toDurationSinceEpoch()) % roundingSeconds, 0);
        // Validate the expected output
        ASSERT_EQ(format(roundedDate), expectedOutput);
    }
}

TEST(TimeseriesOptionsTest, ExtendedRoundMilliTimestampBySeconds) {
    std::vector<std::tuple<long long, Date_t, Date_t>> testCases{
        {60l, Date_t::fromMillisSinceEpoch(-1), Date_t::fromMillisSinceEpoch(-60000)},
        {60l, Date_t::fromMillisSinceEpoch(-1000), Date_t::fromMillisSinceEpoch(-60000)},
        {60l, Date_t::fromMillisSinceEpoch(-1001), Date_t::fromMillisSinceEpoch(-60000)},
        {60l, Date_t::fromMillisSinceEpoch(-60000), Date_t::fromMillisSinceEpoch(-60000)},
        {60l, Date_t::fromMillisSinceEpoch(-60001), Date_t::fromMillisSinceEpoch(-120000)},
        {60l, Date_t::min(), Date_t::min()},

        {3600l, Date_t::fromMillisSinceEpoch(-1), Date_t::fromMillisSinceEpoch(-3600000)},
        {3600l, Date_t::fromMillisSinceEpoch(-1000), Date_t::fromMillisSinceEpoch(-3600000)},
        {3600l, Date_t::fromMillisSinceEpoch(-1001), Date_t::fromMillisSinceEpoch(-3600000)},
        {3600l, Date_t::fromMillisSinceEpoch(-3600000), Date_t::fromMillisSinceEpoch(-3600000)},
        {3600l, Date_t::fromMillisSinceEpoch(-3600001), Date_t::fromMillisSinceEpoch(-7200000)},
        {3600l, Date_t::min(), Date_t::min()},

        {86400l, Date_t::fromMillisSinceEpoch(-1), Date_t::fromMillisSinceEpoch(-86400000)},
        {86400l, Date_t::fromMillisSinceEpoch(-1000), Date_t::fromMillisSinceEpoch(-86400000)},
        {86400l, Date_t::fromMillisSinceEpoch(-1001), Date_t::fromMillisSinceEpoch(-86400000)},
        {86400l, Date_t::fromMillisSinceEpoch(-86400000), Date_t::fromMillisSinceEpoch(-86400000)},
        {86400l, Date_t::fromMillisSinceEpoch(-86400001), Date_t::fromMillisSinceEpoch(-172800000)},
        {86400l, Date_t::min(), Date_t::min()},
    };

    for (const auto& [roundingSeconds, input, expectedOutput] : testCases) {
        auto roundedDate = timeseries::roundTimestampBySeconds(input, roundingSeconds);
        ASSERT_EQ(roundedDate, expectedOutput);
    }
}

}  // namespace mongo
