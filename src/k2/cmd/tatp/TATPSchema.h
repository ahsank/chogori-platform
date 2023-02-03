/*
MIT License

Copyright(c) 2022 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#pragma once

#include <string>

#include <k2/common/Common.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/transport/Payload.h>
#include <k2/transport/PayloadSerialization.h>

#include "rand.h"
#include "Log.h"
using namespace k2;
static const String tatpCollectionName = "TATP";
constexpr unsigned maxSFIDPerSubs = 4;
constexpr unsigned minSFIDPerSubs = 1;
constexpr unsigned maxAInfoPerSubs = 4;
constexpr unsigned minAInfoPerSubs = 1;
constexpr unsigned minCFPerSF = 0;
constexpr unsigned maxCFPerSF = 3;

#define CHECK_READ_STATUS(read_result) \
    do { \
        if (!((read_result).status.is2xxOK())) { \
            K2LOG_D(log::tatp, "TATP failed to read rows: {}", (read_result).status); \
            return make_exception_future(std::runtime_error(String("TATP failed to read rows: ") + __FILE__ + ":" + std::to_string(__LINE__))); \
        } \
    } \
    while (0) \

#define CHECK_READ_STATUS_TYPE(read_result,type) \
    do { \
        if (!((read_result).status.is2xxOK())) { \
            K2LOG_D(log::tatp, "TATP failed to read rows: {}", (read_result).status); \
            return make_exception_future<type>(std::runtime_error(String("TATP failed to read rows: ") + __FILE__ + ":" + std::to_string(__LINE__))); \
        } \
    } \
    while (0) \

template<typename ValueType>
seastar::future<WriteResult> writeRow(ValueType& row, K2TxnHandle& txn, bool erase = false)
{
    return txn.write<ValueType>(row, erase).then([&row] (WriteResult&& result) {
        if (!result.status.is2xxOK()) {
            K2LOG_D(log::tatp, "writeRow failed: {}", result.status);
            return seastar::make_exception_future<WriteResult>(std::runtime_error("writeRow failed!"));
        }

        return seastar::make_ready_future<WriteResult>(std::move(result));
    });
}

template<typename ValueType, typename FieldType>
seastar::future<PartialUpdateResult>
partialUpdateRow(ValueType& row, FieldType fieldsToUpdate, K2TxnHandle& txn) {
    return txn.partialUpdate<ValueType>(row, fieldsToUpdate).then([] (PartialUpdateResult&& result) {
        if (!result.status.is2xxOK()) {
            K2LOG_D(log::tatp, "partialUpdateRow failed: {}", result.status);
            return seastar::make_exception_future<PartialUpdateResult>(std::runtime_error("partialUpdateRow failed!"));
        }

        return seastar::make_ready_future<PartialUpdateResult>(std::move(result));
    });
}


inline uint64_t getDate()
{
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

class Subscriber {
public:
    static inline dto::Schema subscriber_schema {
        .name = "subscriber",
        .version = 1,
        .fields = std::vector<dto::SchemaField> {
                {dto::FieldType::INT32T, "s_id", false, false},
                {dto::FieldType::STRING, "sub_nbr", false, false},
                {dto::FieldType::INT16T, "bits", false, false},
                {dto::FieldType::INT64T, "hexes", false, false},
                {dto::FieldType::INT32T, "msc_location", false, false},
                {dto::FieldType::INT32T, "vlr_location", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> {}
    };


    Subscriber(RandomContext& random, int32_t id) : sID(id) {
        auto str = std::to_string(uint32_t(id));
        // Pad leading 0 to make it 15 digit long
        subNBR = str.length() < 15 ? std::string(15 - str.length(), '0') + str : str;
        bits = random.template UniformRandom<int16_t>();
        hexes = random.template UniformRandom<int64_t>();
        mscLocation = random.template UniformRandom<int32_t>();
        vlrLocation = random.template UniformRandom<int32_t>();
    }

    Subscriber(int16_t id) : sID(id) {}
    Subscriber() = default;

    std::optional<int32_t> sID;
    std::optional<String> subNBR;
    std::optional<int16_t> bits;
    std::optional<int64_t> hexes;
    std::optional<int32_t> mscLocation;
    std::optional<int32_t> vlrLocation;

    static inline thread_local std::shared_ptr<dto::Schema> schema;
    static inline String collectionName = tatpCollectionName;
    
    static unsigned constexpr maxAccessPerSubscriber = 4; // Fixed by spec
    static unsigned constexpr maxFacilityPerSubscriber = 4; // Fixed by Spec

    SKV_RECORD_FIELDS(sID, subNBR, bits, hexes, mscLocation, vlrLocation);
};

class AccessInfo {
public:
    static inline dto::Schema access_info_schema {
        .name = "Access_Info",
        .version = 1,
        .fields = std::vector<dto::SchemaField> {
            {dto::FieldType::INT32T, "s_id", false, false},
            {dto::FieldType::INT16T, "ai_type", false, false}, // 1..4
            {dto::FieldType::INT16T, "data1", false, false},
            {dto::FieldType::INT16T, "data2", false, false},            
            {dto::FieldType::STRING, "data3", false, false},
            {dto::FieldType::STRING, "data4", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1 }
    };

    AccessInfo(RandomContext& random, int32_t sid, int16_t aitype) : s_id(sid), ai_type(aitype) {
        data1 = random.UniformRandom(0, 256);
        data2 = random.UniformRandom(0, 256);
        data3 = random.RandomString(3, 3, 'A', 'Z');
        data4 = random.RandomString(4, 4, 'A', 'Z');
    }

    AccessInfo(int32_t sid, int16_t aitype) : s_id(sid), ai_type(aitype) {}

    AccessInfo() = default;

    std::optional<int32_t> s_id;
    std::optional<int16_t> ai_type;
    std::optional<int16_t> data1;
    std::optional<int16_t> data2;
    std::optional<String> data3;
    std::optional<String> data4;

    static inline thread_local std::shared_ptr<dto::Schema> schema;
    static inline String collectionName = tatpCollectionName;

    SKV_RECORD_FIELDS(s_id, ai_type, data1, data2, data3, data4);
};

class SpecialFacility {
public:
    static inline dto::Schema special_facility_schema {
        .name = "Special_Facility",
        .version = 1,
        .fields = std::vector<dto::SchemaField> {
            {dto::FieldType::INT32T, "s_id", false, false},
            {dto::FieldType::INT16T, "sf_type", false, false}, // 1..4
            {dto::FieldType::INT16T, "is_active", false, false},
            {dto::FieldType::INT16T, "error_cntrl", false, false},            
            {dto::FieldType::INT16T, "data_a", false, false},
            {dto::FieldType::STRING, "data_b", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1 }
    };

    SpecialFacility(RandomContext& random, int32_t sid, int16_t sftype) : s_id(sid), sf_type(sftype) {
        int pct = random.UniformRandom(1, 100);
        is_active = pct <= 15 ? 0 : 1;
        error_cntrl = random.UniformRandom(0, 255);
        data_a = random.UniformRandom(0, 255);
        data_b = random.RandomString(5, 5, 'A', 'Z');
    }

    SpecialFacility(int32_t sid, int16_t sftype) : s_id(sid), sf_type(sftype) {}
    SpecialFacility() = default;

    std::optional<int32_t> s_id;
    std::optional<int16_t> sf_type;
    std::optional<int16_t> is_active;
    std::optional<int16_t> error_cntrl;
    std::optional<int16_t> data_a;
    std::optional<String> data_b;

    static inline thread_local std::shared_ptr<dto::Schema> schema;
    static inline String collectionName = tatpCollectionName;

    SKV_RECORD_FIELDS(s_id, sf_type, is_active, error_cntrl, data_a, data_b);
};

class CallForwarding {
public:
    static inline dto::Schema call_forwarding_schema {
        .name = "Call_Forwarding",
        .version = 1,
        .fields = std::vector<dto::SchemaField> {
            {dto::FieldType::INT32T, "s_id", false, false},
            {dto::FieldType::INT16T, "sf_type", false, false}, // 1..4
            {dto::FieldType::INT16T, "start_time", false, false},
            {dto::FieldType::INT16T, "end_time", false, false},            
            {dto::FieldType::STRING, "numberx", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1, 2 }
    };

    CallForwarding(RandomContext& random, int32_t sid, int16_t sftype, int16_t starttime) :
        s_id(sid), sf_type(sftype), start_time(starttime) {
        end_time = random.UniformRandom(1, 8);
        numberx = random.RandomString(15, 15, '0', '9');
    }

    CallForwarding(int32_t sid, int16_t sftype, int16_t starttime) : s_id(sid), sf_type(sftype), start_time(starttime) {}


    CallForwarding() = default;

    std::optional<int32_t> s_id;
    std::optional<int16_t> sf_type;
    std::optional<int16_t> start_time;
    std::optional<int16_t> end_time;
    std::optional<String> numberx;

    static inline thread_local std::shared_ptr<dto::Schema> schema;
    static inline String collectionName = tatpCollectionName;

    SKV_RECORD_FIELDS(s_id, sf_type, start_time, end_time, numberx);
};

void inline setupSchemaPointers() {
    Subscriber::schema = std::make_shared<dto::Schema>(Subscriber::subscriber_schema);
    AccessInfo::schema = std::make_shared<dto::Schema>(AccessInfo::access_info_schema);
    SpecialFacility::schema = std::make_shared<dto::Schema>(SpecialFacility::special_facility_schema);
    CallForwarding::schema = std::make_shared<dto::Schema>(CallForwarding::call_forwarding_schema);
}


