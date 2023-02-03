/*
MIT License

Copyright(c) 2020 Futurewei Cloud

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

#include <utility>

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/transport/RetryStrategy.h>

#include "TATPSchema.h"
#include "Log.h"

using namespace seastar;
using namespace k2;

// a simple retry strategy, stop after a number of retries
class FixedRetryStrategy {
public:
    FixedRetryStrategy(int retries) : _retries(retries), _try(0), _success(false) {
    }

    ~FixedRetryStrategy() = default;

template<typename Func>
    future<> run(Func&& func) {
        K2LOG_D(log::tatp, "First attempt");
        return do_until(
            [this] { return this->_success || this->_try >= this->_retries; },
            [this, func=std::move(func)] () mutable {
                this->_try++;
                return func().
                    then_wrapped([this] (auto&& fut) {
                        _success = !fut.failed() && fut.get0();
                        K2LOG_D(log::tatp, "round {} ended with success={}", _try, _success);
                        return make_ready_future<>();
                    });
            }).then_wrapped([this] (auto&& fut) {
                if (!fut.failed()) {
                    fut.ignore_ready_future();
                }

                if (fut.failed()) {
                    K2LOG_W_EXC(log::tatp, fut.get_exception(), "Txn failed");
                } else if (!_success) {
                    K2LOG_D(log::tatp, "Txn attempt failed");
                    return make_exception_future<>(std::runtime_error("Attempt failed"));
                }
                return make_ready_future<>();
            });
    }

private:
    // how many times we should retry
    int _retries;
    // which try we're on
    int _try;
    // indicate if the latest round has succeeded (so that we can break the retry loop)
    bool _success;
};


class AtomicVerify;

class TATPTxn {
public:
    virtual future<bool> attempt() {return make_ready_future<bool>(true);}
    virtual ~TATPTxn() = default;

    future<bool> run() {
        return attempt()
            .handle_exception([] (auto exc) {
                K2LOG_W_EXC(log::tatp, exc, "Txn failed after retries");
                return make_ready_future<bool>(false);
        });
    }
};


class GetSubscriberDataT: public TATPTxn {
public:
    GetSubscriberDataT(RandomContext& random, K23SIClient& client, uint32_t max_s_id):
        _client(client) {
        _c_sub_id = random.UniformRandom(1, max_s_id);
    }
        
    future<bool> attempt() override {
        K2TxnOptions options{};
        options.deadline = Deadline(5s);
        return _client.beginTxn(options)
            .then([this] (K2TxnHandle&& txn) {
                return ReadSubscriber(txn);
            }).handle_exception([] (auto exc) {
                K2LOG_W_EXC(log::tatp, exc, "Failed to start txn");
                return make_ready_future<bool>(false);
            });
    }

    future<bool> ReadSubscriber(K2TxnHandle& txn) {
        return txn.read<Subscriber>(Subscriber(_c_sub_id))
            .then([this] (auto&& result) {
                if (!result.status.is2xxOK()) {
                    K2LOG_W(log::tatp, "TATP Get subscriber Txn failed: {}, {}", _c_sub_id,  result.status);                    
                }
                return make_ready_future<bool>(result.status.is2xxOK());
         })
         .finally([this, &txn] {
             return txn.end(!_abort);
         });
    }

private:
    unsigned _c_sub_id;
    bool _failed = false;
    bool _abort = false;
    K23SIClient& _client;
};

namespace dtoe = dto::expression;

class GetNewDestinationT: public TATPTxn {
public:
    GetNewDestinationT(RandomContext& random, K23SIClient& client, uint32_t max_s_id):
        _client(client) {
        _sub_id = random.UniformRandom(1, max_s_id);
        _sf_type = random.UniformRandom(1, 4);
        auto start_time_idx = random.UniformRandom(0,2);
        static unsigned start_times[] = {0, 8, 16};
        _start_time = start_times[start_time_idx];
        _end_time = random.UniformRandom(1,24);
    }
        
    future<bool> attempt() override {
        K2TxnOptions options{};
        options.deadline = Deadline(5s);
        return _client.beginTxn(options)
            .then([this] (K2TxnHandle&& txn) {
                _txn = std::move(txn);
                return when_all_succeed(ReadSpecialFacility(), QueryCallForwarding());
            })
            .then_unpack([this](bool result1, bool result2) {
                _failed = !result1 || !result2;
                return _txn.end(!_abort);
            })
            .then([this](auto&& ) {
                return !_failed;
            })
            .handle_exception([] (auto exc) {
                K2LOG_W_EXC(log::tatp, exc, "Failed to start txn");
                return make_ready_future<bool>(false);
            });
    }

    future<bool> ReadSpecialFacility() {
        return _txn.read<SpecialFacility>(SpecialFacility(_sub_id, _sf_type))
            .then([this] (auto&& result) {
                if (result.status.code == 404) {
                    return make_ready_future<bool>(false);                
                }
                if (!result.status.is2xxOK()) {
                    K2LOG_W(log::tatp, "TATP Get special facility Txn failed: {}, {}, {}", _sub_id, _sf_type, result.status);                    
                }
                return make_ready_future<bool>(result.status.is2xxOK());
         });
    }

    future<bool> QueryCallForwarding() {
        return _client.createQuery(tatpCollectionName, CallForwarding::call_forwarding_schema.name)
            .then([this](auto&& response) {
                CHECK_READ_STATUS_TYPE(response, QueryResult);
                _query  = std::move(response.query);
                _query.startScanRecord.serializeNext<int32_t>(_sub_id);                
                _query.startScanRecord.serializeNext<int16_t>(_sf_type);
                _query.endScanRecord.serializeNext<int32_t>(_sub_id);                
                _query.endScanRecord.serializeNext<int16_t>(_sf_type);
                _query.setLimit(-1);
                _query.setReverseDirection(false);
                std::vector<dtoe::Expression> subexpr;
                std::vector<dtoe::Value> values;
                values.emplace_back(dtoe::makeValueReference("start_time"));
                values.emplace_back(dtoe::makeValueLiteral<int32_t>(_start_time));
                subexpr.emplace_back(dtoe::Expression{
                    dtoe::Operation::LTE,
                    std::move(values),
                    {}
                });
                values.clear();
                values.emplace_back(dtoe::makeValueReference("end_time"));
                values.emplace_back(dtoe::makeValueLiteral<int32_t>(_end_time));
                subexpr.emplace_back(dtoe::Expression{
                    dtoe::Operation::GT,
                    std::move(values),
                    {}
                });

                dtoe::Expression filter{
                    dtoe::Operation::AND,
                    {},
                    std::move(subexpr)
                };
                _query.setFilterExpression(std::move(filter));
                return _txn.query(_query);
            })
            .then([this](auto&& response) {
                   if (!response.status.is2xxOK()) {
                       K2LOG_E(log::tatp, "Query response Error, status: {}", response.status);
                        return make_ready_future<bool>(false);
                   }
                   int count = 0;
                   for (dto::SKVRecord& rec :  response.records) {
                       std::optional<String> num = rec.deserializeField<String>("numberx");
                       K2LOG_D(log::tatp, "Numberx: {}", num);
                       count++;
                   }
                   return make_ready_future<bool>(count > 0);                        
            });
    }
    
private:
    unsigned _sub_id;
    unsigned _sf_type;
    unsigned _start_time;
    unsigned _end_time;
    K2TxnHandle _txn;
    Query _query;
    bool _failed = false;
    bool _abort = false;
    K23SIClient& _client;
};


class GetAccessDataT: public TATPTxn {
public:
    GetAccessDataT(RandomContext& random, K23SIClient& client, uint32_t max_s_id):
        _client(client) {
        _sub_id = random.UniformRandom(1, max_s_id);
        _acc_type = random.UniformRandom(1, 4);
    }
        
    future<bool> attempt() override {
        K2TxnOptions options{};
        options.deadline = Deadline(5s);
        return _client.beginTxn(options)
            .then([this] (K2TxnHandle&& txn) {
                _txn = std::move(txn);
                return ReadAccessData();
            })
            .then([this] {
                return _txn.end(!_abort).discard_result();
            })
            .then([this] {
                return !_failed;
            })
            .handle_exception([] (auto exc) {
                K2LOG_W_EXC(log::tatp, exc, "Failed to start txn");
                return make_ready_future<bool>(false);
            });
    }

    future<> ReadAccessData() {
        return _txn.read<AccessInfo>(AccessInfo(_sub_id, _acc_type))
            .then([this] (auto&& result) {
                _failed = !result.status.is2xxOK();
                if (!result.status.is2xxOK() && result.status.code != 404) {
                    K2LOG_W(log::tatp, "TATP Get Access Data Txn failed: {}, {}", _sub_id,  result.status);                    
                } else {
                    if (result.status.code != 404) {
                        const AccessInfo& val = result.value;
                        K2LOG_D(log::tatp, "TATP access data : {}, {} {} {}",
                                val.data1, val.data2, val.data3, val.data4);           
                    }
                }
                return make_ready_future<>();
            });
    }

private:
    unsigned _sub_id;
    unsigned _acc_type;
    bool _failed = false;
    bool _abort = false;
    K23SIClient& _client;
    K2TxnHandle _txn;
};


class UpdateSubscriberDataT: public TATPTxn {
public:
    UpdateSubscriberDataT(RandomContext& random, K23SIClient& client, uint32_t max_s_id):
        _client(client) {
        _sub_id = random.UniformRandom(1, max_s_id);
        _sf_type = random.UniformRandom(1, 4);
        _bit_1 = random.UniformRandom(0, 1);
        _data_a = random.UniformRandom(0, 255);
    }
        
    future<bool> attempt() override {
        K2TxnOptions options{};
        options.deadline = Deadline(5s);
        return _client.beginTxn(options)
            .then([this] (K2TxnHandle&& txn) {
                _txn = std::move(txn);
                return when_all_succeed(UpdateSubscriber(), UpdateSpecialFacility());
            })
            .then_unpack([this](bool result1, bool result2) {
                _failed = !result1 || !result2;
                return _txn.end(!_abort).discard_result();
            })
            .then([this]() {
                return !_failed;
            })
            .handle_exception([] (auto exc) {
                K2LOG_W_EXC(log::tatp, exc, "Failed to start txn");
                return make_ready_future<bool>(false);
            });
    }

    static const inline std::vector<uint32_t> bit_field = {2};
    static const inline std::vector<uint32_t> data_a_field = {4};

    future<bool> UpdateSpecialFacility() {
        SpecialFacility sf(_sub_id, _sf_type);
        sf.data_a = _data_a;
        return _txn.partialUpdate<SpecialFacility>(sf, data_a_field)
            .then([this] (auto&& result) {
                if (!result.status.is2xxOK() && result.status.code != 404) {
                    K2LOG_W(log::tatp, "TATP Get special facility Txn failed: {}, {}, {}", _sub_id, _sf_type, result.status);                    
                }
                return make_ready_future<bool>(result.status.is2xxOK());
         });
    }
    
    future<bool> UpdateSubscriber() {
        return _txn.read<Subscriber>(Subscriber(_sub_id))
            .then([this] (auto&& result) {
                if (!result.status.is2xxOK()) {
                    K2LOG_W(log::tatp, "TATP Get subscriber Txn failed: {}, {}", _sub_id,  result.status);
                    return make_ready_future<bool>(false);
                }
                std::bitset<10> bitval = result.value.bits.value_or(0);
                bitval[0] = _bit_1;
                result.value.bits = int16_t(bitval.to_ulong());
                return _txn.partialUpdate<Subscriber>(result.value, bit_field)
                    .then([this] (auto&& write_result) {
                        return make_ready_future<bool>(write_result.status.is2xxOK());
                    });


            });
    }
    
private:
    unsigned _sub_id;
    unsigned _sf_type;
    unsigned char _data_a;
    unsigned char _bit_1;
    K2TxnHandle _txn;
    Query _query;
    bool _failed = false;
    bool _abort = false;
    K23SIClient& _client;
};
