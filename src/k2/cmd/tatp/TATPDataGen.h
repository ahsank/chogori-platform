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

#include <seastar/core/future.hh>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/config/Config.h>

#include <vector>

#include "TATPSchema.h"
#include "rand.h"

typedef std::vector<std::function<seastar::future<k2::WriteResult>(k2::K2TxnHandle&)>> TPCCData;

struct TATPDataGen {
    seastar::future<TPCCData> generateSubscriberData(uint32_t id_start, uint32_t id_end)
    {
        K2LOG_I(log::tatp, "Generating Subscriber data st={}, e={}", id_start, id_end);
        TPCCData data;
        uint32_t num_subscribers = id_end - id_start;
        size_t reserve_space = 0;
        reserve_space += num_subscribers;
        reserve_space += num_subscribers * (maxSFIDPerSubs-minSFIDPerSubs+1)/2; // Special facility
        reserve_space += num_subscribers * (maxAInfoPerSubs-minAInfoPerSubs+1)/2; // Access Info
        reserve_space += num_subscribers * ((maxSFIDPerSubs-minSFIDPerSubs+1)/2) * (maxCFPerSF-minCFPerSF+1)/2; // Call Forwarding
        data.reserve(reserve_space);

        return seastar::do_with(
        std::move(data),
        RandomContext(id_start),
        boost::irange(id_start, id_end),
        [this] (auto& data, auto& random, auto& range) {
            return seastar::do_for_each(
                range,
                [this, &data, &random] (auto idx) mutable {
                    K2LOG_D(log::tatp, "Generating subscriber={}", idx);
                    auto subscriber = Subscriber(random, idx);
                    data.push_back([subs=subscriber] (k2::K2TxnHandle& txn) mutable {
                        return writeRow<Subscriber>(subs, txn);
                    });
                    // Generate between 1 to 4 access infos per subscriber
                    auto aids = random.UniqueRandomIds(1,4);
                    for (auto aid : aids) {
                        K2LOG_D(log::tatp, "Generating Access Info {} for subscriber={}", aid, idx);
                        auto ainfo = AccessInfo(random, idx, aid);
                        data.push_back([_ainfo=ainfo] (k2::K2TxnHandle& txn) mutable {
                            return writeRow<AccessInfo>(_ainfo, txn);
                        });
                    }
                    // gGenerate between 1 to 4 Special facilities per subscriber
                    auto sfids = random.UniqueRandomIds(1,4);
                    for (auto sfid: sfids) {
                        K2LOG_D(log::tatp, "Generating Special Facility {} for subscriber={}", sfid, idx);
                        auto sf = SpecialFacility(random, idx, sfid);
                        data.push_back([_sf=sf] (k2::K2TxnHandle& txn) mutable {
                                return writeRow<SpecialFacility>(_sf, txn);
                        });
                        // Generate between 0 to 3 call forwardings per special facility
                        auto cfids = random.UniqueRandomIds(0,3);
                        for (auto cfid : cfids) {
                            static const unsigned start_times[] = {0, 8, 16};
                            auto start_time = start_times[cfid-1];
                            K2LOG_D(log::tatp, "Generating Call Forwading {} for Special Facility={}",  start_time, sfid);                            
                            auto cf = CallForwarding(random, idx, sfid, start_time);
                            data.push_back([_cf=cf] (k2::K2TxnHandle& txn) mutable {
                                return writeRow<CallForwarding>(_cf, txn);
                            });

                        }

                    }
            })
            .then([&data] () mutable {
                return seastar::make_ready_future<TPCCData>(std::move(data));
            });
        });
    }
            

};
