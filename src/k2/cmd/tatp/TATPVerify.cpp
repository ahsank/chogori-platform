/*
MIT License

Copyright(c) 2021 Futurewei Cloud

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

#include "TATPVerify.h"

using namespace k2;
using namespace seastar;

void AtomicVerify::compareAbortValues() {
    K2ASSERT(log::tatp, _before.w_ytd == _after.w_ytd, "Warehouse YTD did not abort!");
    K2ASSERT(log::tatp, _before.d_ytd == _after.d_ytd, "District YTD did not abort!");
    K2ASSERT(log::tatp, _before.c_ytd == _after.c_ytd, "Customer YTD did not abort!");
    K2ASSERT(log::tatp, _before.c_balance == _after.c_balance, "Customer Balance did not abort!");
    K2ASSERT(log::tatp, _before.c_payments == _after.c_payments, "Customer Payment Count did not abort!");
}

future<> AtomicVerify::run() {
    return make_ready_future<>();
}

// Consistency condition 1 of spec: Sum of district YTD == warehouse YTD
future<> ConsistencyVerify::verifyWarehouseYTD() {
    return make_ready_future<>();
}

// Consistency condition 2: District next orderID - 1 == max OrderID == max NewOrderID
future<> ConsistencyVerify::verifyOrderIDs() {
    return make_ready_future<>();
}

// Consistency condition 3: max(new order ID) - min (new order ID) + 1 == number of new order rows
future<> ConsistencyVerify::verifyNewOrderIDs() {
    return make_ready_future<>();
}

// Consistency condition 4: sum of order lines from order table == number or rows in order line table
future<> ConsistencyVerify::verifyOrderLineCount() {
    return make_ready_future<>();
}

// Consistency condition 5: order carrier id is 0 iff there is a matching new order row
future<> ConsistencyVerify::verifyCarrierID() {
    return make_ready_future<>();
}

// Helper for condition 6
future<int16_t> ConsistencyVerify::countOrderLineRows(int64_t) {
    return make_ready_future<int16_t>(0);
}

// Consistency condition 6: for each order, order line count == number of rows in order line table
future<> ConsistencyVerify::verifyOrderLineByOrder() {
    return make_ready_future<>();
}

// Consistency condition 7: order line delivery is 0 iff carrier is 0 in order
future<> ConsistencyVerify::verifyOrderLineDelivery() {
     return make_ready_future<>();
}


// Helper for consistency conditions 8 and 9
future<DecimalD25> ConsistencyVerify::historySum(bool) {
    return make_ready_future<DecimalD25>(0);
}

// Consistency condition 8: Warehouse YTD == sum of history amount
future<> ConsistencyVerify::verifyWarehouseHistorySum() {
    return make_ready_future<>();

}

// Consistency condition 9: District YTD == sum of history amount
future<> ConsistencyVerify::verifyDistrictHistorySum() {
    return make_ready_future<>();
}

future<> ConsistencyVerify::runForEachWarehouse(consistencyOp) {
    return make_ready_future<>();
}

future<> ConsistencyVerify::runForEachWarehouseDistrict(consistencyOp) {
    return make_ready_future<>();
}

future<> ConsistencyVerify::run() {
    K2LOG_I(log::tatp, "Starting consistency verification 1");
    return runForEachWarehouse(&ConsistencyVerify::verifyWarehouseYTD)
    .then([this] () {
        K2LOG_I(log::tatp, "verifyWarehouseYTD consistency success");
        K2LOG_I(log::tatp, "Starting consistency verification 2: order ID");
        return runForEachWarehouseDistrict(&ConsistencyVerify::verifyOrderIDs);
    })
    .then([this] () {
        K2LOG_I(log::tatp, "verifyOrderIDs consistency success");
        K2LOG_I(log::tatp, "Starting consistency verification 3: neworder ID");
        return runForEachWarehouseDistrict(&ConsistencyVerify::verifyNewOrderIDs);
    })
    .then([this] () {
        K2LOG_I(log::tatp, "verifyNewOrderIDs consistency success");
        K2LOG_I(log::tatp, "Starting consistency verification 4: order lines count");
        return runForEachWarehouseDistrict(&ConsistencyVerify::verifyOrderLineCount);
    })
    .then([this] () {
        K2LOG_I(log::tatp, "verifyOrderLineCount consistency success");
        K2LOG_I(log::tatp, "Starting consistency verification 5: carrier ID");
        return runForEachWarehouseDistrict(&ConsistencyVerify::verifyCarrierID);
    })
    .then([this] () {
        K2LOG_I(log::tatp, "verifyCarrierID consistency success");
        K2LOG_I(log::tatp, "Starting consistency verification 6: order line by order");
        return runForEachWarehouseDistrict(&ConsistencyVerify::verifyOrderLineByOrder);
    })
    .then([this] () {
        K2LOG_I(log::tatp, "verifyOrderLineByOrder consistency success");
        K2LOG_I(log::tatp, "Starting consistency verification 7: order line delivery");
        return runForEachWarehouseDistrict(&ConsistencyVerify::verifyOrderLineDelivery);
    })
    .then([this] () {
        K2LOG_I(log::tatp, "verifyOrderLineDelivery consistency success");
        K2LOG_I(log::tatp, "Starting consistency verification 8: warehouse ytd and history sum");
        return runForEachWarehouse(&ConsistencyVerify::verifyWarehouseHistorySum);
    })
    .then([this] () {
        K2LOG_I(log::tatp, "verifyWarehouseHistory sum consistency success");
        K2LOG_I(log::tatp, "Starting consistency verification 9: district ytd and history sum");
        return runForEachWarehouseDistrict(&ConsistencyVerify::verifyDistrictHistorySum);
    });
}
