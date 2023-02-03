#!/bin/bash
set -e
topname=$(dirname "$0")
# source ${topname}/common_defs.sh
cd ${topname}/../..

runhttp=true
runnode=true
runtest=true

CPODIR=/tmp/___cpo_integ_test
EPS=("tcp+k2rpc://0.0.0.0:10000" "tcp+k2rpc://0.0.0.0:10001" "tcp+k2rpc://0.0.0.0:10002" "tcp+k2rpc://0.0.0.0:10003" "tcp+k2rpc://0.0.0.0:10004" "tcp+k2rpc://0.0.0.0:10005" "tcp+k2rpc://0.0.0.0:10006")
NUMCORES=`nproc`
# core on which to run the TSO poller thread. Pick 4 if we have that many, or the highest-available otherwise
#TSO_POLLER_CORE=$(( 5 > $NUMCORES ? $NUMCORES-1 : 4 ))
# poll on random core
TSO_POLLER_CORE=-1
PERSISTENCE=tcp+k2rpc://0.0.0.0:12001
CPO=tcp+k2rpc://0.0.0.0:9000
TSO=tcp+k2rpc://0.0.0.0:13000
COMMON_ARGS="--reactor-backend=epoll --enable_tx_checksum true --thread-affinity false"
HTTP=tcp+k2rpc://0.0.0.0:20000

while [[ $# -gt 0 ]]
do
    opt="$1"
    case $opt in
        --skiphttp|-nh)
            runhttp=false
            shift 1
            ;;
        --skipnode|-nn)
            runnode=false
            shift 1
            ;;
        --skiptest|-nt)
            runtest=false
            shift 1
            ;;
        --valgrind|-vg)
            VALG='valgrind --leak-check=yes'
            shift
            ;;
        *)
            break
            ;;
    esac
done

if [ "${runnode}" == true ]; then
    rm -rf ${CPODIR}
    # start CPO
    ./build/src/k2/cmd/controlPlaneOracle/cpo_main ${COMMON_ARGS} -c1 --tcp_endpoints ${CPO} 9001 --data_dir ${CPODIR} --prometheus_port 63000 --cpo.tso_assignment_timeout=100ms --cpo.assignment_base_backoff=100ms --txn_heartbeat_deadline=1s --nodepool_endpoints ${EPS[@]} --tso_endpoints ${TSO} --tso_error_bound=100us --persistence_endpoints ${PERSISTENCE}&
    cpo_child_pid=$!

    # start nodepool  k2::skv_server=Debug
    ./build/src/k2/cmd/nodepool/nodepool ${COMMON_ARGS} -c${#EPS[@]} --tcp_endpoints ${EPS[@]} --k23si_persistence_endpoint ${PERSISTENCE} --prometheus_port 63001 --memory=2G --partition_request_timeout=6s --log_level=Info &
    nodepool_child_pid=$!

    # start persistence
    ./build/src/k2/cmd/persistence/persistence ${COMMON_ARGS} -c1 --tcp_endpoints ${PERSISTENCE} --prometheus_port 63002 &
    persistence_child_pid=$!

    # start tso
    ./build/src/k2/cmd/tso/tso ${COMMON_ARGS} -c1 --tcp_endpoints ${TSO} --prometheus_port 63003 --tso.clock_poller_cpu=${TSO_POLLER_CORE} &
    tso_child_pid=$!
fi

if [ "${runhttp}" == true ]; then
    ${VALG} ./build/src/k2/cmd/httpproxy/http_proxy ${COMMON_ARGS} -c1 --tcp_endpoints ${HTTP} --memory=6G --cpo ${CPO} --log_level=Info k2::httpproxy=Info --httpproxy_txn_timeout=1h --httpproxy_expiry_timer_interval=50s --partition_request_timeout=100s &
    http_child_pid=$!
fi

function finish {
    rv=$?
    if [ "${runnode}" == true ]; then
 
       # cleanup code
       rm -rf ${CPODIR}

       kill ${nodepool_child_pid}
       echo "Waiting for nodepool child pid: ${nodepool_child_pid}"
       wait ${nodepool_child_pid}

       kill ${cpo_child_pid}
       echo "Waiting for cpo child pid: ${cpo_child_pid}"
       wait ${cpo_child_pid}

       kill ${persistence_child_pid}
       echo "Waiting for persistence child pid: ${persistence_child_pid}"
       wait ${persistence_child_pid}

       kill ${tso_child_pid}
       echo "Waiting for tso child pid: ${tso_child_pid}"
       wait ${tso_child_pid}
    fi
    if [ "${runhttp}" == true ]; then

        kill ${http_child_pid}
        echo "Waiting for http child pid: ${http_child_pid}"
        wait ${http_child_pid}
    fi
    echo ">>>> Test ${0} finished with code ${rv}"
}
trap finish EXIT
if [ "${runtest}" == true ];
then
    sleep 1
    echo ">>> Starting http test ..."
    PYTHONPATH=${PYTHONPATH}:./test/integration ./test/integration/test_http.py --http http://127.0.0.1:30000
else
    sleep infinity
fi

