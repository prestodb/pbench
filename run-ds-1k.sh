wget https://go.dev/dl/go1.21.5.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.21.5.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
sudo yum install -y git
git clone https://github.com/yzhang1991/presto-benchmark.git

cd presto-benchmark
go build
cd benchmarks
benchmark_base='/Users/ezhang/Downloads/benchmark_runs'
server='https://engethanb334n.ibm.prestodb.dev'

# Java SF-1K
../presto-benchmark \
    -s $server \
    -o $benchmark_base \
    base.json \
    java.json \
    save_output.json \
    tpc-ds/queries_serial.json \
    tpc-ds/tpcds_sf1k_hive.json | tee $benchmark_base/tpcds_sf1k_hive_java.log

# C++ SF-1K
../presto-benchmark \
    -s $server \
    -o $benchmark_base \
    base.json \
    native.json \
    save_output.json \
    tpc-ds/queries_serial.json \
    tpc-ds/tpcds_sf1k_hive.json | tee $benchmark_base/tpcds_sf1k_hive_native.log

# Java SF-1 on Hive Query Runner
../presto-benchmark \
    -o $benchmark_base \
    base.json \
    java.json \
    save_output.json \
    tpc-ds/queries_serial.json \
    tpc-ds/tpcds_sf1.json | tee $benchmark_base/tpcds_sf1_java.log

# Very lightweight test stages on Hive Query Runner
../presto-benchmark \
    -o $benchmark_base \
    save_output.json \
    test/stage_1.json | tee $benchmark_base/test.log
