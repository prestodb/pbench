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

# Java SF-1K standard
../presto-benchmark \
    -s $server \
    -o $benchmark_base \
    tpc-ds/tpc-ds_sf1k.json \
    java.json \
    tpc-ds/standard.json | tee $benchmark_base/tpc-ds_sf1k_java_standard.log

# Java SF-1K with order_by
../presto-benchmark \
    -s $server \
    -o $benchmark_base \
    tpc-ds/tpc-ds_sf1k.json \
    java.json \
    tpc-ds/ordered.json | tee $benchmark_base/tpc-ds_sf1k_java_ordered.log

# Native SF-1K standard
../presto-benchmark \
    -s $server \
    -o $benchmark_base \
    tpc-ds/tpc-ds_sf1k.json \
    native.json \
    tpc-ds/standard.json | tee $benchmark_base/tpc-ds_sf1k_native_standard.log

# Native SF-1K with order_by
../presto-benchmark \
    -s $server \
    -o $benchmark_base \
    tpc-ds/tpc-ds_sf1k.json \
    native.json \
    tpc-ds/ordered.json | tee $benchmark_base/tpc-ds_sf1k_native_ordered.log

# Java SF-1 on Hive Query Runner
../presto-benchmark \
    -o $benchmark_base \
    tpc-ds/tpc-ds_sf1.json \
    java.json \
    tpc-ds/standard.json | tee $benchmark_base/tpc-ds_sf1_java_standard.log

# Very lightweight test stages on Hive Query Runner
../presto-benchmark \
    -o $benchmark_base \
    save_output.json \
    test/stage_1.json | tee $benchmark_base/stage_test.log
