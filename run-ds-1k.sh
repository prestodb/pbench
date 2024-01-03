wget https://go.dev/dl/go1.21.5.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.21.5.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
sudo yum install -y git
git clone https://github.com/yzhang1991/presto-benchmark.git

cd presto-benchmark
go build
cd benchmarks
nohup ../presto-benchmark \
    --server https://engethanb333j.ibm.prestodb.dev \
    --output-path /home/centos \
    base.json \
    java.json \
    save_output.json \
    tpc-ds/serial.json \
    tpc-ds/tpcds_sf1k_hive.json | tee /home/centos/tpcds_sf1k_hive.log &
