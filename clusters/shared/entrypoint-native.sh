#!/bin/sh
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

echo "node.id=$HOSTNAME" >> /opt/presto-server/etc/node.properties
NODE_IP=$(getent hosts | awk -v hn="$HOSTNAME" '$2==hn && $1 !~ /:/ {print $1}')
echo "node.internal-address=$NODE_IP" >> /opt/presto-server/etc/node.properties

dnf install procps-ng -y

if [ -f /opt/entrypoint_debug.sh ]
then
  cp ~/.ssh/authorized_keys2 ~/.ssh/authorized_keys
  chmod 600 /etc/ssh/*_key
  /usr/sbin/sshd
fi

/usr/bin/telegraf &
GLOG_logtostderr=1 presto_server \
    --etc-dir=/opt/presto-server/etc
