#!/bin/sh

set -e
echo "node.id=$HOSTNAME" >> $PRESTO_HOME/etc/node.properties

/usr/bin/telegraf &
$PRESTO_HOME/bin/launcher run \
2>&1 | tee /var/log/presto-server/console-$(date '+%Y-%m-%dT%H:%M:%S').log
