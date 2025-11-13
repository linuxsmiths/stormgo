#!/bin/bash

#
# This script is present in src/pylive/broker/.
# Following readlink is needed for correctly logging $srcdir in the error path.
#
SCRIPT_DIR="$(dirname $(readlink -f $0))"
srcdir="$SCRIPT_DIR/../.."
pylive_dir="$(readlink -f $srcdir/pylive)"

if [ -z "$pylive_dir" ] || [ ! -d "$pylive_dir" ]; then
    echo "$srcdir/pylive not present!"
    echo "Make sure install.sh is present in src/pylive/broker directory!"
    exit 1
fi

#
# Perform cleanup first.
# If not already installed, some of these will complain, but that's ok.
#
echo "STOP: pylive-broker.timer"
systemctl stop pylive-broker.timer

echo "STOP: pylive-broker.service"
systemctl stop pylive-broker.service

echo "DISABLE: pylive-broker.timer"
systemctl disable pylive-broker.timer

echo "DISABLE: pylive-broker.service"
systemctl disable pylive-broker.service

echo "RM: /lib/systemd/system/pylive-broker.timer"
rm -vf /lib/systemd/system/pylive-broker.timer

echo "RM: /lib/systemd/system/pylive-broker.service"
rm -vf /lib/systemd/system/pylive-broker.service

#
# Now install.
#
echo "SYMLINK: /usr/sbin/pylive-broker -> $pylive_dir/broker/run.sh"
ln -sf $pylive_dir/broker/run.sh /usr/sbin/pylive-broker

echo "CHMOD: /usr/sbin/pylive-broker"
chmod +x /usr/sbin/pylive-broker

echo "CP: pylive-broker.service pylive-broker.timer /lib/systemd/system/"
cp -vf pylive-broker.service pylive-broker.timer /lib/systemd/system/

systemctl daemon-reload

echo "ENABLE: pylive-broker.timer"
systemctl enable pylive-broker.timer

echo "START: pylive-broker.timer"
systemctl start pylive-broker.timer

systemctl -l --no-pager status pylive-broker.timer pylive-broker.service
echo

# Dump when the timer will run next
echo "NEXT TIMER AT"
systemctl list-timers --all | grep pylive-broker
