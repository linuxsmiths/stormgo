#!/bin/bash

#
# This script is present in src/pylive/cron/.
# Following readlink is needed for correctly logging $srcdir in the error path.
#
SCRIPT_DIR="$(dirname $(readlink -f $0))"
srcdir="$SCRIPT_DIR/../.."
pylive_dir="$(readlink -f $srcdir/pylive)"

if [ -z "$pylive_dir" ] || [ ! -d "$pylive_dir" ]; then
    echo "$srcdir/pylive not present!"
    echo "Make sure install.sh is present in src/pylive/cron directory!"
    exit 1
fi

#
# Perform cleanup first.
# If not already installed, some of these will complain, but that's ok.
#
echo "STOP: pylive-cron.timer"
systemctl stop pylive-cron.timer

echo "STOP: pylive-cron.service"
systemctl stop pylive-cron.service

echo "DISABLE: pylive-cron.timer"
systemctl disable pylive-cron.timer

echo "DISABLE: pylive-cron.service"
systemctl disable pylive-cron.service

echo "RM: /lib/systemd/system/pylive-cron.timer"
rm -vf /lib/systemd/system/pylive-cron.timer

echo "RM: /lib/systemd/system/pylive-cron.service"
rm -vf /lib/systemd/system/pylive-cron.service

#
# Now install.
#
echo "SYMLINK: /usr/sbin/pylive-cron -> $pylive_dir/cron/run.sh"
ln -sf $pylive_dir/cron/run.sh /usr/sbin/pylive-cron

echo "CHMOD: /usr/sbin/pylive-cron"
chmod +x /usr/sbin/pylive-cron

echo "CP: pylive-cron.service pylive-cron.timer /lib/systemd/system/"
cp -f pylive-cron.service pylive-cron.timer /lib/systemd/system/

systemctl daemon-reload

echo "ENABLE: pylive-cron.timer"
systemctl enable pylive-cron.timer

echo "START: pylive-cron.timer"
systemctl start pylive-cron.timer

systemctl -l --no-pager status pylive-cron.timer pylive-cron.service
echo

# Dump when the timer will run next
echo "NEXT TIMER AT"
systemctl list-timers --all | grep pylive-cron
