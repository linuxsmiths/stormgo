#!/bin/bash

#
# This script is present in src/pylive/cron/.
# Following readlink is needed for correctly logging $srcdir in the error path.
#
SCRIPT_DIR="$(dirname $(readlink -f $0))"
srcdir="$SCRIPT_DIR/../.."
engine_dir="$srcdir/engine"
engine_config="$engine_dir/backtester.json"
pylive_dir="$(readlink -f $srcdir/pylive)"

if [ -z "$pylive_dir" ] || [ ! -d "$pylive_dir" ]; then
    echo "$srcdir/pylive not present!"
    echo "Make sure run.sh must be present in src/pylive/cron directory!"
    # systemd should not restart us, as restarting is not going to help.
    exit 0
fi

# engine/backtester.json is the fundamental config.
if [ ! -f "$engine_config" ]; then
    echo "$engine_config not present!"
    # systemd should not restart us, as restarting is not going to help.
    exit 0
fi

#
# "logdir" config from engine/backtester.json
# That's where live logs are created.
#
LOGDIR=$(cat $engine_config | egrep '^\s*"logdir"\s*:' | cut -d: -f2 | sed -e "s/^\s*//g" -e "s/\s*$//g" | tr -d ',"')
if [ -z "$LOGDIR" ]; then
    echo "logdir not defined in $engine_config!"
    # systemd should not restart us, as restarting is not going to help.
    exit 0
fi

#
# Following commonly happens when LOGDIR is on a removable media and
# that's not connected, bail out early in that case to avoid confusing logs
# later.
#
if [ ! -d "$LOGDIR" ]; then
    echo "$LOGDIR not present, bailing out!"
    # systemd should not restart us, as restarting is not going to help.
    exit 0
fi

STATUS_FILE="$LOGDIR/status"

is_market_open()
{
        hour=$(date +%H)
        minute=$(date +%M)

        if [ $hour -gt 9 ]; then
                return 0
        fi

        if [ $hour -eq 9 -a $minute -gt 15 ]; then
                return 0
        fi

        return 1
}

#
# Clear logs before starting, but only when starting before market open.
# During market open if we are ever restarted we don't want to lose the days
# logs.
#
if ! is_market_open; then
        #
        # If pylive.log is present use its lmt for the directory name to move
        # last run's logs to, else use the current secs since epoch.
        #

        # Log file's day-of-year.
        if [ -e "$LOGDIR/pylive.log" ]; then
            fdoy=$(date +%j -r "$LOGDIR/pylive.log")
            l_mvdir=$LOGDIR/$(date +%D -r "$LOGDIR/pylive.log" | tr "/" "-")
        elif [ -e "$LOGDIR/pyprocess.log" ]; then
            fdoy=$(date +%j -r "$LOGDIR/pyprocess.log")
            l_mvdir=$LOGDIR/$(date +%D -r "$LOGDIR/pyprocess.log" | tr "/" "-")
        elif [ -e "$LOGDIR/engine.log" ]; then
            fdoy=$(date +%j -r "$LOGDIR/engine.log")
            l_mvdir=$LOGDIR/$(date +%D -r "$LOGDIR/engine.log" | tr "/" "-")
        fi

        # Today's day-of-year.
        tdoy=$(date +%j)

        #
        # If logfiles exist and they are not created today then we
        # need to back them up. We move only once in the morning and after
        # that any number of restarts of the processes cause logs to be added
        # to the same log files. This is good for debugging.
        #
        if [ -n "$fdoy" -a "$fdoy" != "$tdoy" ]; then
            mvdir=$l_mvdir
            if [ -d "$mvdir" ]; then
                mvdir="${mvdir}-$(date +%s)"
            fi
        fi

        if [ -n "$mvdir" -a ! -d "$mvdir" ]; then
            mkdir -p "$mvdir"
            mv -vf "$LOGDIR"/*.log "$mvdir"

            #
            # Filesystem may not support symlinks, so save $mvdir and use that
            # from pylive/cron/main.py for saving following files that can be used
            # for analysis later, if needed.
            #
            # $stock.prelive.csv
            # $stock.live.csv
            # $stock.final.live.*.csv
            #
            echo "$mvdir" > "$LOGDIR/backupdir_marker"
            [ $? -eq 0 ] || echo "Failed to write '$mvdir' to $LOGDIR/backupdir_marker]!"
        fi
fi

if [ "$RUNNING_FROM_SYSTEMD" == "1" ]; then
        # Log on stdout too so that "systemctl status pylive-cron" can show it
        echo "[$(date)] Starting pylive/cron/main.py from systemd"
        echo "[$(date)] Starting pylive/cron/main.py from systemd" >> "$STATUS_FILE"
        $pylive_dir/cron/main.py
        ret=$?
        echo "[$(date)] pylive/cron/main.py exited with code $ret"
        echo "[$(date)] pylive/cron/main.py exited with code $ret" >> "$STATUS_FILE"
        exit $ret
else
        echo "[$(date)] Starting pylive/cron/main.py"
        echo "[$(date)] Starting pylive/cron/main.py" >> "$STATUS_FILE"
        $pylive_dir/cron/main.py
        ret=$?
        echo "[$(date)] pylive/cron/main.py exited with code $ret"
        echo "[$(date)] pylive/cron/main.py exited with code $ret" >> "$STATUS_FILE"
        exit $ret
fi
