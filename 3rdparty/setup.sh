#!/bin/bash

#
# Update this if your dist-packages dir is different.
#
DIST_PACKAGES="/usr/local/lib/python3.12/dist-packages"

_3RDPARTY_DIR="$(dirname $(readlink -f $0))"

#
# Must use the checked-in SmartApi version.
# If installed using pip install dist-packages will have a directory with the
# name of the package, what we need is a symlink pointing to the corresponding
# directory inside 3rdparty/
#
symlink="$DIST_PACKAGES/SmartApi"
if [ ! -h "$symlink" -a -d "$symlink" ]; then
    echo
    echo "************************************************************"
    echo "You have already installed smartapi-python using pip install."
    echo "It is highly recommended that you don't use the installed version,"
    echo "but instead use the checked in version, with our fixes."
    echo "Run the following command and make sure $symlink is not present"
    echo
    echo "pip uninstall smartapi-python"
    echo
    echo "and then run setup.sh again!"
    echo "************************************************************"
    echo
    exit 1
fi

# If present and not a directory, must be a symlink.
if [ -e "$symlink" -a ! -h "$symlink" ]; then
    echo "*** $symlink is not a symlink! ***"
    exit 1
fi

#
# If symlink, make sure it points to the desired directory, if not setup
# symlink correctly.
#
need_setup=true
desired_tgt="$_3RDPARTY_DIR/smartapi-python/SmartApi"

if [ -h "$symlink" ]; then
    existing_tgt=$(readlink "$symlink")
    if [ "$existing_tgt" == "$desired_tgt" ]; then
        echo "Desired symlink [$symlink -> $existing_tgt] already present!"
        echo
        need_setup=false
    else
        echo "Replacing symlink:"
        echo "  [$symlink -> $existing_tgt], with"
        echo "  [$symlink -> $desired_tgt]..."

        rm -f "$symlink"
    fi
fi

if $need_setup; then
    ln -sf "$desired_tgt" "$symlink"
    echo
fi

#
# Must use the checked-in nselib version.
# If installed using pip install dist-packages will have a directory with the
# name of the package, what we need is a symlink pointing to the corresponding
# directory inside 3rdparty/
#
symlink="$DIST_PACKAGES/nselib"
if [ ! -h "$symlink" -a -d "$symlink" ]; then
    echo
    echo "************************************************************"
    echo "You have already installed nselib using pip install."
    echo "It is highly recommended that you don't use the installed version,"
    echo "but instead use the checked in version, with our fixes."
    echo "Run the following command and make sure $symlink is not present"
    echo
    echo "pip uninstall nselib"
    echo
    echo "and then run setup.sh again!"
    echo "************************************************************"
    echo
    exit 1
fi

# If present and not a directory, must be a symlink.
if [ -e "$symlink" -a ! -h "$symlink" ]; then
    echo "*** $symlink is not a symlink! ***"
    exit 1
fi

#
# If symlink, make sure it points to the desired directory, if not setup
# symlink correctly.
#
need_setup=true
desired_tgt="$_3RDPARTY_DIR/nselib-2.0/nselib"

if [ -h "$symlink" ]; then
    existing_tgt=$(readlink "$symlink")
    if [ "$existing_tgt" == "$desired_tgt" ]; then
        echo "Desired symlink [$symlink -> $existing_tgt] already present!"
        echo
        need_setup=false
    else
        echo "Replacing symlink:"
        echo "  [$symlink -> $existing_tgt], with"
        echo "  [$symlink -> $desired_tgt]..."

        rm -f "$symlink"
    fi
fi

if $need_setup; then
    ln -sf "$desired_tgt" "$symlink"
    echo
fi
