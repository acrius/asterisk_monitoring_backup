#!/bin/bash

DEVDIR=/home/admindok/dev/asterisk_monitoring_backup
VENVDIR=$DEVDIR/env

cd $DEVDIR
source $VENVDIR/bin/activate
python3 server.py
