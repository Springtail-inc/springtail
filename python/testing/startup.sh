#!/bin/bash
python3 ./springtail.py --start -f ../../system.json.test -b ../../debug $1 $2 $3
python3 ./springtail.py --start -f ../../system.json.test -b ../../debug --check
