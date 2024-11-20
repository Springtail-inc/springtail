#!/bin/bash
if [ ! -e './install' ]; then
	echo "Run 'make install' first"
	return 1
fi
mkdir -p releases
tar cvfz releases/"springtail-$(date +%Y%m%d).tgz" -C ./install/ .
