#!/bin/bash
if [ ! -e './install' ]; then
	echo "Run 'make install' first"
	return 1
fi
CWD=`pwd`
find python -name "*.py" -print0 | tar --null -cvf release.tar --exclude='benchmarks' --exclude='testing' --files-from=-
tar --append -f release.tar -C ./install/ . -C $CWD shared-lib/
gzip release.tar
mv release.tar.gz releases/"springtail-$(date +%Y%m%d).tgz"
#tar cpvfz releases/"springtail-$(date +%Y%m%d).tgz" -C ./install/ . -C $CWD shared-lib/ -C python/shared python/shared -C python/coordinator python/coordinator
