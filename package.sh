#!/bin/bash
if [ ! -e './install' ]; then
	echo "Run 'make install' first"
	return 1
fi
hash=$(git log -1 --format=%H -- prod.system.settings.json | cut -c1-7)

# modify INFO.txt file with correct Config Hash
info_file='./install/INFO.txt'
sed -i '/^Config Hash/d' ${info_file}
echo "Config Hash: ${hash}" >> ${info_file}

CWD=`pwd`
rm -rf release.tar release.tar.gz
mkdir -p releases
find python -name "*.py" -print0 | tar --null -cvf release.tar --exclude='benchmarks' --exclude='testing' --files-from=-
tar --append -vf release.tar -C ./install/ . -C $CWD shared-lib/
gzip release.tar
hash=$(git log -1 --format=%H -- prod.system.settings.json | cut -c1-7)
mv release.tar.gz releases/"springtail-$(date +%Y%m%d%H%M)-${hash}.tgz"
