rm -rf release/*
rm -rf debug/*
rmdir release
rmdir debug
find . -name "*~" | xargs rm -f
find . -name "#*" | xargs rm -f
