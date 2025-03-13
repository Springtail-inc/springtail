rm -rf release/*
rm -rf debug/*
rm -rf shared-lib/*
find . -name "*~" | xargs rm -f
find . -name "#*" | xargs rm -f
