#!/bin/sh

OPTIND=1 # Reset in case getopts has been used previously in the shell.

dataset_folder=""
build="release"

while getopts "h?f:d?" opt; do
  case "$opt" in
    h|\?)
      echo "-f <dataset folder> [-d]"
      echo "-d for using debug builds"
      exit 0
      ;;
    f)  dataset_folder=$OPTARG
      ;;
    d)  build="debug"
      ;;
  esac
done

shift $((OPTIND-1))

if [ "$OPTIND" -eq 1 ]; then
  echo "No options provided."
  echo "-f <dataset folder> [-d]"
  echo "-d for using debug builds"
  exit 1
fi


echo "Build: ${build}"
echo "Using ... dataset_folder='$dataset_folder'"

echo "Starting PG..."

python3 ./springtail.py -f ../../system.json.test -b ../../${build} --kill
# don't start springtail yet
python3 ./springtail.py -s $dataset_folder/create_dataset_tables.sql -f ../../system.json.test -b ../../${build} --start --postgres-only

# load data into primary 
while read -r line; do
    set -- $line
    echo "Found config to load $dataset_folder/$2 INTO $1"
    python3 ./load_csv.py -c $dataset_folder/$2 -t $1 -f ../../system.json.test
    echo "-----------------------------"
done < "$dataset_folder/load_csv.cfg"

# this will start springtail and copy the dataset from primary to springtail
echo "Starting springtail..."
python3 ./springtail.py -f ../../system.json.test -b ../../${build} --start --no-cleanup

