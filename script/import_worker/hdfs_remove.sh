path=$1
if
sudo -u hive hadoop fs -test -e $path
then
sudo -u hive hadoop fs -rm -r $path
fi