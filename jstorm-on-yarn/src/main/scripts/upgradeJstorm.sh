hadoop fs -rm -r -f /tmp/$1/deploy && hadoop fs -mkdir -p /tmp/$1/ && hadoop fs -put -f $2/deploy /tmp/$1/
