export HADOOP_HOME=$5
export JAVA_HOME=$6
export prefix=$(pwd)
export JSTORM_HOME=$prefix/deploy/jstorm
export PATH=$JATORM_HOME/bin:$HADOOP_HOME/bin:$JAVA_HOME/bin:$PATH

hadoop fs -get $3 deploy 

echo -e " storm.local.dir: "$2/data" \n" >> deploy/jstorm/conf/storm.yaml

IFS=', ' read -r -a array <<< "$4"
echo -e " supervisor.slots.ports: [$4]" >> deploy/jstorm/conf/storm.yaml
#echo -e " supervisor.slots.ports: " >> deploy/jstorm/conf/storm.yaml
#for element in "${array[@]}"
#do
#    echo -e "    - $element" >> deploy/jstorm/conf/storm.yaml
#done

python ./deploy/jstorm/bin/jstorm $1
