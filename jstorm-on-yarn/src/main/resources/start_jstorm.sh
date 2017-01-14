export HADOOP_HOME=$5
export JAVA_HOME=$6
export PYTHON_HOME=$7
export prefix=$(pwd)
export JSTORM_HOME=$prefix/deploy/jstorm

export PATH=$PYTHON_HOME/bin:$JSTORM_HOME/bin:$HADOOP_HOME/bin:$JAVA_HOME/bin:$PATH

#mkdir $8

rm -rf deploy
#hadoop fs -get $3 $8/deploy
hadoop fs -get $3 deploy

echo -e " storm.local.dir: "$2/data" \n" >> deploy/jstorm/conf/storm.yaml

IFS=', ' read -r -a array <<< "$4"
echo -e " supervisor.slots.ports: [$4]" >> deploy/jstorm/conf/storm.yaml
#for element in "${array[@]}"
#do
#    echo -e "    - $element" >> deploy/jstorm/conf/storm.yaml
#done

echo -e " $1.deamon.logview.port: $9" >> deploy/jstorm/conf/storm.yaml
echo -e " nimbus.thrift.port: ${10}" >> deploy/jstorm/conf/storm.yaml

echo "begin jstorm: " >> ${11}
JAVA_HOME=$6 python ./deploy/jstorm/bin/jstorm $1  >> ${11} 2>>${11}