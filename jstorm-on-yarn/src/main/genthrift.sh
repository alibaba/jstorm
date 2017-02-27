rm -rf gen-java gen-py
rm -rf java/com/alibaba/jstorm/yarn/generated
thrift --gen java --gen py JstormAM.thrift
cp -r gen-java/* java/
rm -rf gen-java
