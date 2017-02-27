namespace java com.alibaba.jstorm.yarn.generated

service JstormAM {

  //info
  string info();
  void stopAppMaster();

  // start/stop/remove nimbus
  void startNimbus(1: i32 number,2: i32 container_memory,3: i32 container_vcorecount);
  void stopNimbus();
  void removeNimbus(1: i32 number);
  void removeSpecNimbus(1: string container_id);

  // supervisors
  void addSupervisors(1: i32 number,2: i32 container_memory,3: i32 container_vcorecount);

  // add supervisor to specific racks or hosts`
  void addSpecSupervisor(1: i32 number,2: i32 container_memory,3: i32 container_vcorecount,4: list<string> racks,5: list<string> hosts);

  // add nimbus to specific racks or hosts
  void startSpecNimbus(1: i32 number,2: i32 container_memory,3: i32 container_vcorecount,4: list<string> racks,5: list<string> hosts);

  // enable/disable supervisors
  void startSupervisors();
  void stopSupervisors();
  void removeSupervisors(1: i32 number);
  void removeSpecSupervisors(1: string container_id);

  void upgradeCluster();

  // shutdown storm cluster
  void shutdown();

  //config
  string getConfig();
  void setConfig(1: string key,2: string value);
}
