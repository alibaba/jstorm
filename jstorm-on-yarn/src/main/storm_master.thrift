namespace java com.alibaba.jstorm.yarn.generated

service StormMaster {
  // Storm configuration
  string getStormConf();
  void setStormConf(1: string storm_conf);

  // supervisors
  void addSupervisors(1: i32 number);

  // start/stop nimber
  void startNimbus();
  void stopNimbus();

  // enable/disable ui
  void startUI();
  void stopUI();

  // enable/disable supervisors
  void startSupervisors();
  void stopSupervisors();

  // shutdown storm cluster
  void shutdown();
}