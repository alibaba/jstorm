package com.alibaba.jstorm.elasticsearch.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EsOutputDeclarer implements Serializable {

  private static final long serialVersionUID = 8273553454942900376L;
  
  private List<String> fields = new ArrayList<String>();

  public EsOutputDeclarer addField(String... names) {
    for (String name : names) {
      fields.add(name);
    }
    return this;
  }

  public String[] getFields() {
    return fields.toArray(new String[0]);
  }

  public List<Object> getValues(Map<String, Object> source) {
    List<Object> values = new ArrayList<Object>();
    for (String field : fields) {
      values.add(source.get(field));
    }
    return values;
  }
  
}
