/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package backtype.storm.generated;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-10-20")
public class RebalanceOptions implements org.apache.thrift.TBase<RebalanceOptions, RebalanceOptions._Fields>, java.io.Serializable, Cloneable, Comparable<RebalanceOptions> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("RebalanceOptions");

  private static final org.apache.thrift.protocol.TField WAIT_SECS_FIELD_DESC = new org.apache.thrift.protocol.TField("wait_secs", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField REASSIGN_FIELD_DESC = new org.apache.thrift.protocol.TField("reassign", org.apache.thrift.protocol.TType.BOOL, (short)2);
  private static final org.apache.thrift.protocol.TField CONF_FIELD_DESC = new org.apache.thrift.protocol.TField("conf", org.apache.thrift.protocol.TType.STRING, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new RebalanceOptionsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new RebalanceOptionsTupleSchemeFactory());
  }

  private int wait_secs; // optional
  private boolean reassign; // optional
  private String conf; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    WAIT_SECS((short)1, "wait_secs"),
    REASSIGN((short)2, "reassign"),
    CONF((short)3, "conf");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // WAIT_SECS
          return WAIT_SECS;
        case 2: // REASSIGN
          return REASSIGN;
        case 3: // CONF
          return CONF;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __WAIT_SECS_ISSET_ID = 0;
  private static final int __REASSIGN_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.WAIT_SECS,_Fields.REASSIGN,_Fields.CONF};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.WAIT_SECS, new org.apache.thrift.meta_data.FieldMetaData("wait_secs", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.REASSIGN, new org.apache.thrift.meta_data.FieldMetaData("reassign", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.CONF, new org.apache.thrift.meta_data.FieldMetaData("conf", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(RebalanceOptions.class, metaDataMap);
  }

  public RebalanceOptions() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public RebalanceOptions(RebalanceOptions other) {
    __isset_bitfield = other.__isset_bitfield;
    this.wait_secs = other.wait_secs;
    this.reassign = other.reassign;
    if (other.is_set_conf()) {
      this.conf = other.conf;
    }
  }

  public RebalanceOptions deepCopy() {
    return new RebalanceOptions(this);
  }

  @Override
  public void clear() {
    set_wait_secs_isSet(false);
    this.wait_secs = 0;
    set_reassign_isSet(false);
    this.reassign = false;
    this.conf = null;
  }

  public int get_wait_secs() {
    return this.wait_secs;
  }

  public void set_wait_secs(int wait_secs) {
    this.wait_secs = wait_secs;
    set_wait_secs_isSet(true);
  }

  public void unset_wait_secs() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __WAIT_SECS_ISSET_ID);
  }

  /** Returns true if field wait_secs is set (has been assigned a value) and false otherwise */
  public boolean is_set_wait_secs() {
    return EncodingUtils.testBit(__isset_bitfield, __WAIT_SECS_ISSET_ID);
  }

  public void set_wait_secs_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __WAIT_SECS_ISSET_ID, value);
  }

  public boolean is_reassign() {
    return this.reassign;
  }

  public void set_reassign(boolean reassign) {
    this.reassign = reassign;
    set_reassign_isSet(true);
  }

  public void unset_reassign() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __REASSIGN_ISSET_ID);
  }

  /** Returns true if field reassign is set (has been assigned a value) and false otherwise */
  public boolean is_set_reassign() {
    return EncodingUtils.testBit(__isset_bitfield, __REASSIGN_ISSET_ID);
  }

  public void set_reassign_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __REASSIGN_ISSET_ID, value);
  }

  public String get_conf() {
    return this.conf;
  }

  public void set_conf(String conf) {
    this.conf = conf;
  }

  public void unset_conf() {
    this.conf = null;
  }

  /** Returns true if field conf is set (has been assigned a value) and false otherwise */
  public boolean is_set_conf() {
    return this.conf != null;
  }

  public void set_conf_isSet(boolean value) {
    if (!value) {
      this.conf = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case WAIT_SECS:
      if (value == null) {
        unset_wait_secs();
      } else {
        set_wait_secs((Integer)value);
      }
      break;

    case REASSIGN:
      if (value == null) {
        unset_reassign();
      } else {
        set_reassign((Boolean)value);
      }
      break;

    case CONF:
      if (value == null) {
        unset_conf();
      } else {
        set_conf((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case WAIT_SECS:
      return Integer.valueOf(get_wait_secs());

    case REASSIGN:
      return Boolean.valueOf(is_reassign());

    case CONF:
      return get_conf();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case WAIT_SECS:
      return is_set_wait_secs();
    case REASSIGN:
      return is_set_reassign();
    case CONF:
      return is_set_conf();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof RebalanceOptions)
      return this.equals((RebalanceOptions)that);
    return false;
  }

  public boolean equals(RebalanceOptions that) {
    if (that == null)
      return false;

    boolean this_present_wait_secs = true && this.is_set_wait_secs();
    boolean that_present_wait_secs = true && that.is_set_wait_secs();
    if (this_present_wait_secs || that_present_wait_secs) {
      if (!(this_present_wait_secs && that_present_wait_secs))
        return false;
      if (this.wait_secs != that.wait_secs)
        return false;
    }

    boolean this_present_reassign = true && this.is_set_reassign();
    boolean that_present_reassign = true && that.is_set_reassign();
    if (this_present_reassign || that_present_reassign) {
      if (!(this_present_reassign && that_present_reassign))
        return false;
      if (this.reassign != that.reassign)
        return false;
    }

    boolean this_present_conf = true && this.is_set_conf();
    boolean that_present_conf = true && that.is_set_conf();
    if (this_present_conf || that_present_conf) {
      if (!(this_present_conf && that_present_conf))
        return false;
      if (!this.conf.equals(that.conf))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_wait_secs = true && (is_set_wait_secs());
    list.add(present_wait_secs);
    if (present_wait_secs)
      list.add(wait_secs);

    boolean present_reassign = true && (is_set_reassign());
    list.add(present_reassign);
    if (present_reassign)
      list.add(reassign);

    boolean present_conf = true && (is_set_conf());
    list.add(present_conf);
    if (present_conf)
      list.add(conf);

    return list.hashCode();
  }

  @Override
  public int compareTo(RebalanceOptions other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_wait_secs()).compareTo(other.is_set_wait_secs());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_wait_secs()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.wait_secs, other.wait_secs);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_reassign()).compareTo(other.is_set_reassign());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_reassign()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.reassign, other.reassign);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_conf()).compareTo(other.is_set_conf());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_conf()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.conf, other.conf);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("RebalanceOptions(");
    boolean first = true;

    if (is_set_wait_secs()) {
      sb.append("wait_secs:");
      sb.append(this.wait_secs);
      first = false;
    }
    if (is_set_reassign()) {
      if (!first) sb.append(", ");
      sb.append("reassign:");
      sb.append(this.reassign);
      first = false;
    }
    if (is_set_conf()) {
      if (!first) sb.append(", ");
      sb.append("conf:");
      if (this.conf == null) {
        sb.append("null");
      } else {
        sb.append(this.conf);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class RebalanceOptionsStandardSchemeFactory implements SchemeFactory {
    public RebalanceOptionsStandardScheme getScheme() {
      return new RebalanceOptionsStandardScheme();
    }
  }

  private static class RebalanceOptionsStandardScheme extends StandardScheme<RebalanceOptions> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, RebalanceOptions struct) throws TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // WAIT_SECS
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.wait_secs = iprot.readI32();
              struct.set_wait_secs_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // REASSIGN
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.reassign = iprot.readBool();
              struct.set_reassign_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // CONF
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.conf = iprot.readString();
              struct.set_conf_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, RebalanceOptions struct) throws TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.is_set_wait_secs()) {
        oprot.writeFieldBegin(WAIT_SECS_FIELD_DESC);
        oprot.writeI32(struct.wait_secs);
        oprot.writeFieldEnd();
      }
      if (struct.is_set_reassign()) {
        oprot.writeFieldBegin(REASSIGN_FIELD_DESC);
        oprot.writeBool(struct.reassign);
        oprot.writeFieldEnd();
      }
      if (struct.conf != null) {
        if (struct.is_set_conf()) {
          oprot.writeFieldBegin(CONF_FIELD_DESC);
          oprot.writeString(struct.conf);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class RebalanceOptionsTupleSchemeFactory implements SchemeFactory {
    public RebalanceOptionsTupleScheme getScheme() {
      return new RebalanceOptionsTupleScheme();
    }
  }

  private static class RebalanceOptionsTupleScheme extends TupleScheme<RebalanceOptions> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, RebalanceOptions struct) throws TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.is_set_wait_secs()) {
        optionals.set(0);
      }
      if (struct.is_set_reassign()) {
        optionals.set(1);
      }
      if (struct.is_set_conf()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.is_set_wait_secs()) {
        oprot.writeI32(struct.wait_secs);
      }
      if (struct.is_set_reassign()) {
        oprot.writeBool(struct.reassign);
      }
      if (struct.is_set_conf()) {
        oprot.writeString(struct.conf);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, RebalanceOptions struct) throws TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.wait_secs = iprot.readI32();
        struct.set_wait_secs_isSet(true);
      }
      if (incoming.get(1)) {
        struct.reassign = iprot.readBool();
        struct.set_reassign_isSet(true);
      }
      if (incoming.get(2)) {
        struct.conf = iprot.readString();
        struct.set_conf_isSet(true);
      }
    }
  }

}

