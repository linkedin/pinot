/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.pinot.common.request;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
/**
 * AUTO GENERATED: DO NOT EDIT
 *  Aggregation
 * 
 */
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.12.0)", date = "2019-07-19")
public class AggregationInfo implements org.apache.thrift.TBase<AggregationInfo, AggregationInfo._Fields>, java.io.Serializable, Cloneable, Comparable<AggregationInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("AggregationInfo");

  private static final org.apache.thrift.protocol.TField AGGREGATION_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("aggregationType", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField AGGREGATION_PARAMS_FIELD_DESC = new org.apache.thrift.protocol.TField("aggregationParams", org.apache.thrift.protocol.TType.MAP, (short)2);
  private static final org.apache.thrift.protocol.TField IS_IN_SELECT_LIST_FIELD_DESC = new org.apache.thrift.protocol.TField("isInSelectList", org.apache.thrift.protocol.TType.BOOL, (short)3);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new AggregationInfoStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new AggregationInfoTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.lang.String aggregationType; // optional
  public @org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> aggregationParams; // optional
  public boolean isInSelectList; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    AGGREGATION_TYPE((short)1, "aggregationType"),
    AGGREGATION_PARAMS((short)2, "aggregationParams"),
    IS_IN_SELECT_LIST((short)3, "isInSelectList");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // AGGREGATION_TYPE
          return AGGREGATION_TYPE;
        case 2: // AGGREGATION_PARAMS
          return AGGREGATION_PARAMS;
        case 3: // IS_IN_SELECT_LIST
          return IS_IN_SELECT_LIST;
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
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __ISINSELECTLIST_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.AGGREGATION_TYPE,_Fields.AGGREGATION_PARAMS,_Fields.IS_IN_SELECT_LIST};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.AGGREGATION_TYPE, new org.apache.thrift.meta_data.FieldMetaData("aggregationType", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.AGGREGATION_PARAMS, new org.apache.thrift.meta_data.FieldMetaData("aggregationParams", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.IS_IN_SELECT_LIST, new org.apache.thrift.meta_data.FieldMetaData("isInSelectList", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(AggregationInfo.class, metaDataMap);
  }

  public AggregationInfo() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public AggregationInfo(AggregationInfo other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetAggregationType()) {
      this.aggregationType = other.aggregationType;
    }
    if (other.isSetAggregationParams()) {
      java.util.Map<java.lang.String,java.lang.String> __this__aggregationParams = new java.util.HashMap<java.lang.String,java.lang.String>(other.aggregationParams);
      this.aggregationParams = __this__aggregationParams;
    }
    this.isInSelectList = other.isInSelectList;
  }

  public AggregationInfo deepCopy() {
    return new AggregationInfo(this);
  }

  @Override
  public void clear() {
    this.aggregationType = null;
    this.aggregationParams = null;
    setIsInSelectListIsSet(false);
    this.isInSelectList = false;
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getAggregationType() {
    return this.aggregationType;
  }

  public AggregationInfo setAggregationType(@org.apache.thrift.annotation.Nullable java.lang.String aggregationType) {
    this.aggregationType = aggregationType;
    return this;
  }

  public void unsetAggregationType() {
    this.aggregationType = null;
  }

  /** Returns true if field aggregationType is set (has been assigned a value) and false otherwise */
  public boolean isSetAggregationType() {
    return this.aggregationType != null;
  }

  public void setAggregationTypeIsSet(boolean value) {
    if (!value) {
      this.aggregationType = null;
    }
  }

  public int getAggregationParamsSize() {
    return (this.aggregationParams == null) ? 0 : this.aggregationParams.size();
  }

  public void putToAggregationParams(java.lang.String key, java.lang.String val) {
    if (this.aggregationParams == null) {
      this.aggregationParams = new java.util.HashMap<java.lang.String,java.lang.String>();
    }
    this.aggregationParams.put(key, val);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Map<java.lang.String,java.lang.String> getAggregationParams() {
    return this.aggregationParams;
  }

  public AggregationInfo setAggregationParams(@org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> aggregationParams) {
    this.aggregationParams = aggregationParams;
    return this;
  }

  public void unsetAggregationParams() {
    this.aggregationParams = null;
  }

  /** Returns true if field aggregationParams is set (has been assigned a value) and false otherwise */
  public boolean isSetAggregationParams() {
    return this.aggregationParams != null;
  }

  public void setAggregationParamsIsSet(boolean value) {
    if (!value) {
      this.aggregationParams = null;
    }
  }

  public boolean isIsInSelectList() {
    return this.isInSelectList;
  }

  public AggregationInfo setIsInSelectList(boolean isInSelectList) {
    this.isInSelectList = isInSelectList;
    setIsInSelectListIsSet(true);
    return this;
  }

  public void unsetIsInSelectList() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __ISINSELECTLIST_ISSET_ID);
  }

  /** Returns true if field isInSelectList is set (has been assigned a value) and false otherwise */
  public boolean isSetIsInSelectList() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __ISINSELECTLIST_ISSET_ID);
  }

  public void setIsInSelectListIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __ISINSELECTLIST_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case AGGREGATION_TYPE:
      if (value == null) {
        unsetAggregationType();
      } else {
        setAggregationType((java.lang.String)value);
      }
      break;

    case AGGREGATION_PARAMS:
      if (value == null) {
        unsetAggregationParams();
      } else {
        setAggregationParams((java.util.Map<java.lang.String,java.lang.String>)value);
      }
      break;

    case IS_IN_SELECT_LIST:
      if (value == null) {
        unsetIsInSelectList();
      } else {
        setIsInSelectList((java.lang.Boolean)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case AGGREGATION_TYPE:
      return getAggregationType();

    case AGGREGATION_PARAMS:
      return getAggregationParams();

    case IS_IN_SELECT_LIST:
      return isIsInSelectList();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case AGGREGATION_TYPE:
      return isSetAggregationType();
    case AGGREGATION_PARAMS:
      return isSetAggregationParams();
    case IS_IN_SELECT_LIST:
      return isSetIsInSelectList();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof AggregationInfo)
      return this.equals((AggregationInfo)that);
    return false;
  }

  public boolean equals(AggregationInfo that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_aggregationType = true && this.isSetAggregationType();
    boolean that_present_aggregationType = true && that.isSetAggregationType();
    if (this_present_aggregationType || that_present_aggregationType) {
      if (!(this_present_aggregationType && that_present_aggregationType))
        return false;
      if (!this.aggregationType.equals(that.aggregationType))
        return false;
    }

    boolean this_present_aggregationParams = true && this.isSetAggregationParams();
    boolean that_present_aggregationParams = true && that.isSetAggregationParams();
    if (this_present_aggregationParams || that_present_aggregationParams) {
      if (!(this_present_aggregationParams && that_present_aggregationParams))
        return false;
      if (!this.aggregationParams.equals(that.aggregationParams))
        return false;
    }

    boolean this_present_isInSelectList = true && this.isSetIsInSelectList();
    boolean that_present_isInSelectList = true && that.isSetIsInSelectList();
    if (this_present_isInSelectList || that_present_isInSelectList) {
      if (!(this_present_isInSelectList && that_present_isInSelectList))
        return false;
      if (this.isInSelectList != that.isInSelectList)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetAggregationType()) ? 131071 : 524287);
    if (isSetAggregationType())
      hashCode = hashCode * 8191 + aggregationType.hashCode();

    hashCode = hashCode * 8191 + ((isSetAggregationParams()) ? 131071 : 524287);
    if (isSetAggregationParams())
      hashCode = hashCode * 8191 + aggregationParams.hashCode();

    hashCode = hashCode * 8191 + ((isSetIsInSelectList()) ? 131071 : 524287);
    if (isSetIsInSelectList())
      hashCode = hashCode * 8191 + ((isInSelectList) ? 131071 : 524287);

    return hashCode;
  }

  @Override
  public int compareTo(AggregationInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetAggregationType()).compareTo(other.isSetAggregationType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAggregationType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.aggregationType, other.aggregationType);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetAggregationParams()).compareTo(other.isSetAggregationParams());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAggregationParams()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.aggregationParams, other.aggregationParams);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetIsInSelectList()).compareTo(other.isSetIsInSelectList());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetIsInSelectList()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.isInSelectList, other.isInSelectList);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("AggregationInfo(");
    boolean first = true;

    if (isSetAggregationType()) {
      sb.append("aggregationType:");
      if (this.aggregationType == null) {
        sb.append("null");
      } else {
        sb.append(this.aggregationType);
      }
      first = false;
    }
    if (isSetAggregationParams()) {
      if (!first) sb.append(", ");
      sb.append("aggregationParams:");
      if (this.aggregationParams == null) {
        sb.append("null");
      } else {
        sb.append(this.aggregationParams);
      }
      first = false;
    }
    if (isSetIsInSelectList()) {
      if (!first) sb.append(", ");
      sb.append("isInSelectList:");
      sb.append(this.isInSelectList);
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class AggregationInfoStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public AggregationInfoStandardScheme getScheme() {
      return new AggregationInfoStandardScheme();
    }
  }

  private static class AggregationInfoStandardScheme extends org.apache.thrift.scheme.StandardScheme<AggregationInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, AggregationInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // AGGREGATION_TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.aggregationType = iprot.readString();
              struct.setAggregationTypeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // AGGREGATION_PARAMS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map52 = iprot.readMapBegin();
                struct.aggregationParams = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map52.size);
                @org.apache.thrift.annotation.Nullable java.lang.String _key53;
                @org.apache.thrift.annotation.Nullable java.lang.String _val54;
                for (int _i55 = 0; _i55 < _map52.size; ++_i55)
                {
                  _key53 = iprot.readString();
                  _val54 = iprot.readString();
                  struct.aggregationParams.put(_key53, _val54);
                }
                iprot.readMapEnd();
              }
              struct.setAggregationParamsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // IS_IN_SELECT_LIST
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.isInSelectList = iprot.readBool();
              struct.setIsInSelectListIsSet(true);
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

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, AggregationInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.aggregationType != null) {
        if (struct.isSetAggregationType()) {
          oprot.writeFieldBegin(AGGREGATION_TYPE_FIELD_DESC);
          oprot.writeString(struct.aggregationType);
          oprot.writeFieldEnd();
        }
      }
      if (struct.aggregationParams != null) {
        if (struct.isSetAggregationParams()) {
          oprot.writeFieldBegin(AGGREGATION_PARAMS_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.aggregationParams.size()));
            for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter56 : struct.aggregationParams.entrySet())
            {
              oprot.writeString(_iter56.getKey());
              oprot.writeString(_iter56.getValue());
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.isSetIsInSelectList()) {
        oprot.writeFieldBegin(IS_IN_SELECT_LIST_FIELD_DESC);
        oprot.writeBool(struct.isInSelectList);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class AggregationInfoTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public AggregationInfoTupleScheme getScheme() {
      return new AggregationInfoTupleScheme();
    }
  }

  private static class AggregationInfoTupleScheme extends org.apache.thrift.scheme.TupleScheme<AggregationInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, AggregationInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetAggregationType()) {
        optionals.set(0);
      }
      if (struct.isSetAggregationParams()) {
        optionals.set(1);
      }
      if (struct.isSetIsInSelectList()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetAggregationType()) {
        oprot.writeString(struct.aggregationType);
      }
      if (struct.isSetAggregationParams()) {
        {
          oprot.writeI32(struct.aggregationParams.size());
          for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter57 : struct.aggregationParams.entrySet())
          {
            oprot.writeString(_iter57.getKey());
            oprot.writeString(_iter57.getValue());
          }
        }
      }
      if (struct.isSetIsInSelectList()) {
        oprot.writeBool(struct.isInSelectList);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, AggregationInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.aggregationType = iprot.readString();
        struct.setAggregationTypeIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TMap _map58 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.aggregationParams = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map58.size);
          @org.apache.thrift.annotation.Nullable java.lang.String _key59;
          @org.apache.thrift.annotation.Nullable java.lang.String _val60;
          for (int _i61 = 0; _i61 < _map58.size; ++_i61)
          {
            _key59 = iprot.readString();
            _val60 = iprot.readString();
            struct.aggregationParams.put(_key59, _val60);
          }
        }
        struct.setAggregationParamsIsSet(true);
      }
      if (incoming.get(2)) {
        struct.isInSelectList = iprot.readBool();
        struct.setIsInSelectListIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

