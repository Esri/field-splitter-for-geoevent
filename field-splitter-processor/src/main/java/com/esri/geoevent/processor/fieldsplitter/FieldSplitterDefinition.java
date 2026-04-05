/*
  Copyright 2017 Esri

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.​

  For additional information, contact:
  Environmental Systems Research Institute, Inc.
  Attn: Contracts Dept
  380 New York Street
  Redlands, California, USA 92373

  email: contracts@esri.com
*/

package com.esri.geoevent.processor.fieldsplitter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.processor.GeoEventProcessorDefinitionBase;

public class FieldSplitterDefinition extends GeoEventProcessorDefinitionBase
{
  private static final BundleLogger LOG = BundleLoggerFactory.getLogger(FieldSplitter.class);
  private static final String RESOURCE_PATH = "com/esri/geoevent/processor/field-splitter-processor.properties";
  private static final String BUNDLE_SYMBOLIC_NAME = "com.esri.geoevent.processor.field-splitter-processor";

  static final String PROPERTY_FIELD_TO_SPLIT = "fieldToSplit";
  static final String PROPERTY_FIELD_SPLITTER = "fieldSplitter";

  private static final String KEY_DEFAULT_FIELD_TO_SPLIT = "default.fieldToSplit";
  private static final String KEY_DEFAULT_FIELD_SPLITTER = "default.fieldSplitter";
  private static final String KEY_PROCESSOR_VERSION = "PROCESSOR_VERSION";
  private static final String KEY_PROCESSOR_NAME = "PROCESSOR_NAME";
  private static final String KEY_PROCESSOR_DOMAIN = "PROCESSOR_DOMAIN";
  private static final String FALLBACK_FIELD_TO_SPLIT = "GeoTagged";
  private static final String FALLBACK_FIELD_SPLITTER = ",";
  private static final String FALLBACK_PROCESSOR_VERSION = "10.6.0";
  private static final String FALLBACK_PROCESSOR_NAME = "FieldSplitter";
  private static final String FALLBACK_PROCESSOR_DOMAIN = "com.esri.geoevent.processor";

  private static final Properties DEFAULTS = loadDefaults();
  private static final String DEFAULT_FIELD_TO_SPLIT = getConfiguredValue(KEY_DEFAULT_FIELD_TO_SPLIT, FALLBACK_FIELD_TO_SPLIT);
  private static final String DEFAULT_FIELD_SPLITTER = getConfiguredValue(KEY_DEFAULT_FIELD_SPLITTER, FALLBACK_FIELD_SPLITTER);
  private static final String PROCESSOR_VERSION = getConfiguredValue(KEY_PROCESSOR_VERSION, FALLBACK_PROCESSOR_VERSION);
  private static final String PROCESSOR_NAME = getConfiguredValue(KEY_PROCESSOR_NAME, FALLBACK_PROCESSOR_NAME);
  private static final String PROCESSOR_DOMAIN = getConfiguredValue(KEY_PROCESSOR_DOMAIN, FALLBACK_PROCESSOR_DOMAIN);

  static final String LOG_PROPERTY_DEFINITION_ERROR = "${" + BUNDLE_SYMBOLIC_NAME + ".PROPERTY_DEFINITION_ERROR}";
  static final String LOG_RESOURCE_NOT_FOUND = "${" + BUNDLE_SYMBOLIC_NAME + ".RESOURCE_NOT_FOUND}";
  static final String LOG_RESOURCE_LOAD_FAILED = "${" + BUNDLE_SYMBOLIC_NAME + ".RESOURCE_LOAD_FAILED}";
  static final String LOG_PROCESSOR_INSTANTIATED = "${" + BUNDLE_SYMBOLIC_NAME + ".PROCESSOR_INSTANTIATED}";
  static final String LOG_SERVICE_INSTANTIATED = "${" + BUNDLE_SYMBOLIC_NAME + ".SERVICE_INSTANTIATED}";
  static final String LOG_SPLIT_FAILED_FIELD = "${" + BUNDLE_SYMBOLIC_NAME + ".SPLIT_FAILED_FIELD}";
  static final String LOG_SPLIT_FAILED = "${" + BUNDLE_SYMBOLIC_NAME + ".SPLIT_FAILED}";
  static final String LOG_SPLIT_FIELD_NOT_FOUND = "${" + BUNDLE_SYMBOLIC_NAME + ".SPLIT_FIELD_NOT_FOUND}";
  static final String LOG_SPLIT_FIELD_NULL = "${" + BUNDLE_SYMBOLIC_NAME + ".SPLIT_FIELD_NULL}";
  static final String LOG_PROCESSOR_NOT_INITIALIZED = "${" + BUNDLE_SYMBOLIC_NAME + ".PROCESSOR_NOT_INITIALIZED}";
  static final String LOG_VALIDATION_FAILED = "${" + BUNDLE_SYMBOLIC_NAME + ".VALIDATION_FAILED}";

  private static final String PROCESSOR_LABEL = "${" + BUNDLE_SYMBOLIC_NAME + ".PROCESSOR_LABEL}";
  private static final String PROCESSOR_DESC = "${" + BUNDLE_SYMBOLIC_NAME + ".PROCESSOR_DESC}";
  private static final String PROCESSOR_CONTACT = "${" + BUNDLE_SYMBOLIC_NAME + ".PROCESSOR_CONTACT}";
  private static final String FIELD_TO_SPLIT_LABEL = "${" + BUNDLE_SYMBOLIC_NAME + ".FIELD_TO_SPLIT_LABEL}";
  private static final String FIELD_TO_SPLIT_DESC = "${" + BUNDLE_SYMBOLIC_NAME + ".FIELD_TO_SPLIT_DESC}";
  private static final String FIELD_SPLITTER_LABEL = "${" + BUNDLE_SYMBOLIC_NAME + ".FIELD_SPLITTER_LABEL}";
  private static final String FIELD_SPLITTER_DESC = "${" + BUNDLE_SYMBOLIC_NAME + ".FIELD_SPLITTER_DESC}";

  public FieldSplitterDefinition()
  {
    try
    {
      propertyDefinitions.put(PROPERTY_FIELD_TO_SPLIT, new PropertyDefinition(PROPERTY_FIELD_TO_SPLIT, PropertyType.String, DEFAULT_FIELD_TO_SPLIT, FIELD_TO_SPLIT_LABEL, FIELD_TO_SPLIT_DESC, false, false));
      propertyDefinitions.put(PROPERTY_FIELD_SPLITTER, new PropertyDefinition(PROPERTY_FIELD_SPLITTER, PropertyType.String, DEFAULT_FIELD_SPLITTER, FIELD_SPLITTER_LABEL, FIELD_SPLITTER_DESC, false, false));
    }
    catch (PropertyException ex)
    {
      LOG.error(LOG_PROPERTY_DEFINITION_ERROR, ex);
    }
  }

  @Override
  public String getVersion()
  {
    return PROCESSOR_VERSION;
  }

  @Override
  public String getDomain()
  {
    return PROCESSOR_DOMAIN;
  }

  @Override
  public String getName()
  {
    return PROCESSOR_NAME;
  }

  @Override
  public String getLabel()
  {
    return PROCESSOR_LABEL;
  }

  @Override
  public String getDescription()
  {
    return PROCESSOR_DESC;
  }

  @Override
  public String getContactInfo()
  {
    return PROCESSOR_CONTACT;
  }

  private static Properties loadDefaults()
  {
    Properties properties = new Properties();
    try (InputStream inputStream = FieldSplitterDefinition.class.getClassLoader().getResourceAsStream(RESOURCE_PATH))
    {
      if (inputStream == null)
      {
        LOG.warn(LOG_RESOURCE_NOT_FOUND, RESOURCE_PATH);
        return properties;
      }

      properties.load(inputStream);
    }
    catch (IOException e)
    {
      LOG.warn(LOG_RESOURCE_LOAD_FAILED, e, RESOURCE_PATH);
    }
    return properties;
  }

  private static String getConfiguredValue(String key, String fallback)
  {
    String value = DEFAULTS.getProperty(key);
    if (value == null)
      return fallback;

    String normalizedValue = value.trim();
    if (normalizedValue.isEmpty() || normalizedValue.contains("${"))
      return fallback;

    return normalizedValue;
  }
}
