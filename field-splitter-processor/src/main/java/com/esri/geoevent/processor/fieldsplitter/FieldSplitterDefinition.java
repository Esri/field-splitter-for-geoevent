package com.esri.geoevent.processor.fieldsplitter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.processor.GeoEventProcessorDefinitionBase;

public class FieldSplitterDefinition extends GeoEventProcessorDefinitionBase
{
  final private static Log LOG = LogFactory.getLog(FieldSplitterDefinition.class);

  public FieldSplitterDefinition()
  {
    try
    {
      propertyDefinitions.put("fieldToSplit", new PropertyDefinition("fieldToSplit", PropertyType.String, "GeoTagged", "Field to Split", "Field name its value to be split into individual GeoEvents", false, false));
      propertyDefinitions.put("fieldSplitter", new PropertyDefinition("fieldSplitter", PropertyType.String, ",", "Field Splitter", "Field splitter characters", false, false));
    }
    catch (Exception e)
    {
      LOG.error("Error setting up Event Counter Definition.", e);
    }
  }

  @Override
  public String getVersion()
  {
    return "10.5.0";
  }

  @Override
  public String getDomain()
  {
    return "com.esri.geoevent.processor";
  }

  @Override
  public String getName()
  {
    return "FieldSplitter";
  }

  @Override
  public String getLabel()
  {
    return "Field Splitter";
  }

  @Override
  public String getDescription()
  {
    return "Split field with comma delimited values into individual GeoEvents";
  }

  @Override
  public String getContactInfo()
  {
    return "mpilouk@esri.com";
  }

}
