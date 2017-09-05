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
