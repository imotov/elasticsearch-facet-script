package org.elasticsearch.search.facet.script;

import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.FacetBuilder;

import java.io.IOException;
import java.util.Map;

public class ScriptFacetBuilder extends FacetBuilder {
  
  private Map<String, Object> params;
  private Map<String, Object> reduceParams;
  private String lang;
  private String initScript;
  private String mapScript;
  private String combineScript;
  private String reduceScript;
  
  public ScriptFacetBuilder(String name) {
    super(name);
  }
  
  public ScriptFacetBuilder param(String name, Object value) {
    if (params == null) {
      params = Maps.newHashMap();
    }
    params.put(name, value);
    return this;
  }

  public ScriptFacetBuilder reduceParam(String name, Object value) {
    if (reduceParams == null) {
      reduceParams = Maps.newHashMap();
    }
    reduceParams.put(name, value);
    return this;
  }
  
  public ScriptFacetBuilder lang(String lang) {
    this.lang = lang;
    return this;
  }

  public ScriptFacetBuilder initScript(String scriptName) {
    this.initScript = scriptName;
    return this;
  }

  public ScriptFacetBuilder mapScript(String scriptName) {
    this.mapScript = scriptName;
    return this;
  }

  public ScriptFacetBuilder combineScript(String scriptName) {
    this.combineScript = scriptName;
    return this;
  }

  public ScriptFacetBuilder reduceScript(String scriptName) {
    this.reduceScript = scriptName;
    return this;
  }

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    if (mapScript == null) {
      throw new SearchSourceBuilderException("mapScript must be set in ScriptFacetBuilder for facet [" + name + "]");
    }
    builder.startObject(name);
    
    builder.startObject("script");
    if (lang != null) {
      builder.field("lang", lang);
    }
    if (this.params != null) {
      builder.field("params", this.params);
    }
    if (this.reduceParams != null) {
      builder.field("reduce_params", this.reduceParams);
    }
    if (initScript != null) {
      builder.field("init_script", initScript);
    }
    if (mapScript != null) {
      builder.field("map_script", mapScript);
    }
    if (combineScript != null) {
      builder.field("combine_script", combineScript);
    }
    if (reduceScript != null) {
      builder.field("reduce_script", reduceScript);
    }

    builder.endObject();
    
    addFilterFacetAndGlobal(builder, params);
    
    builder.endObject();
    return builder;
  }
}
