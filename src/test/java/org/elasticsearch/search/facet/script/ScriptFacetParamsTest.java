package org.elasticsearch.search.facet.script;

import static org.hamcrest.MatcherAssert.assertThat;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.AbstractExecutableScript;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class ScriptFacetParamsTest extends AbstractNodesTests {
  
  private static final Map<String, Object> CONTEXT = new HashMap<String, Object>();

  private final String INDEX = "test";
  private final String TYPE = "t";
  
  private Client client;
  
  
  // ---------------- pre/postconditions ----------------
  
  @BeforeClass
  public void createNodes() throws Exception {
    Settings settings = ImmutableSettings.settingsBuilder()
        .put("index.number_of_shards", numberOfShards())
        .put("index.number_of_replicas", 0)
        .put("script.native.test_init.type", InitScriptFactory.class)
        .put("script.native.test_map.type", MapScriptFactory.class)
        .put("script.native.test_combine.type", CombineScriptFactory.class)
        .put("script.native.test_reduce.type", ReduceScriptFactory.class)
        .build();
    for (int i = 0; i < numberOfNodes(); i++) {
      startNode("node" + i, settings);
    }
    client = getClient();
  }

  @AfterClass
  public void closeNodes() {
    client.close();
    closeAllNodes();
  }

  
  
  // ---------------- tests ----------------
  
  @SuppressWarnings("unchecked")
  @Test
  public void testInitParams() throws Exception {
    prepareIndexData(getClient());
    
    // query
    QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
    
    ScriptFacetBuilder facetBuilder = new ScriptFacetBuilder("test_facet")
      .initScript("test_init").mapScript("test_map")
      .combineScript("test_combine").reduceScript("test_reduce")
      .lang("native").param("p1", "val1").reduceParam("p2", "val2");
    
    SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch(INDEX)
        .setSearchType(SearchType.COUNT)
        .addFacet(facetBuilder)
        .setQuery(queryBuilder);
    
    searchRequestBuilder.execute().actionGet();
    
    // assertions
    Map<String, Object> initMap = (Map<String, Object>) CONTEXT.get("init");
    assertThat("init content", initMap.get("p1").equals("val1"));
    
    Map<String, Object> mapMap = (Map<String, Object>) CONTEXT.get("map");
    assertThat("map content", mapMap.get("p1").equals("val1"));
    
    Map<String, Object> combineMap = (Map<String, Object>) CONTEXT.get("combine");
    assertThat("combine content", combineMap.get("p1").equals("val1"));
    
    Map<String, Object> reduceMap = (Map<String, Object>) CONTEXT.get("reduce");
    assertThat("reduce content", reduceMap.get("p2").equals("val2"));
  }
  
  
  // ---------------- helper methods ----------------
  
  private void prepareIndexData(Client client) {
    
    try {
      client.admin().indices().prepareDelete("test").execute().actionGet();
    } catch (Exception e) {
      // ignore
    }
    client.admin().indices().prepareCreate("test").execute().actionGet();
    client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
    
    // add data
    client.prepareIndex(INDEX, TYPE, "1").setSource(ImmutableMap.<String, Object>of("field1", "valueA")).execute().actionGet();
    client.prepareIndex(INDEX, TYPE, "2").setSource(ImmutableMap.<String, Object>of("field1", "valueA")).execute().actionGet();
    client.prepareIndex(INDEX, TYPE, "3").setSource(ImmutableMap.<String, Object>of("field1", "valueA")).execute().actionGet();
    client.prepareIndex(INDEX, TYPE, "4").setSource(ImmutableMap.<String, Object>of("field1", "valueB")).execute().actionGet();
    client.prepareIndex(INDEX, TYPE, "5").setSource(ImmutableMap.<String, Object>of("field1", "valueB")).execute().actionGet();
    client.prepareIndex(INDEX, TYPE, "6").setSource(ImmutableMap.<String, Object>of("field1", "valueC")).execute().actionGet();
    
    // flush and refresh
    client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
  }
  
  protected int numberOfShards() {
    return 5;
  }

  protected int numberOfNodes() {
    return 2;
  }

  protected Client getClient() {
    return client("node0");
  }
  
  
  // Static classes with the factories and scripts
  // They set state in a shared context on their "run" methods
  // which are verified as assertions by the test
  
  static class InitScriptFactory implements NativeScriptFactory {
    @Override
    public ExecutableScript newScript(final Map<String, Object> params) {
      return new AbstractExecutableScript() {
        @Override
        public Object run() {
          CONTEXT.put("init", params);
          return null;
        }
      };
    }
  }
  
  static class MapScriptFactory implements NativeScriptFactory {
    @Override
    public ExecutableScript newScript(final Map<String, Object> params) {
      return new AbstractSearchScript() {
        @Override
        public Object run() {
          CONTEXT.put("map", params);
          return null;
        }
      };
    }
  }
  
  static class CombineScriptFactory implements NativeScriptFactory {
    @Override
    public ExecutableScript newScript(final Map<String, Object> params) {
      return new AbstractExecutableScript() {
        @Override
        public Object run() {
          CONTEXT.put("combine", params);
          return null;
        }
      };
    }
  }
  
  static class ReduceScriptFactory implements NativeScriptFactory {
    @Override
    public ExecutableScript newScript(final Map<String, Object> params) {
      return new AbstractExecutableScript() {
        @Override
        public Object run() {
          CONTEXT.put("reduce", params);
          return null;
        }
      };
    }
  }
}
