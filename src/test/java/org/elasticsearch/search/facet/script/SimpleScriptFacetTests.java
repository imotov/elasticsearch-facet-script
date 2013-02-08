package org.elasticsearch.search.facet.script;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleScriptFacetTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder().put("index.number_of_shards", numberOfShards()).put("index.number_of_replicas", 0).build();
        for (int i = 0; i < numberOfNodes(); i++) {
            startNode("node" + i, settings);
        }
        client = getClient();
    }

    protected int numberOfShards() {
        return 1;
    }

    protected int numberOfNodes() {
        return 1;
    }

    protected int numberOfRuns() {
        return 5;
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node0");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBinaryFacet() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().indices().preparePutMapping("test")
                .setType("type1")
                .setSource("{ type1 : { properties : { tag : { type : \"string\", store : \"yes\" } } } }")
                .execute().actionGet();


        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        for (int i = 0; i < 10; i++) {
            client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                    .field("tag", "green")
                    .endObject()).execute().actionGet();
        }

        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                    .field("tag", "blue")
                    .endObject()).execute().actionGet();
        }

        client.admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setIndices("test")
                    .setSearchType(SearchType.COUNT)
                    .setExtraSource(XContentFactory.jsonBuilder().startObject()
                            .startObject("facets")
                            .startObject("facet1")
                            .startObject("script")
                            .field("init_script", "" +
                                    "def add(map, key, n) {" +
                                    "   map[key] = map.containsKey(key) ? map[key] + n : n;" +
                                    "}")
                            .field("map_script", "add(facet, _fields['tag'].value, 1)")
                            .field("combine_script", "facet")
                            .field("reduce_script", "result = null;" +
                                    "for (f : facets) {" +
                                    "  if (result != null) {" +
                                    "    for (e : f.entrySet()) {" +
                                    "       result[e.key] = result.containsKey(e.key) ? result[e.key] + e.value : e.value;" +
                                    "    }" +
                                    "  } else {" +
                                    "    result = f;" +
                                    "  }" +
                                    "};" +
                                    "result")
                            .startObject("params")
                            .startObject("facet")
                            .endObject()
                            .endObject()
                            .endObject()
                            .endObject()
                            .endObject()
                            .endObject())
                    .execute().actionGet();

            logger.trace(searchResponse.toString());
            assertThat(searchResponse.hits().totalHits(), equalTo(15l));
            assertThat(searchResponse.hits().hits().length, equalTo(0));
            ScriptFacet facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            Map<String, Object> facetResult = (Map<String, Object>) facet.facet();
            assertThat(facetResult.size(), equalTo(2));
            assertThat(facetResult.get("green"), equalTo((Object) 10));
            assertThat(facetResult.get("blue"), equalTo((Object) 5));
        }
    }


    @Test
    public void testUpdateFacet() throws Exception {
        try {
            client.admin().indices().prepareDelete("test1").execute().actionGet();
            client.admin().indices().prepareDelete("test2").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test1").execute().actionGet();
        client.admin().indices().preparePutMapping("test1")
                .setType("type1")
                .setSource("{ type1 : { properties : { tag : { type : \"string\", store : \"yes\" } } } }")
                .execute().actionGet();

        client.admin().indices().prepareCreate("test2").execute().actionGet();
        client.admin().indices().preparePutMapping("test2")
                .setType("type1")
                .setSource("{ type1 : { properties : { tag : { type : \"string\", store : \"yes\" } } } }")
                .execute().actionGet();


        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        for (int i = 0; i < 10; i++) {
            client.prepareIndex("test1", "type1").setSource(jsonBuilder().startObject()
                    .field("tag", "green")
                    .endObject()).execute().actionGet();
        }

        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test1", "type1").setSource(jsonBuilder().startObject()
                    .field("tag", "blue")
                    .endObject()).execute().actionGet();
        }

        for (int i = 0; i < 10; i++) {
            client.prepareIndex("test2", "type1").setSource(jsonBuilder().startObject()
                    .field("tag", "yellow")
                    .endObject()).execute().actionGet();
        }

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setIndices("test1", "test2")
                .setExtraSource(XContentFactory.jsonBuilder().startObject()
                        .startObject("facets")
                        .startObject("facet1")
                        .startObject("script")
                        .field("init_script", "index = _ctx.request().index();")
                        .field("map_script", "" +
                                "uid = doc._uid.value;" +
                                "id = org.elasticsearch.index.mapper.Uid.idFromUid(uid);" +
                                "type = org.elasticsearch.index.mapper.Uid.typeFromUid(uid);" +
                                "if (!_source.isEmpty()) {" +
                                "  modified = true;" +
                                "  map = _source.source();" +
                                "  complementary = colors.get(map.tag);" +
                                "  if (complementary != null) {" +
                                "    map.put(\"complementary\", complementary);" +
                                "    _client.prepareIndex(index, type, id).setSource(map).execute().actionGet();" +
                                "  }" +
                                "}")
                        .startObject("params")
                        .startObject("facet")
                        .endObject()
                        .startObject("colors")
                        .field("blue", "orange")
                        .field("yellow", "violet")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject())
                .execute().actionGet();

        logger.trace(searchResponse.toString());
        assertThat(searchResponse.hits().totalHits(), equalTo(25l));
        assertThat(searchResponse.hits().hits().length, equalTo(0));

        client.admin().indices().prepareRefresh().execute().actionGet();

        searchResponse = client.prepareSearch()
                .setIndices("test1", "test2")
                .setQuery(QueryBuilders.matchQuery("complementary", "orange"))
                .execute().actionGet();

        assertThat(searchResponse.hits().totalHits(), equalTo(5L));

        searchResponse = client.prepareSearch()
                .setIndices("test2")
                .setQuery(QueryBuilders.matchQuery("complementary", "violet"))
                .execute().actionGet();

        assertThat(searchResponse.hits().totalHits(), equalTo(10L));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCharFrequencies() throws Exception {
        try {
            client.admin().indices().prepareDelete("test1").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test1").execute().actionGet();
        client.admin().indices().preparePutMapping("test1")
                .setType("type1")
                .setSource("{ \"type1\" : { \"properties\" : { \"message\" : { \"type\" : \"string\", \"store\" : \"yes\" } } } }")
                .execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test1", "type1").setSource(jsonBuilder().startObject()
                .field("message", "ABCD ABCDEF")
                .endObject()).execute().actionGet();

        client.prepareIndex("test1", "type1").setSource(jsonBuilder().startObject()
                .field("message", "EFGHIJ")
                .endObject()).execute().actionGet();

        client.prepareIndex("test1", "type1").setSource(jsonBuilder().startObject()
                .field("message", "IJKLMNOP")
                .endObject()).execute().actionGet();

        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setIndices("test1")
                .setExtraSource(XContentFactory.jsonBuilder().startObject()
                        .startObject("facets")
                        .startObject("facet1")
                        .startObject("script")
                        .field("init_script", "charfreq_init")
                        .field("map_script", "charfreq_map")
                        .field("reduce_script", "charfreq_reduce")
                        .startObject("params")
                        .startArray("facet")
                        .endArray()
                        .field("field", "message")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject())
                .execute().actionGet();

        logger.trace(searchResponse.toString());
        assertThat(searchResponse.hits().totalHits(), equalTo(3l));
        assertThat(searchResponse.hits().hits().length, equalTo(0));

        ScriptFacet facet = searchResponse.facets().facet("facet1");
        assertThat(facet.name(), equalTo("facet1"));
        Map<String, Object> facetResult = (Map<String, Object>) facet.facet();
        assertThat(facetResult.get("total"), equalTo((Object) 24));
        assertThat((ArrayList<Integer>) facetResult.get("counts"),
                //       A  B  C  D  E  F  G  H  I  J  K  L  M  N  O  P  Q  R  S  T  U  V  W  X  Y  Z
                contains(2, 2, 2, 2, 2, 2, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
    }
}
