package org.elasticsearch.search.facet.script;

/**
 *
 */
public class SimpleScriptFacetMultiShardTests extends SimpleScriptFacetTests {

    @Override
    protected int numberOfShards() {
        return 3;
    }

}
