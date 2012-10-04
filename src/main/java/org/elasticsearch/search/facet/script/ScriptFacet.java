package org.elasticsearch.search.facet.script;

import org.elasticsearch.search.facet.Facet;

/**
 *
 */
public interface ScriptFacet extends Facet {
    /**
     * The type of the filter facet.
     */
    public static final String TYPE = "script";

    public Object facet();

    public Object getFacet();
}
