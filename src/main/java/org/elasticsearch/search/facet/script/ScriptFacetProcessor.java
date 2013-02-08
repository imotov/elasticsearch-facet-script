package org.elasticsearch.search.facet.script;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.collect.Lists.newArrayList;

/**
 *
 */
public class ScriptFacetProcessor extends AbstractComponent implements FacetProcessor {

    private final ScriptService scriptService;

    private final Client client;

    @Inject
    public ScriptFacetProcessor(Settings settings, ScriptService scriptService, Client client) {
        super(settings);
        InternalScriptFacet.registerStreams();
        this.scriptService = scriptService;
        this.client = client;
    }

    @Override
    public String[] types() {
        return new String[]{
                ScriptFacet.TYPE
        };
    }

    @Override
    public FacetCollector parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String initScript = null;
        String mapScript = null;
        String combineScript = null;
        String reduceScript = null;
        String scriptLang = null;
        Map<String, Object> params = null;
        XContentParser.Token token;
        String fieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(fieldName)) {
                    params = parser.map();
                }
            } else if (token.isValue()) {
                if ("init_script".equals(fieldName) || "initScript".equals(fieldName)) {
                    initScript = parser.text();
                } else if ("map_script".equals(fieldName) || "mapScript".equals(fieldName)) {
                    mapScript = parser.text();
                } else if ("combine_script".equals(fieldName) || "combineScript".equals(fieldName)) {
                    combineScript = parser.text();
                } else if ("reduce_script".equals(fieldName) || "reduceScript".equals(fieldName)) {
                    reduceScript = parser.text();
                } else if ("lang".equals(fieldName)) {
                    scriptLang = parser.text();
                }
            }
        }

        if (mapScript == null) {
            throw new FacetPhaseExecutionException(facetName, "map_script field is required");
        }

        return new ScriptFacetCollector(facetName, scriptLang, initScript, mapScript, combineScript, reduceScript, params, context, client);
    }

    @Override
    public Facet reduce(String s, List<Facet> facets) {
        List<Object> facetObjects = newArrayList();
        for (Facet facet : facets) {
            InternalScriptFacet mapReduceFacet = (InternalScriptFacet) facet;
            facetObjects.add(mapReduceFacet.facet());
        }
        InternalScriptFacet firstFacet = ((InternalScriptFacet) facets.get(0));
        Object facet;
        if (firstFacet.reduceScript() != null) {
            ExecutableScript script = scriptService.executable(firstFacet.scriptLang(), firstFacet.reduceScript(), new HashMap());
            script.setNextVar("facets", facetObjects);
            script.setNextVar("_client", client);
            facet = script.run();
        } else {
            facet = facetObjects;
        }
        return new InternalScriptFacet(firstFacet.name(), facet, firstFacet.scriptLang(), firstFacet.reduceScript());
    }
}
