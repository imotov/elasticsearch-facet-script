package org.elasticsearch.search.facet.script;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class ScriptFacetProcessor extends AbstractComponent implements FacetProcessor {

    private final Client client;

    @Inject
    public ScriptFacetProcessor(Settings settings, ScriptService scriptService, Client client) {
        super(settings);
        InternalScriptFacet.registerStreams(scriptService, client);
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

}
