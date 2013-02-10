package org.elasticsearch.search.facet.script;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.collect.Maps.newHashMap;

/**
 *
 */
public class ScriptFacetCollector extends AbstractFacetCollector {

    private final String scriptLang;

    private final SearchScript mapScript;

    private final ExecutableScript combineScript;

    private final String reduceScript;

    private final Map<String, Object> params;

    private ScriptService scriptService;

    private Client client;

    public ScriptFacetCollector(String facetName, String scriptLang, String initScript, String mapScript, String combineScript,
                                String reduceScript, Map<String, Object> params, SearchContext context, Client client) {
        super(facetName);
        this.scriptService = context.scriptService();
        this.client = client;
        this.scriptLang = scriptLang;
        if (params == null) {
            this.params = newHashMap();
        } else {
            this.params = params;
        }
        this.params.put("_ctx", context);
        this.params.put("_client", client);
        if (initScript != null) {
            scriptService.executable(scriptLang, initScript, this.params).run();
        }
        this.mapScript = scriptService.search(context.lookup(), scriptLang, mapScript, this.params);
        if (combineScript != null) {
            this.combineScript = scriptService.executable(scriptLang, combineScript, this.params);
        } else {
            this.combineScript = null;
        }
        this.reduceScript = reduceScript;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        mapScript.setScorer(scorer);
    }

    @Override
    protected void doSetNextReader(AtomicReaderContext context) throws IOException {
        mapScript.setNextReader(context);
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        mapScript.setNextDocId(doc);
        mapScript.run();
    }

    @Override
    public Facet facet() {
        Object facet;
        if (combineScript != null) {
            facet = combineScript.run();
        } else {
            facet = params.get("facet");
        }
        return new InternalScriptFacet(facetName, facet, scriptLang, reduceScript, scriptService, client);
    }
}
