package org.elasticsearch.search.facet.script;

import static org.elasticsearch.common.collect.Lists.newArrayList;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class InternalScriptFacet extends InternalFacet implements ScriptFacet {
    private static final BytesReference STREAM_TYPE = new HashedBytesArray("script");

    private Object facet;
    private String scriptLang;
    private String reduceScript;
    private Map<String, Object> reduceParams;
    private ScriptService scriptService;
    private Client client;


    public static void registerStreams(ScriptService scriptService, Client client) {
        Streams.registerStream(new ScriptFacetStream(scriptService, client), STREAM_TYPE);
    }

    private InternalScriptFacet(ScriptService scriptService, Client client) {
        this.scriptService = scriptService;
        this.client = client;
    }

    private InternalScriptFacet(String name, ScriptService scriptService, Client client) {
        super(name);
        this.scriptService = scriptService;
        this.client = client;
    }

    public InternalScriptFacet(String name, Object facet, String scriptLang, String reduceScript, Map<String, Object> reduceParams, ScriptService scriptService, Client client) {
        this(name, scriptService, client);
        this.facet = facet;
        this.reduceScript = reduceScript;
        this.reduceParams = reduceParams;
        this.scriptLang = scriptLang;
    }

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    @Override
    public Facet reduce(List<Facet> facets) {
        List<Object> facetObjects = newArrayList();
        for (Facet facet : facets) {
            InternalScriptFacet mapReduceFacet = (InternalScriptFacet) facet;
            facetObjects.add(mapReduceFacet.facet());
        }
        InternalScriptFacet firstFacet = ((InternalScriptFacet) facets.get(0));
        Object facet;
        if (firstFacet.reduceScript() != null) {
            ExecutableScript script = scriptService.executable(firstFacet.scriptLang(), firstFacet.reduceScript(), firstFacet.reduceParams());
            script.setNextVar("facets", facetObjects);
            script.setNextVar("_client", client);
            facet = script.run();
        } else {
            facet = facetObjects;
        }
        return new InternalScriptFacet(firstFacet.getName(), facet, firstFacet.scriptLang(), firstFacet.reduceScript(), firstFacet.reduceParams(), scriptService, client);
    }

    @Override
    public String getType() {
        return ScriptFacet.TYPE;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        scriptLang = in.readOptionalString();
        reduceScript = in.readOptionalString();
        reduceParams = in.readMap();
        facet = in.readGenericValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(scriptLang);
        out.writeOptionalString(reduceScript);
        out.writeMap(reduceParams);
        out.writeGenericValue(facet);
    }

    @Override
    public Object facet() {
        return facet;
    }

    @Override
    public Object getFacet() {
        return facet();
    }

    public String scriptLang() {
        return scriptLang;
    }

    public String reduceScript() {
        return reduceScript;
    }

    public Map<String, Object> reduceParams() {
      return reduceParams;
    }
    
    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString FACET = new XContentBuilderString("facet");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field(Fields._TYPE, ScriptFacet.TYPE);
        builder.field(Fields.FACET, facet);
        builder.endObject();
        return builder;
    }

    public static InternalScriptFacet readMapReduceFacet(StreamInput in, ScriptService scriptService, Client client) throws IOException {
        InternalScriptFacet facet = new InternalScriptFacet(scriptService, client);
        facet.readFrom(in);
        return facet;
    }

    private static class ScriptFacetStream implements InternalFacet.Stream {

        private ScriptService scriptService;
        private Client client;


        public ScriptFacetStream(ScriptService scriptService, Client client) {
            this.scriptService = scriptService;
            this.client = client;
        }

        @Override
        public Facet readFacet(StreamInput in) throws IOException {
            return InternalScriptFacet.readMapReduceFacet(in, scriptService, client);
        }

    }
}
