package org.elasticsearch.search.facet.script;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.common.collect.Lists.newArrayList;

/**
 *
 */
public class InternalScriptFacet implements ScriptFacet, InternalFacet {
    private static final String STREAM_TYPE = "script";

    private String name;
    private Object facet;
    private String scriptLang;
    private String reduceScript;
    private ScriptService scriptService;
    private Client client;


    public static void registerStreams(ScriptService scriptService, Client client) {
        Streams.registerStream(new ScriptFacetStream(scriptService, client), STREAM_TYPE);
    }

    private InternalScriptFacet(ScriptService scriptService, Client client) {
        this.scriptService = scriptService;
        this.client = client;
    }

    public InternalScriptFacet(String name, Object facet, String scriptLang, String reduceScript, ScriptService scriptService, Client client) {
        this(scriptService, client);
        this.name = name;
        this.facet = facet;
        this.reduceScript = reduceScript;
        this.scriptLang = scriptLang;
    }

    @Override
    public String streamType() {
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
            ExecutableScript script = scriptService.executable(firstFacet.scriptLang(), firstFacet.reduceScript(), new HashMap());
            script.setNextVar("facets", facetObjects);
            script.setNextVar("_client", client);
            facet = script.run();
        } else {
            facet = facetObjects;
        }
        return new InternalScriptFacet(firstFacet.name(), facet, firstFacet.scriptLang(), firstFacet.reduceScript(), scriptService, client);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String getName() {
        return name();
    }

    @Override
    public String type() {
        return ScriptFacet.TYPE;
    }

    @Override
    public String getType() {
        return type();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        scriptLang = in.readOptionalString();
        reduceScript = in.readOptionalString();
        facet = in.readGenericValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalString(scriptLang);
        out.writeOptionalString(reduceScript);
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

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString FACET = new XContentBuilderString("facet");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields._TYPE, STREAM_TYPE);
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
        public Facet readFacet(String type, StreamInput in) throws IOException {
            return InternalScriptFacet.readMapReduceFacet(in, scriptService, client);
        }
    }
}
