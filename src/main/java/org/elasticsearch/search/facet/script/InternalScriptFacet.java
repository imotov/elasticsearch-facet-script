package org.elasticsearch.search.facet.script;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import java.io.IOException;

/**
 *
 */
public class InternalScriptFacet implements ScriptFacet, InternalFacet {
    private static final String STREAM_TYPE = "script";

    private String name;
    private Object facet;
    private String scriptLang;
    private String reduceScript;


    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(String type, StreamInput in) throws IOException {
            return readMapReduceFacet(in);
        }
    };

    private InternalScriptFacet() {

    }

    public InternalScriptFacet(String name, Object facet, String scriptLang, String reduceScript) {
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
        name = in.readUTF();
        scriptLang = in.readOptionalUTF();
        reduceScript = in.readOptionalUTF();
        facet = in.readGenericValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeOptionalUTF(scriptLang);
        out.writeOptionalUTF(reduceScript);
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

    public static InternalScriptFacet readMapReduceFacet(StreamInput in) throws IOException {
        InternalScriptFacet facet = new InternalScriptFacet();
        facet.readFrom(in);
        return facet;
    }


}
