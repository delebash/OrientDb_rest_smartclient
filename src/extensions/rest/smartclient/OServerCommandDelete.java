package extensions.rest.smartclient;


import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;

import java.io.IOException;
import java.io.StringWriter;

public class OServerCommandDelete extends OServerCommandAuthenticatedDbAbstract {
    private static final String[] NAMES = { "POST|scDelete/*" };

    public OServerCommandDelete(final OServerCommandConfiguration iConfiguration) {

    }
    @Override
    public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
        ODatabaseDocumentTx db = null;

        JsonParser parser = new JsonParser();
        JsonObject jsonObject = (JsonObject) parser.parse(iRequest.content);
        final JsonObject data = jsonObject.getAsJsonObject("data");
        final String rid = data.get("@rid").getAsString();
        final String version = data.get("@version").getAsString();

        String sException;
        //Holds data retrieved from query for sending back to client
        StringWriter buffer;

        try {

            iRequest.data.commandInfo = "Delete document";

            db = getProfiledDatabaseInstance(iRequest);

            // PARSE PARAMETERS

            final ORecordId recordId = new ORecordId(rid);

            if (!recordId.isValid())
                throw new IllegalArgumentException("Invalid Record ID in request: " + rid);

            final ODocument doc = new ODocument(recordId);

            // UNMARSHALL DOCUMENT WITH REQUEST CONTENT
            if (iRequest.content != null){
                    doc.getRecordVersion().getSerializer().fromString(version, doc.getRecordVersion());
            } else {
                    // IGNORE THE VERSION
                    doc.getRecordVersion().disable();
            }
            doc.delete();

            buffer = writeRecords(doc.getIdentity().toString(), 0);
            iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, buffer.toString(), null);

            // iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, null, null);

        } catch (OException e) {
            //sException = e.toString();
            // JOptionPane.showMessageDialog(null,"Generic exception caught");
        } finally {
            if (db != null)
                db.close();
        }
        return false;
    }
    private StringWriter writeRecords(String rid, Integer status) throws IOException {

        String iFormat = "type,indent:-1,rid,version,attribSameRow,class";
        String iFetchPlan = null;

        final StringWriter buffer = new StringWriter();
        final OJSONWriter json = new OJSONWriter(buffer, iFormat);
        json.beginObject();
        json.beginObject("response");
        json.writeAttribute("status", status);

        // WRITE RECORDS
        json.beginCollection(-1, true, "data");
        json.writeAttribute("@rid",rid);
        json.endCollection(-1, true);

        json.endObject();
        json.endObject();

        return buffer;
    }
    @Override
    public String[] getNames() {
        return NAMES;
    }
}

