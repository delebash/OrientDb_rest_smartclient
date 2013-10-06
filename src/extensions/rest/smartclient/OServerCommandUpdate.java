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


import java.io.IOException;
import java.io.StringWriter;

public class OServerCommandUpdate extends OServerCommandAuthenticatedDbAbstract {
    private static final String[] NAMES = {"POST|scUpdate/*"};

    public OServerCommandUpdate(final OServerCommandConfiguration iConfiguration) {

    }


    @Override
    public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {

        ODatabaseDocumentTx db = null;

        JsonParser parser = new JsonParser();
        JsonObject jsonObject = (JsonObject) parser.parse(iRequest.content);

        //Get smartclient rest params
        final String dataSourceName = jsonObject.get("dataSource").getAsString();
        final String operationType = jsonObject.get("operationType").getAsString();
        final String componentId = jsonObject.get("componentId").getAsString();
        final JsonObject data = jsonObject.getAsJsonObject("data");

        //SmartClient adds it to data field and not meta-data, need to remove after set
        String sException;
        //Holds data retrieved from query for sending back to client
        StringWriter buffer;


        iRequest.data.commandInfo = "Edit Document";


        ORecordId recordId = null;

        final ODocument doc;
        doc = new ODocument();
        try {
            db = getProfiledDatabaseInstance(iRequest);

            final String rid = data.get("@rid").getAsString();
            recordId = new ORecordId(rid);

            if (!recordId.isValid()) {
                throw new IllegalArgumentException("Invalid Record ID in request: " + recordId);
            } else
                recordId = new ORecordId();

           doc.fromJSON(data.toString());

            if (!recordId.isValid())
                recordId = (ORecordId) doc.getIdentity();
            else
                doc.setIdentity(recordId);

            if (!recordId.isValid())
                throw new IllegalArgumentException("Invalid Record ID in request: " + recordId);

            final ODocument currentDocument = db.load(recordId);

            if (currentDocument == null) {
                iResponse.send(OHttpUtils.STATUS_NOTFOUND_CODE, OHttpUtils.STATUS_NOTFOUND_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
                        "Record " + recordId + " was not found.", null);
            }

            currentDocument.merge(doc, true, false);
            currentDocument.getRecordVersion().copyFrom(doc.getRecordVersion());

            currentDocument.save();

            //data.remove("@version");
           // data.addProperty("@version", Integer.valueOf(currentDocument.getRecordVersion().toString()));
            buffer = writeRecords(currentDocument.toJSON(), 0);
            iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, buffer.toString(), null);

        } catch (OException e) {
            sException = e.toString();
            // JOptionPane.showMessageDialog(null,"Generic exception caught");
        } finally {
            if (db != null)
                db.close();
        }
        return false;
    }

    private StringWriter writeRecords(String jsonDoc, Integer status) throws IOException {

        String iFormat = "type,indent:-1,rid,version,attribSameRow,class";
        String iFetchPlan = null;

        final StringWriter buffer = new StringWriter();
        final OJSONWriter json = new OJSONWriter(buffer, iFormat);
        json.beginObject();
        json.beginObject("response");
        json.writeAttribute("status", status);

        // WRITE RECORDS
        json.beginCollection(-1, true, "data");
        json.write(jsonDoc);
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