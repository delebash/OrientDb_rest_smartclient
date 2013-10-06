package extensions.rest.smartclient;


import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
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

public class OServerCommandAdd extends OServerCommandAuthenticatedDbAbstract {
     private static final String[] NAMES = { "POST|scAdd/*" };

    public OServerCommandAdd(final OServerCommandConfiguration iConfiguration) {

    }

    @Override
    public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {

        iRequest.data.commandInfo = "Create document";

        JsonParser parser = new JsonParser();
        JsonObject jsonObject = (JsonObject) parser.parse(iRequest.content);
        final JsonObject data = jsonObject.getAsJsonObject("data");

        ODatabaseDocumentTx db = null;

        ODocument doc = null;

        String sException;
        //Holds data retrieved from query for sending back to client
        StringWriter buffer;

        try {
            db = getProfiledDatabaseInstance(iRequest);

            doc = new ODocument().fromJSON(data.toString());

            // ASSURE TO MAKE THE RECORD ID INVALID
            ((ORecordId) doc.getIdentity()).clusterPosition = ORID.CLUSTER_POS_INVALID;

            doc.save();

            //iResponse.send(OHttpUtils.STATUS_CREATED_CODE, OHttpUtils.STATUS_CREATED_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
            //        doc.toJSON(), null, true);

            buffer = writeRecords(doc.toJSON(), 0);
            iResponse.send(OHttpUtils.STATUS_CREATED_CODE, OHttpUtils.STATUS_CREATED_DESCRIPTION, OHttpUtils.CONTENT_JSON, buffer.toString(), null,true);

        } catch (OException e) {
            sException = e.toString();
            // JOptionPane.showMessageDialog(null,"Generic exception caught");
        }  finally {
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