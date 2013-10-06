package extensions.rest.smartclient;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

import java.io.IOException;
import java.io.StringWriter;

public class OServerCommandScDelete {
    public OServerCommandScResponse execute(ODatabaseDocumentTx db, JsonObject record) throws Exception {

        final JsonObject data = record.getAsJsonObject("data");
        final String rid = data.get("@rid").getAsString();
        final String version = data.get("@version").getAsString();
        String sException;
        OServerCommandScResponse scResponse = new OServerCommandScResponse();

        //   iRequest.data.commandInfo = "Delete document";
        ODocument doc = null;
        try {

            // PARSE PARAMETERS
            final ORecordId recordId = new ORecordId(rid);

            if (!recordId.isValid())
                throw new OException("Invalid Record ID in request: " + rid);

            doc = new ODocument(recordId);

            // UNMARSHALL DOCUMENT WITH REQUEST CONTENT

            doc.getRecordVersion().getSerializer().fromString(version, doc.getRecordVersion());
            doc.delete();
            scResponse.setoDoc(doc);
            scResponse.setStatus(0);
            scResponse.setOperationType("remove");

        }  catch (Exception e) {
            sException = e.toString();
            if (e instanceof OException) {
                scResponse.setServerError(true);
                scResponse.setServerErrorsString(sException);
                scResponse.setStatus(-1);
                scResponse.setQueueStatus(-1);
            } else if (e instanceof OValidationException) {
                scResponse.setFieldErrors(true);
                scResponse.setStatus(-4);
                scResponse.parseFieldErrors(e.getMessage());
            }
        } finally {

        }
        return scResponse;


    }

}

