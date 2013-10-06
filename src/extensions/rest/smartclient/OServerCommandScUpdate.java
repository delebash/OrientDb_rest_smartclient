package extensions.rest.smartclient;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;


public class OServerCommandScUpdate {
    public  OServerCommandScResponse execute(ODatabaseDocumentTx db, JsonObject record) throws Exception {

        //Get smartclient rest params
        final JsonObject data = record.getAsJsonObject("data");
        String sException;
        OServerCommandScResponse scResponse = new OServerCommandScResponse();


        try {
            ORecordId recordId = null;
            final ODocument doc;
            ODocument currentDocument = null;

            doc = new ODocument();
            final String rid = data.get("@rid").getAsString();
            recordId = new ORecordId(rid);

            if (!recordId.isValid()) {
                throw new OException("Invalid Record ID in request: " + recordId);
            } else
                recordId = new ORecordId();

            doc.fromJSON(data.toString());

            if (!recordId.isValid())
                recordId = (ORecordId) doc.getIdentity();
            else
                doc.setIdentity(recordId);

            if (!recordId.isValid()) {
                throw new OException("Invalid Record ID in request: " + recordId);
            }

            currentDocument = db.load(recordId);

            if (currentDocument == null) {
                throw new OException("Record " + recordId + " was not found.");
            }

            currentDocument.merge(doc, true, false);
            currentDocument.getRecordVersion().copyFrom(doc.getRecordVersion());

            currentDocument.save();
            scResponse.setoDoc(currentDocument);
            scResponse.setStatus(0);
            scResponse.setOperationType("update");
//            if(e instanceof IllegalArgumentException || e instanceof SecurityException ||
//                    e instanceof IllegalAccessException || e instanceof NoSuchFieldException
        }  catch (Exception e) {
            sException = e.toString();
            if (e instanceof OException || e instanceof OSerializationException) {
                scResponse.setServerError(true);
                scResponse.setServerErrorsString(sException);
                scResponse.setStatus(-1);
                scResponse.setQueueStatus(-1);
            } else if (e instanceof OValidationException) {
                //Hanlde special case where additional field is sent but not in schema
                if (e.getMessage().contains("Found additional field")) {
                    scResponse.setServerError(true);
                    scResponse.setServerErrorsString(sException);
                    scResponse.setStatus(-1);
                    scResponse.setQueueStatus(-1);
                } else {
                    scResponse.setFieldErrors(true);
                    scResponse.setStatus(-4);
                    scResponse.parseFieldErrors(e.getMessage());
                }
            }else{
                scResponse.setServerError(true);
                scResponse.setServerErrorsString(sException);
                scResponse.setStatus(-1);
                scResponse.setQueueStatus(-1);
            }
        } finally {

        }

        return scResponse;

    }
}
