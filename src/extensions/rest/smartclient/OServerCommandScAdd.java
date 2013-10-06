package extensions.rest.smartclient;

import com.google.gson.JsonObject;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;


public class OServerCommandScAdd {
    public OServerCommandScResponse execute(ODatabaseDocumentTx db, JsonObject record) throws Exception {
        final String dataSourceName = record.get("dataSource").getAsString();
        final JsonObject data = record.getAsJsonObject("data");
        data.addProperty("@class", dataSourceName);
        String sException;
        OServerCommandScResponse scResponse = new OServerCommandScResponse();
        ODocument doc = null;

        try {
            doc = new ODocument().fromJSON(data.toString());

            // ASSURE TO MAKE THE RECORD ID INVALID
            ((ORecordId) doc.getIdentity()).clusterPosition = ORID.CLUSTER_POS_INVALID;

            doc.save();
            scResponse.setoDoc(doc);
            scResponse.setStatus(0);
            scResponse.setOperationType("add");
        } catch (Exception e) {
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
            } else {
                scResponse.setServerError(true);
                scResponse.setServerErrorsString(sException);
                scResponse.setStatus(-1);
                scResponse.setQueueStatus(-1);
            }
        } finally

        {

        }

        return scResponse;
    }

}
