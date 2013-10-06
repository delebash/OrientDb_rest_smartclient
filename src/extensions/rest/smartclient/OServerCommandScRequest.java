package extensions.rest.smartclient;


import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;


public class OServerCommandScRequest extends OServerCommandAuthenticatedDbAbstract {
    private static final String[] NAMES = {"POST|scRequest/*"};

    public OServerCommandScRequest(final OServerCommandConfiguration iConfiguration) {

    }

    @Override
    public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {

        JsonParser parser = new JsonParser();
        JsonObject jsonObject = (JsonObject) parser.parse(iRequest.content);
        StringWriter transactionBuffer;
        boolean isTransaction;
        String sException;

        //Single operations
        if (jsonObject.get("transaction") == null) {
            isTransaction = false;
            transactionBuffer = MakeIntoTransaction(jsonObject);
            jsonObject = (JsonObject) parser.parse(transactionBuffer.toString());
        } else { //True transaction multiple operations
            isTransaction = true;
        }

        String operationType = "";
        JsonObject transactionObject = jsonObject.get("transaction").getAsJsonObject();
        Integer transactionNum = transactionObject.get("transactionNum").getAsInt();
        JsonArray operations = transactionObject.get("operations").getAsJsonArray();
        ODatabaseDocumentTx db = null;
        StringWriter fetchBuffer = new StringWriter();
        List<OServerCommandScResponse> scResponseList = new ArrayList<OServerCommandScResponse>();


//        iRequest.data.commandInfo = "SmartClient";
//        iRequest.data.commandDetail = operationType + " " + dataSourceName;

        try {
            db = getProfiledDatabaseInstance(iRequest);
            if (isTransaction) {
                db.begin();
            }
            //Even if just one CUD we still handle same way but just don't start a database transaction
            for (int i = 0; i < operations.size(); ++i) {
                JsonObject record = operations.get(i).getAsJsonObject();
                operationType = record.get("operationType").getAsString();
                //Fetch operation should be only 1 and not really in transaction
                if ("fetch".equals(operationType) && operations.size() == 1) {
                    OServerCommandScFetch objFetch = new OServerCommandScFetch();
                    fetchBuffer = objFetch.execute(db, record);
                }
                if ("add".equals(operationType)) {
                    OServerCommandScAdd objAdd = new OServerCommandScAdd();
                    scResponseList.add(objAdd.execute(db, record));
                } else if ("update".equals(operationType)) {
                    OServerCommandScUpdate objUpdate = new OServerCommandScUpdate();
                    scResponseList.add(objUpdate.execute(db, record));
                } else if ("remove".equals(operationType)) {
                    OServerCommandScDelete objRemove = new OServerCommandScDelete();
                    scResponseList.add(objRemove.execute(db, record));
                } else {
                    //Should not be here
                    System.err.println("error");
                }
            }
            if (isTransaction) {
                db.commit();
            }

            if (fetchBuffer.toString().length() > 0) {
                iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, fetchBuffer.toString(), null);
            } else { //Must be CUD
                String responseString = "";
                responseString = getResponseString(scResponseList, isTransaction);
                if (isTransaction) {
                    responseString = "[" + responseString + "]";
                }
                iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, responseString, null);
            }
        } catch (Exception e) {
            iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, getResponseString(e.toString()), null);
        } finally {
            if (db != null)
                db.close();
        }
        return false;
    }

    public StringWriter MakeIntoTransaction(final JsonObject jsonObject) throws Exception {

        final String JSON_FORMAT = "type,indent:-1,rid,version,attribSameRow,class";
        final StringWriter buffer = new StringWriter();
        final OJSONWriter json = new OJSONWriter(buffer, JSON_FORMAT);

        json.beginObject();
        json.beginObject("transaction");
        json.writeAttribute("transactionNum", "-1");
        json.beginCollection(-1, true, "operations");
        json.write(jsonObject.toString());
        json.endCollection(-1, true);
        json.endObject();
        json.endObject();

        return buffer;
    }

    public String getResponseString(List<OServerCommandScResponse> scResponseList, boolean isTransaction) throws Exception {
        StringWriter responseBuffer;
        String responseString = "";
        String separator = ",";
        for (Iterator iterator = scResponseList.iterator(); iterator.hasNext(); ) {
            OServerCommandScResponse scResponse = (OServerCommandScResponse) iterator.next();
            responseBuffer = writeRecords(scResponse, isTransaction);
            responseString += responseBuffer.toString() + separator;
        }
        if (responseString.length() > 0) {
            responseString = responseString.substring(0, responseString.length() - 1);
        }
        return responseString;
    }

    public String getResponseString(String errorMessage) throws Exception {
        OServerCommandScResponse scResponse = new OServerCommandScResponse();
        scResponse.setStatus(-1);
        scResponse.setOperationType("");
        scResponse.setServerErrorsString(errorMessage);
        List<OServerCommandScResponse> scResponseList = new ArrayList<OServerCommandScResponse>();
        scResponseList.add(scResponse);
        String response = getResponseString(scResponseList, false);
        return response;
    }

    public StringWriter writeRecords(OServerCommandScResponse scResponse, boolean isTransaction) throws IOException {

        String iFormat = "type,indent:-1,rid,version,attribSameRow,class";
        String operationType = scResponse.getOperationType();
        final StringWriter buffer = new StringWriter();
        final OJSONWriter json = new OJSONWriter(buffer, iFormat);

        if (scResponse.getStatus() != -4) {
            json.beginObject();
            json.beginObject("response");
            if (isTransaction) {
                json.writeAttribute("queueStatus", scResponse.getQueueStatus());
            }
            json.writeAttribute("status", scResponse.getStatus());
            // WRITE RECORDS
            // If server error just send data with error message no collection
            if (scResponse.getStatus() == -1) {
                json.writeAttribute("data", scResponse.getServerErrorsString());
            } else if (scResponse.getStatus() == 0) {
                json.beginCollection(-1, true, "data");
                if (operationType == "remove") {
                    json.beginObject();
                    json.writeAttribute("@rid", scResponse.getoDoc().getIdentity().toString());
                    json.endObject();
                } else {
                    json.write(scResponse.getoDoc().toJSON());
                }
                json.endCollection(-1, true);
            }
            json.endObject();
            json.endObject();
        } else if (scResponse.getStatus() == -4) {  //Field errors
            json.beginObject();
            json.beginObject("response");
            json.writeAttribute("status", scResponse.getStatus());
            json.beginObject("errors");
            json.beginObject(scResponse.getScfieldErrors().getFieldName());
            json.writeAttribute("errorMessage", scResponse.getScfieldErrors().getScErrorMsgString());
            json.endObject();
            json.endObject();
            json.endObject();
            json.endObject();
        }

        return buffer;
    }

    @Override
    public String[] getNames() {
        return NAMES;
    }
}
