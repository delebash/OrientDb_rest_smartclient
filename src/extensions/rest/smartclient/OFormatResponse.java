package extensions.rest.smartclient;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;

import java.io.IOException;
import java.io.StringWriter;


public class OFormatResponse {
    public static StringWriter writeRecords(ODocument oDoc, Integer status, boolean isTransaction,String transactionType) throws IOException {

        String iFormat = "type,indent:-1,rid,version,attribSameRow,class";
        String iFetchPlan = null;

        final StringWriter buffer = new StringWriter();
        final OJSONWriter json = new OJSONWriter(buffer, iFormat);
        json.beginObject();
        json.beginObject("response");
        if (isTransaction) {
            json.writeAttribute("queueStatus", 0);
        }
        json.writeAttribute("status", status);
        // WRITE RECORDS
        json.beginCollection(-1, true, "data");
        if (transactionType == "delete"){
             json.write(oDoc.getIdentity().toString());
        } else{
            json.write(oDoc.toJSON());
        }

        json.endCollection(-1, true);

        json.endObject();
        json.endObject();

        return buffer;
    }

}
