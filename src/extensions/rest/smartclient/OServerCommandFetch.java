package extensions.rest.smartclient;


import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

public class OServerCommandFetch extends OServerCommandAuthenticatedDbAbstract {
    private static final String[] NAMES = { "POST|scFetch/*" };

    public OServerCommandFetch(final OServerCommandConfiguration iConfiguration) {

    }

    @Override
    public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {

        ODatabaseDocumentTx db = null;

        ODocument request = null;
        request = new ODocument().fromJSON(iRequest.content);

        //Get smartclient rest params
        final String dataSourceName = request.field("dataSource");
        final String operationType = request.field("operationType");
        final Integer startRow = request.field("startRow");
        final Integer endRow = request.field("endRow");
        final String textMatchStyle = request.field("textMatchStyle");
        final String componentId = request.field("componentId");
        final HashMap<String, Object> data = request.field("data");
        final ArrayList<String> sortBy = request.field("sortBy");
        final ODocument oldValues = request.field("oldValues");

        //Added request param className to indicate Orientdb class/table to use
        final String className = "Contact";//data.get("@class").toString();
        //SmartClient adds it to data field and not meta-data, need to remove after set
        // data.remove("@class");

        //Server response status
        Integer status = 0;
        //Needed for paging
        long totalRecords;
        //Number of records to return default to all
        Integer limit = -1;
        //Depth of related data to expand
        String fetchPlan = null;

        if (!"fetch".equals(operationType)) {
            iResponse.send(OHttpUtils.STATUS_INVALIDMETHOD_CODE, "Only fetch request is allowed for this url", OHttpUtils.CONTENT_TEXT_PLAIN, "Operation type" + operationType + "not allowed", null);
            return false;
        }
        //For paging
        limit = endRow - startRow;

        //Initial query string
       String query_string = "Select from " + className;
       String query_count = "Select count(*) as count from " + className;
        //Build where parameters and sortBy params
        query_string += buildSqlString(data, sortBy, startRow,false);
        query_count += buildSqlString(data, sortBy, startRow,true);

        //Holds data retrieved from query for sending back to client
        StringWriter buffer;

        iRequest.data.commandInfo = "SmartClient";
        iRequest.data.commandDetail = operationType + " " + className;

        final List<OIdentifiable> response;

        try {
            db = getProfiledDatabaseInstance(iRequest);

            List<ODocument> recordCount = db.command(
                    new OCommandSQL(query_count))
                    .execute();

            totalRecords = recordCount.get(0).field("count");

            response = db.command(
                    new OCommandSQL(query_string))
                    .setLimit(limit)
                    .setFetchPlan(fetchPlan)
                    .execute();

            final Iterator<OIdentifiable> iRecords = response.iterator();
            if (iRecords == null)
                return false;

            buffer = writeRecords(iRecords, fetchPlan, null, startRow, endRow, totalRecords, status);
            iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, buffer.toString(), null);

        } finally {
            if (db != null)
                db.close();
        }
        return false;
    }

    private String buildSqlString(HashMap<String, Object> data, ArrayList<String> sortBy, Integer startRow, boolean isRecordCount) {
        //Build sql from criteria
        //Add where from filter params
        String temp_query_string="";

        if (data.size() > 0) {
            temp_query_string += " where ";
            //query_count += " where ";

            Iterator<Map.Entry<String, Object>> entries = data.entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry<String, Object> entry = entries.next();
                String key = entry.getKey();
                String value = entry.getValue().toString();

                // condition will be something like key = :key (:key being parameter placeholder)
                temp_query_string += key + " like  '%" + value + "%' and ";
                //query_count += key + " like  '%" + value + "%' and ";

                // remove 'and' of the query
                temp_query_string = temp_query_string.substring(0, temp_query_string.length() - 4);
                //query_count = query_count.substring(0, query_count.length() - 4);
            }
        }
        // sort by
        if(!isRecordCount){
        if (sortBy != null) {
            // set the orderBy
            temp_query_string += " order by ";

            // we start to build a coma separated list of items. First item won't have coma
            // but every possible next will do
            String separator = "";
            Iterator<String> iterator = sortBy.iterator();
            while (iterator.hasNext()) {
                String sort = iterator.next();
                // if column name is with -, then ordering is descending, otherwise ascending
                if (sort.contains("-")) {
                    temp_query_string += separator + sort.substring(1) + " DESC";
                } else {
                    temp_query_string += separator + sort + " ASC";
                }
                separator = ",";
            }
        }
        }

        //Add Skip for paging
        temp_query_string += " SKIP " + startRow;

        return temp_query_string;
    }

    private StringWriter writeRecords(final Iterator<OIdentifiable> iRecords, final String iFetchPlan,
                                      String iFormat, final int startRow, final int endRow,
                                      final long totalRecords, final int status) throws IOException {

        final String JSON_FORMAT = "type,indent:-1,rid,version,attribSameRow,class";

        // final Iterator<OIdentifiable> iRecords = response.iterator();
        // if (iRecords == null)
        //      return null;
        if (iFormat == null)
            iFormat = JSON_FORMAT;

        final StringWriter buffer = new StringWriter();
        final OJSONWriter json = new OJSONWriter(buffer, iFormat);
        json.beginObject();
        json.beginObject("response");
        json.writeAttribute("status", status);
        json.writeAttribute("startRow", startRow);
        json.writeAttribute("endRow", endRow);
        json.writeAttribute("totalRows", totalRecords);


        // /  buffer.append("" + );
        final String format = iFetchPlan != null ? iFormat + ",fetchPlan:" + iFetchPlan : iFormat;

        // WRITE RECORDS
        json.beginCollection(-1, true, "data");
        formatMultiValue(iRecords, buffer, format);
        json.endCollection(-1, true);

        json.endObject();
        json.endObject();

        return buffer;

    }

    public void formatMultiValue(final Iterator<?> iIterator, final StringWriter buffer, final String format) throws IOException {
        if (iIterator != null) {
            int counter = 0;
            String objectJson;

            while (iIterator.hasNext()) {
                final Object entry = iIterator.next();
                if (entry != null) {
                    if (counter++ > 0)
                        buffer.append(", ");

                    if (entry instanceof OIdentifiable) {
                        ORecord<?> rec = ((OIdentifiable) entry).getRecord();
                        try {
                            objectJson = rec.getRecord().toJSON(format);

                            buffer.append(objectJson);
                        } catch (Exception e) {
                            OLogManager.instance().error(this, "Error transforming record " + rec.getIdentity() + " to JSON", e);
                        }
                    } else if (OMultiValue.isMultiValue(entry))
                        formatMultiValue(OMultiValue.getMultiValueIterator(entry), buffer, format);
                    else
                        buffer.append(OJSONWriter.writeValue(entry, format));
                }
            }
        }
    }

    @Override
    public String[] getNames() {
        return NAMES;
    }
}