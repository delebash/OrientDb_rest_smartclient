package extensions.rest.smartclient;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;


import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


public class OServerCommandCombinedOld extends OServerCommandAuthenticatedDbAbstract {
    private static final String[] NAMES = { "POST|SmartClient/*" };


    public OServerCommandCombinedOld(final OServerCommandConfiguration iConfiguration) {
//        super(iConfiguration.pattern);
//
//        // PARSE PARAMETERS ON STARTUP
//        for (OServerEntryConfiguration par : iConfiguration.parameters) {
//            if (par.name.equals("italic"))
//                italic = Boolean.parseBoolean( par.value );
//        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
        //Try to fix error Database instance is not set in current thread
        ODatabaseDocumentTx db = null;


        ODocument request = null;
        request = new ODocument().fromJSON(iRequest.content);

        //Get smartclient rest params
        String dataSource = request.field("dataSource");
        String operationType = request.field("operationType");
        Integer startRow = request.field("startRow");
        Integer endRow = request.field("endRow");
        String textMatchStyle = request.field("textMatchStyle");
        String componentId = request.field("componentId");
        HashMap<String, Object> data = request.field("data");
        ArrayList<String> sortBy = request.field("sortBy");
        ODocument oldValues = request.field("oldValues");


        if ("fetch".equals(operationType)) {
            fetch(db, iRequest, iResponse, dataSource, startRow, endRow, textMatchStyle, data, sortBy);
        } else if ("add".equals(operationType)) {
            add(db, iRequest, iResponse, dataSource, data);
        } else if ("update".equals(operationType)) {
            update(db, iRequest, iResponse, dataSource, data);
        } else if ("delete".equals(operationType)) {
            delete(db, iRequest, iResponse, dataSource, data);
        }


        return false;
    }

    private void add(ODatabaseDocumentTx db, OHttpRequest iRequest, OHttpResponse iResponse, String dataSource, HashMap<String, Object> data) {

    }

    private void update(ODatabaseDocumentTx db, OHttpRequest iRequest, OHttpResponse iResponse, String dataSource, HashMap<String, Object> data) throws Exception {

        ORecordId recordId = null;

        final ODocument doc;
        doc = new ODocument();
        try {
            db = getProfiledDatabaseInstance(iRequest);

            final String rid = data.get("@rid").toString();
            recordId = new ORecordId(rid);

            if (!recordId.isValid()) {
                throw new IllegalArgumentException("Invalid Record ID in request: " + recordId);
            } else
                recordId = new ORecordId();

            String json = OJSONWriter.mapToJSON(data);
            doc.fromJSON(json);

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

            currentDocument.merge(doc, false, false);
            currentDocument.getRecordVersion().copyFrom(doc.getRecordVersion());

            currentDocument.save();

            iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Record " + recordId + " updated successfully.", null);

        } finally {
            if (db != null)
                db.close();
        }
    }

    private void fetch(ODatabaseDocumentTx db, OHttpRequest iRequest, OHttpResponse iResponse, String dataSource, int startRow, int endRow, String textMatchStyle,
                       HashMap<String, Object> data, ArrayList<String> sortBy) throws Exception {


        //For paging default to return all records
        long totalRecords = 0;
        Integer limit = endRow - startRow;
        String query_string = "Select from " + dataSource;
        String query_count = "Select count(*) as count from " + dataSource;

        //Build sql from criteria
        //Add where from filter params
        if (data.size() > 0) {
            query_string += " where ";
            query_count += " where ";

            Iterator<Map.Entry<String, Object>> entries = data.entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry<String, Object> entry = entries.next();
                String key = entry.getKey();
                String value = entry.getValue().toString();

                // condition will be something like key = :key (:key being parameter placeholder)
                query_string += key + " like  '%" + value + "%' and ";
                query_count += key + " like  '%" + value + "%' and ";

                // remove 'and' of the query
                query_string = query_string.substring(0, query_string.length() - 4);
                query_count = query_count.substring(0, query_count.length() - 4);
            }
        }
        // sort by
        if (sortBy != null) {
            // set the orderBy
            query_string += " order by ";

            // we start to build a coma separated list of items. First item won't have coma
            // but every possible next will do
            String separator = "";
            Iterator<String> iterator = sortBy.iterator();
            while (iterator.hasNext()) {
                String sort = iterator.next();
                // if column name is with -, then ordering is descending, otherwise ascending
                if (sort.contains("-")) {
                    query_string += separator + sort.substring(1) + " DESC";
                } else {
                    query_string += separator + sort + " ASC";
                }
                separator = ",";
            }
        }

        //Add Skip for paging
        query_string += " SKIP " + startRow;

        //Server response status
        int status = 0;


        //Depth of related data to expand
        String fetchPlan = null;

        //Holds data retrieved from query for sending back to client
        StringWriter buffer;

        iRequest.data.commandInfo = "SmartClient";
        iRequest.data.commandDetail = "fetch " + dataSource;


        final List<OIdentifiable> response;


        try {
            db = getProfiledDatabaseInstance(iRequest);

            List<ODocument> result = db.query(
                    new OSQLSynchQuery<ODocument>(query_count));

            totalRecords = result.get(0).field("count");

            //  response = (List<OIdentifiable>) db.query(new OSQLSynchQuery<ORecordSchemaAware<?>>(query_string, limit).setFetchPlan(fetchPlan));
            response = db.command(
                    new OCommandSQL(query_string)).setLimit(limit).setFetchPlan(fetchPlan).execute();

            buffer = writeRecords(response, fetchPlan, null, startRow, endRow, totalRecords, status);
            iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, buffer.toString(), null);

        } finally {
            if (db != null)
                db.close();
        }
    }

    private void delete(ODatabaseDocumentTx db, OHttpRequest iRequest, OHttpResponse iResponse, String dataSource, HashMap<String, Object> data) {

    }

    private StringWriter writeRecords(final Iterable<OIdentifiable> response, final String iFetchPlan,
                                      String iFormat, final int startRow, final int endRow,
                                      final long totalRecords, final int status) throws IOException {

        final String JSON_FORMAT = "type,indent:-1,rid,version,attribSameRow,class";

        final Iterator<OIdentifiable> iRecords = response.iterator();
        if (iRecords == null)
            return null;
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

    public ODocument GetDataSource(String dataSourceId) throws IOException {
        String data_source = readFile("ds/" + dataSourceId + ".js", Charset.defaultCharset());

        data_source = data_source.replace("isc.RestDataSource.create(", "");
        data_source = data_source.replace(");", "");
        ODocument ds = null;
        ds = new ODocument().fromJSON(data_source);

        return ds;
    }

    static String readFile(String path, Charset encoding)
            throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return encoding.decode(ByteBuffer.wrap(encoded)).toString();
    }

    @Override
    public String[] getNames() {
        return NAMES;
    }
}

