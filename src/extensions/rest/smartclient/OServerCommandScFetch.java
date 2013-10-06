package extensions.rest.smartclient;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.ORecordSchemaAwareAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetDatabase;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class OServerCommandScFetch {
    JsonArray schemaProperties;

    public StringWriter execute(ODatabaseDocumentTx db, JsonObject record) throws Exception {

        //Get smartclient rest params
        final String dataSourceName = record.get("dataSource").getAsString();
        final JsonObject filterCriteria = record.getAsJsonObject("data");
        final Integer startRow = record.get("startRow").getAsInt();
        final Integer endRow = record.get("endRow").getAsInt();
        final JsonArray sortBy = record.getAsJsonArray("sortBy");

        String sException;
        //Holds data retrieved from query for sending back to client
        StringWriter buffer = null;

        //Server response status
        Integer status = 0;
        //Needed for paging
        long totalRecords;
        //Number of records to return default to all
        Integer limit;
        //Depth of related data to expand
        String fetchPlan = null;

        //For paging
        limit = endRow - startRow;
        String pagingParam = " SKIP " + startRow;
        String simpleCriteria = "";
        String advancedCritera = "";
        //Initial query string
        String query_string = "Select from " + dataSourceName;
        String query_count = "Select count(*) as count from " + dataSourceName;


        //Used to get field types integer, string for criteria
        String schema = getSchema(dataSourceName, db);
        JsonParser parser = new JsonParser();
        JsonObject jsonSchema = (JsonObject) parser.parse(schema);
        schemaProperties = jsonSchema.get("properties").getAsJsonArray();

        if (filterCriteria.get("_constructor") == null && filterCriteria.get("operator") == null) {
            //Build Simple Criteria where parameters and sortBy params
            simpleCriteria = buildSimpleSqlCriteria(filterCriteria);
            query_string += simpleCriteria;
            query_string += buildSqlSortBy(sortBy);
            query_string += pagingParam;
            //Count for paging
            query_count += simpleCriteria;
        } else {
            advancedCritera = buildAdvancedSqlCriteria(filterCriteria);
            query_string += advancedCritera;
            query_string += buildSqlSortBy(sortBy);
            query_string += pagingParam;
            //Count for paging
            query_count += advancedCritera;
        }

        final List<OIdentifiable> response;

        try {

            List<ODocument> recordCount = db.command(
                    new OCommandSQL(query_count))
                    .execute();

            totalRecords = recordCount.get(0).field("count");

            response = db.command(
                    new OCommandSQL(query_string))
                    .setLimit(limit)
                    .setFetchPlan(fetchPlan)
                    .execute();
            if (response.size() == 0) {
                return writeException("No records found");
            } else {
                final Iterator<OIdentifiable> iRecords = response.iterator();
                buffer = writeRecords(iRecords, fetchPlan, null, startRow, endRow, totalRecords, status,dataSourceName);
                //  iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, buffer.toString(), null);
            }

        } catch (OException e) {
            buffer = writeException(e.toString());
        } finally {

        }
        return buffer;
    }

    private String buildSimpleSqlCriteria(JsonObject filterCriteria) {
        //Build sql from criteria
        //Add where from filter params
        String temp_query_string = "";
        if (filterCriteria.entrySet().size() > 0) {
            temp_query_string += " where ";
            //query_count += " where ";
            Iterator<Map.Entry<String, JsonElement>> entries = filterCriteria.entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry<String, JsonElement> entry = entries.next();
                String key = entry.getKey();
                String value = entry.getValue().getAsString();
                // condition will be something like key = :key (:key being parameter placeholder)
                String type = getField(key);
                if ("STRING".equals(type) || "LINK".equals(type)) {
                    temp_query_string += key + " like  '%" + value + "%' and ";
                } else {
                    temp_query_string += key + " = " + value + " and ";
                }
            }
            // remove 'and' of the query
            temp_query_string = temp_query_string.substring(0, temp_query_string.length() - 4);
        }

        return temp_query_string;
    }

    private String buildSqlSortBy(JsonArray sortBy) {
        //  sort by
        String temp_query_string = "";
        if (sortBy != null) {
            // set the orderBy
            temp_query_string += " order by ";
            // we start to build a coma separated list of items. First item won't have coma
            // but every possible next will do
            String separator = "";
            // Iterator<?> keys = jObject.keys();
            // Iterator<Map.Entry<String, JsonElement>> entries = data.entrySet().iterator();
            Iterator<JsonElement> iterator = sortBy.iterator();
            while (iterator.hasNext()) {
                String sort = iterator.next().getAsString();
                // if column name is with -, then ordering is descending, otherwise ascending
                if (sort.contains("-")) {
                    temp_query_string += separator + sort.substring(1) + " DESC";
                } else {
                    temp_query_string += separator + sort + " ASC";
                }
                separator = ",";
            }
        }
        return temp_query_string;
    }

    private String buildAdvancedSqlCriteria(JsonObject filterCriteria) {
        JsonArray criterias = filterCriteria.get("criteria").getAsJsonArray();
        String operator = filterCriteria.get("operator").getAsString();
        String _constructor = filterCriteria.get("_constructor").getAsString();
        String result = "";
        String temp_query = " WHERE ";
        String query = "";
        String result_query = "";
        String type = "";

        //Possible bug when using showFilterCteria
        //If field is empty at this point then there is a duplicate wrapper of
        // operator _constructor
        JsonObject test = criterias.get(0).getAsJsonObject();
        if (test.get("fieldName") == null) {
            result_query = buildAdvancedSqlCriteria(test);
        } else {

            for (int i = 0; i < criterias.size(); i++) {
                JsonObject jo = criterias.get(i).getAsJsonObject();
                String op = (jo.get("operator") == null) ? "" : jo.get("operator").getAsString();
                String fieldName = (jo.get("fieldName") == null) ? "" : jo.get("fieldName").getAsString();
                String value = (jo.get("value") == null) ? "" : jo.get("value").getAsString();
                String start = (jo.get("start") == null) ? "" : jo.get("start").getAsString();
                String end = (jo.get("end") == null) ? "" : jo.get("end").getAsString();


                //Means nested criteria
                //String constructor = (jo.get("_constructor") == null) ? "" : jo.get("_constructor").getAsString();
                JsonArray criteria = (jo.get("criteria") == null) ? null : jo.get("criteria").getAsJsonArray();

                if (criteria == null) {
                    type = getField(fieldName);
                    value = formatValue(type, value, op);
                    //32 different operator values
                    if ("equals".equals(op)) {
                        query = fieldName + " = " + value;
                    } else if ("notEqual".equals(op)) {
                        query = fieldName + "<> " + value;
                    } else if ("iEquals".equals(op)) {
                        query = fieldName + ".toUpperCase() = " + value.toUpperCase();
                    } else if ("iNotEqual".equals(op)) {
                        query = fieldName + ".toUpperCase() <> " + value.toUpperCase();
                    } else if ("greaterThan".equals(op)) {
                        query = fieldName + " > " + value;
                    } else if ("lessThan".equals(op)) {
                        query = fieldName + " < " + value;
                    } else if ("greaterOrEqual".equals(op)) {
                        query = fieldName + " >= " + value;
                    } else if ("lessOrEqual".equals(op)) {
                        query = fieldName + " <= " + value;
                    } else if ("contains".equals(op)) {
                        query = fieldName + " LIKE " + "'%" + value + "%'";
                    } else if ("startsWith".equals(op)) {
                        query = fieldName + " LIKE " + "'" + value + "%'";
                    } else if ("endsWith".equals(op)) {
                        query = fieldName + " LIKE " + "'%" + value + "'";
                    } else if ("iContains".equals(op)) {
                        query = fieldName + ".toUpperCase() LIKE " + "'%" + value.toUpperCase() + "%'";
                    } else if ("iStartsWith".equals(op)) {
                        query = fieldName + ".toUpperCase() LIKE " + "'" + value.toUpperCase() + "%'";
                    } else if ("iEndsWith".equals(op)) {
                        query = fieldName + ".toUpperCase() LIKE " + "'%" + value.toUpperCase() + "'";
                    } else if ("notContains".equals(op)) {
                        query = fieldName + " NOT LIKE " + "'%" + value + "%'";
                    } else if ("notStartsWith".equals(op)) {
                        query = fieldName + " NOT LIKE " + "'" + value + "%'";
                    } else if ("notEndsWith".equals(op)) {
                        query = fieldName + " NOT LIKE " + "'%" + value + "'";
                    } else if ("iNotContains".equals(op)) {
                        query = fieldName + ".toUpperCase() NOT LIKE " + "'%" + value.toUpperCase() + "%'";
                    } else if ("iNotStartsWith".equals(op)) {
                        query = fieldName + ".toUpperCase() NOT LIKE " + "'" + value.toUpperCase() + "%'";
                    } else if ("iNotEndsWith".equals(op)) {
                        query = fieldName + ".toUpperCase() NOT LIKE " + "'%" + value.toUpperCase() + "'";
                    } else if ("isNull".equals(op)) {
                        query = fieldName + " IS NULL";
                    } else if ("notNull".equals(op)) {
                        query = fieldName + " IS NOT NULL";
                    } else if ("equalsField".equals(op)) {
                        query = fieldName + " = " + value;
                    } else if ("iEqualsField".equals(op)) {
                        query = fieldName + ".toUpperCase() = " + value.toUpperCase();
                    } else if ("iNotEqualField".equals(op)) {
                        query = fieldName + ".toUpperCase() <> " + value.toUpperCase();
                    } else if ("notEqualField".equals(op)) {
                        query = fieldName + "<> " + value;
                    } else if ("greaterThanField".equals(op)) {
                        query = fieldName + " > " + value;
                    } else if ("lessThanField".equals(op)) {
                        query = fieldName + " < " + value;
                    } else if ("greaterOrEqualField".equals(op)) {
                        query = fieldName + " >= " + value;
                    } else if ("lessOrEqualField".equals(op)) {
                        query = fieldName + " <= " + value;
                    } else if ("iBetweenInclusive".equals(op)) {
                        query = fieldName + ".toUpperCase() BETWEEN " + start.toUpperCase() + " AND " + end.toUpperCase();
                    } else if ("betweenInclusive".equals(op)) {
                        query = fieldName + " BETWEEN " + start + " AND " + end;
                    }


                    result += " " + query + " " + operator + " ";
                } else {
                    // build the list of subcriterias or criterions
                    String temp = result;
                    String result1 = buildAdvancedSqlCriteria(jo);
                    result = temp + "(" + result1 + ") " + operator + " ";
                }

            }

            result_query = result.substring(0, result.lastIndexOf(operator));
            result_query = temp_query + result_query;
            //Trimm space from right side Rtrim
            result_query = result_query.replaceAll("\\s+$", "");
        }
        return result_query;
    }

    private StringWriter writeRecords(final Iterator<OIdentifiable> iRecords, final String iFetchPlan,
                                      String iFormat, final int startRow, final int endRow,
                                      final long totalRecords, final int status,String dataSourceName) throws IOException {

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

    private StringWriter writeException(final String exception) throws IOException {

        final String JSON_FORMAT = "type,indent:-1,rid,version,attribSameRow,class";

        final StringWriter buffer = new StringWriter();
        final OJSONWriter json = new OJSONWriter(buffer, JSON_FORMAT);
        json.beginObject();
        json.beginObject("response");
        json.writeAttribute("status", 0);


        // WRITE RECORDS
        json.beginCollection(-1, true, "errors");

        json.endCollection(-1, true);
        json.write(exception);
        json.endObject();
        json.endObject();

        return buffer;

    }

    public String getSchema(String className, ODatabaseDocumentTx db) throws Exception {
        final String JSON_FORMAT = "type,indent:-1,rid,version,attribSameRow,class";
        if (db.getMetadata().getSchema().getClass(className) == null)
            throw new OException("Invalid class '" + className + "'");

        final StringWriter buffer = new StringWriter();
        final OJSONWriter json = new OJSONWriter(buffer, JSON_FORMAT);
        OServerCommandGetDatabase.exportClass(db, json, db.getMetadata().getSchema().getClass(className));
        return buffer.toString();
    }

    public String getField(String fieldName) {
        for (int i = 0; i < schemaProperties.size(); i++) {
            JsonObject jo = schemaProperties.get(i).getAsJsonObject();
            String name = jo.get("name").getAsString();
            String type = jo.get("type").getAsString();
            if (name.equals(fieldName)) {
                return type;
            }
        }
        return null;
    }

    public String formatValue(String type, String value, String op) {
        if (("STRING".equals(type) || "DATE".equals(type) || "DATETIME".equals(type)
                || "LINK".equals(type)) && !("contains".equals(op) || "startsWith".equals(op)
                || "endsWith".equals(op) || "iContains".equals(op) || "iStartsWith".equals(op)
                || "iEndsWith".equals(op) || "notContains".equals(op) || "notStartsWith".equals(op)
                || "notEndsWith".equals(op) || "iNotContains".equals(op) || "iNotStartsWith".equals(op)
                || "iNotEndsWith".equals(op))) {

            value = "'" + value + "'";
        }
        return value;
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
//                            Object fieldValue;
                            //Orient stores date type fields with Time 00:00:00 regardless of date format
                            //Strip time from date fields
//                            ORecordSchemaAware myrec = rec.;
//                                 myrec.con
//                            if (iRecord.containsField(p.getName())) {
//
//                            }
//                            for (OProperty p : _clazz.properties()) {
//                                validateField(this, p);
//                            }

                          //  test(rec);
                          //  rec.
                          //  ORecordSchemaAwareAbstract Orec = rec.getRecord();
                            //test(Orec);
                            //test2(rec);
                            objectJson = rec.getRecord().toJSON(format);

                           // json.writeAttribute(settings.indentLevel + 1, true, "value", record.value());
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
//    public static void test2(ORecordSchemaAwareAbstract iRecord) throws OValidationException {
//        ODocument Odoc = ((ODocument) iRecord);
//            OClass myclass = Odoc.getSchemaClass();
//
//       // ((ODocument) iRecord).ge
//
//
////        for (int i = 0; i < Odoc.fields(); i++) {
////            Odoc.fields(i);
////
////        }
//        for (OProperty p : myclass.properties()) {
//            if (p.getType().equals(OType.DATE)) {
//                if (Odoc.containsField(p.getName())){
//                  Odoc
//                }
//            }
//        }
////
////        String test = "";
//          //for(Object : iRecord.f)
//    }
//    public static void test(ORecordSchemaAwareAbstract<?> iRecord) throws OValidationException {
//        final Object fieldValue;
//             //for(field<ODocument> : iRecord.fie)
////        if (iRecord.containsField(p.getName())) {
////            if (iRecord instanceof ODocument)
////                // AVOID CONVERSIONS: FASTER!
////                fieldValue = ((ODocument) iRecord).rawField(p.getName());
////            else
////                fieldValue = iRecord.field(p.getName());
////        }
////        final OType type = p.getType();
////         if (p.getType().equals(OType.DATE)) {
////
////         }
////        if (fieldValue != null && type != null) {
////            // CHECK TYPE
////            switch (type) {
////                case LINK:
////                    validateLink(p, fieldValue);
////                    break;
//    }
}
