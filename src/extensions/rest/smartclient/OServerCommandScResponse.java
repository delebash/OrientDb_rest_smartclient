package extensions.rest.smartclient;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.commons.lang.StringUtils;

public class OServerCommandScResponse {
    private ODocument oDoc = null;
    private String operationType = "";
    private boolean fieldErrors = false;
    private boolean serverError = false;
    private String serverErrorsString = "";
    private Integer status = 0;
    private static int queueStatus = 0;
    private FieldErrors scfieldErrors = null;

    public void parseFieldErrors(String OvalidationErrorString) {
        this.scfieldErrors = new FieldErrors(OvalidationErrorString);
    }

    public static int getQueueStatus() {
        return queueStatus;
    }

    public static void setQueueStatus(int queueStatus) {
        OServerCommandScResponse.queueStatus = queueStatus;
    }

    public ODocument getoDoc() {
        return oDoc;
    }

    public void setoDoc(ODocument oDoc) {
        this.oDoc = oDoc;
    }

    public boolean isFieldErrors() {
        return fieldErrors;
    }

    public void setFieldErrors(boolean fieldErrors) {
        this.fieldErrors = fieldErrors;
    }

    public boolean isServerError() {
        return serverError;
    }

    public void setServerError(boolean serverError) {
        this.serverError = serverError;
    }

    public String getServerErrorsString() {
        return serverErrorsString;
    }

    public void setServerErrorsString(String serverErrorsString) {
        this.serverErrorsString = serverErrorsString;
    }

    public String getOperationType() {
        return operationType;
    }

    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public FieldErrors getScfieldErrors() {
        return scfieldErrors;
    }

    public void setScfieldErrors(FieldErrors scfieldErrors) {
        this.scfieldErrors = scfieldErrors;
    }

    class FieldErrors {
        private  String originalErrorMsg;
        private String beginErrorMsg;
        private String errorMsg;
        private String fieldName;
        private String className;
        private String scErrorMsgString;

        FieldErrors(String errorString) {
            String[] split = errorString.split("field");
            String beginErrorMsg = split[0].trim();
            String errorMsg = split[1].trim();
            String classFieldString = StringUtils.substringBetween(errorMsg, "'", "'").trim();
            errorMsg = StringUtils.remove(errorMsg,classFieldString).trim();
            errorMsg = StringUtils.substring(errorMsg,2).trim();


            String[] classField = StringUtils.split(classFieldString, ".");
            String className = classField[0];
            String fieldName = classField[1];

            this.originalErrorMsg = errorString;
            this.beginErrorMsg = beginErrorMsg;
            this.errorMsg = errorMsg;
            this.fieldName = fieldName;
            this.className = className;
            this.scErrorMsgString = "The field " + fieldName + " " + errorMsg;
        }


        String getBeginErrorMsg() {
            return beginErrorMsg;
        }

        void setBeginErrorMsg(String beginErrorMsg) {
            this.beginErrorMsg = beginErrorMsg;
        }

        String getErrorMsg() {
            return errorMsg;
        }

        void setErrorMsg(String errorMsg) {
            this.errorMsg = errorMsg;
        }

        String getFieldName() {
            return fieldName;
        }

        void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        String getClassName() {
            return className;
        }

        void setClassName(String className) {
            this.className = className;
        }

        String getOriginalErrorMsg() {
            return originalErrorMsg;
        }

        void setOriginalErrorMsg(String originalErrorMsg) {
            this.originalErrorMsg = originalErrorMsg;
        }

        String getScErrorMsgString() {
            return scErrorMsgString;
        }

        void setScErrorMsgString(String scErrorMsgString) {
            this.scErrorMsgString = scErrorMsgString;
        }
    }
}
