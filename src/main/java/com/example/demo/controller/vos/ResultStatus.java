package com.example.demo.controller.vos;


import com.example.demo.common.Constant;

public enum ResultStatus {
    SUCCESS(Constant.SUCCESS, "success"),
    FAIL(Constant.FAIL, "fail"),
    INTERNAL_SERVER_ERROR(Constant.INTERNAL_SERVER_ERROR, "unknown exception");

    private int code;
    private String message;

    ResultStatus(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

}
