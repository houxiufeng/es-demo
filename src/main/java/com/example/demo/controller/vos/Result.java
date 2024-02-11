package com.example.demo.controller.vos;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Result<T> {
    private T data;
    private String message;
    private Integer code;

    public Result(T data) {
        this.data = data;
        this.code = ResultStatus.SUCCESS.getCode();
        this.message = ResultStatus.SUCCESS.getMessage();
    }

    public Result(Integer code, String message) {
        this.message = message;
        this.code = code;
    }

    public Result(Integer code, T data, String message) {
        this.data = data;
        this.message = message;
        this.code = code;
    }

    public static Result success() {
        Result result = new Result();
        result.setCode(ResultStatus.SUCCESS.getCode());
        result.setMessage(ResultStatus.SUCCESS.getMessage());
        return result;
    }

    public static <T> Result<T> success(T data) {
        Result<T> result = new Result<T>();
        result.setCode(ResultStatus.SUCCESS.getCode());
        result.setMessage(ResultStatus.SUCCESS.getMessage());
        result.setData(data);
        return result;
    }

    public static <T> Result<T> error(String message) {
        Result<T> result = new Result<T>();
        result.setCode(ResultStatus.FAIL.getCode());
        result.setMessage(message);
        result.setData(null);
        return result;
    }

    public static <T> Result<T> error(Integer code, String message) {
        Result<T> result = new Result<T>();
        result.setCode(code);
        result.setMessage(message);
        result.setData(null);
        return result;
    }

    public static Result build(Integer code, Object data, String message) {
        return new Result(code, data, message);
    }

    public static Result build(ResultStatus resultStatus) {
        Result result = new Result();
        result.setCode(resultStatus.getCode());
        result.setMessage(resultStatus.getMessage());
        return result;
    }

    public static Result build(ResultStatus resultStatus, Object data) {
        Result result = new Result();
        result.setCode(resultStatus.getCode());
        result.setMessage(resultStatus.getMessage());
        result.setData(data);
        return result;
    }
    //third change

}
