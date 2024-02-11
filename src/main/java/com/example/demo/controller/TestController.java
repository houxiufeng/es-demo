package com.example.demo.controller;

import com.example.demo.controller.vos.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class TestController {

    /**
     * This method is a sample controller that returns a greeting message.
     *
     * @return a greeting message
     */
    @RequestMapping("hi")
    public String sayHi() {
        return "hi es-demo";
    }

    @RequestMapping("hello")
    public Result<String> sayHello() {
        log.info("test sayHello");
        return Result.success("bingo");
    }
}
