package com.example.demo;

import com.example.demo.controller.vos.Result;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ExtendWith(SpringExtension.class)
public class TestControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void sayHelloReturns200AndBingo() throws Exception {
        // Arrange
        String requestBody = "";

        // Act
        String response = mockMvc.perform(post("/hello")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();

        // Assert
        ObjectMapper mapper = new ObjectMapper();
        Result<String> expectedResponse = mapper.readValue(
                "{\"data\":\"bingo\",\"message\":\"success\",\"code\":0}",
                new TypeReference<Result<String>>() {
                });
        Result<String> actualResponse = mapper.readValue(response, new TypeReference<Result<String>>() {});
        assertEquals(expectedResponse, actualResponse);
    }
}
