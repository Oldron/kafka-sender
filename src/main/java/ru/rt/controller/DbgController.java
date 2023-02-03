package ru.rt.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Api(tags = "Контроллер отладки")
@Slf4j
@CrossOrigin
@RequiredArgsConstructor
public class DbgController {

    @GetMapping(value = {"/ok"})
    @ApiOperation("Проверка доступности")
    public ResponseEntity<String> ok() {
        return new ResponseEntity<>("OK", HttpStatus.OK);
    }
}
