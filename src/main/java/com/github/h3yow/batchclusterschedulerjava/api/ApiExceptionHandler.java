package com.github.h3yow.batchclusterschedulerjava.api;

import com.github.h3yow.batchclusterschedulerjava.exception.JobRejectedException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(JobRejectedException.class)
    public ResponseEntity<ErrorResponse> handle(JobRejectedException ex) {
        HttpStatus status = switch (ex.reason()) {
            case JOB_NOT_FOUND, NODE_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case QUOTA_EXCEEDED, INVALID_REQUEST -> HttpStatus.BAD_REQUEST;
            case INVALID_STATE_TRANSITION -> HttpStatus.CONFLICT;
        };
        return ResponseEntity.status(status)
                .body(new ErrorResponse(ex.reason(), ex.getMessage()));
    }
}
