package com.github.h3yow.batchclusterschedulerjava.api;

import com.github.h3yow.batchclusterschedulerjava.model.RejectionReason;

/** 拒绝响应：原因可区分。 */
public record ErrorResponse(RejectionReason reason, String message) {}
