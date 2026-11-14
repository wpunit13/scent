package com.scent.trino.exceptions;

import java.io.IOException;

public class ParquetSchemaMismatchException extends IOException {
    public ParquetSchemaMismatchException(String message) {
        super(message);
    }
}
