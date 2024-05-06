package com.neurio.app.common.exceptions;

public  class Exceptions {

    public static class ResourceNotFoundException extends RuntimeException  {
        public ResourceNotFoundException(String message) {
            super(message);
        }

        public ResourceNotFoundException(String message, Throwable throwable) {
            super(message, throwable);
        }
    }

    public static class ServiceUnavailableException extends RuntimeException  {
        public ServiceUnavailableException(String message) {
            super(message);
        }

        public ServiceUnavailableException(String message, Throwable throwable) {
            super(message, throwable);
        }
    }
}
