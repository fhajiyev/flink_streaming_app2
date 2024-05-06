package com.neurio.app.utils;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.*;
import com.google.gson.Gson;

import java.util.Base64;

public class SecretsManagerUtils {

    private static Gson gson = new Gson();
    private static AWSSecretsManager secretsManager;
    private static AwsClientBuilder.EndpointConfiguration config = new AwsClientBuilder.EndpointConfiguration("secretsmanager.us-east-1.amazonaws.com", "us-east-1");
    private static AWSSecretsManagerClientBuilder clientBuilder = AWSSecretsManagerClientBuilder.standard();

    public static NewRelicSecret getNewRelicSecret(String secretName) {

        clientBuilder.setEndpointConfiguration(config);
        secretsManager = clientBuilder.build();

        String secret = null;

        // In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        // See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        // We rethrow the exception by default.

        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest();
        getSecretValueRequest.setSecretId(secretName);
        GetSecretValueResult getSecretValueResult = null;

        try {
            getSecretValueResult = secretsManager.getSecretValue(getSecretValueRequest);
        } catch (DecryptionFailureException e) {
            // Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (InternalServiceErrorException e) {
            // An error occurred on the server side.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (InvalidParameterException e) {
            // You provided an invalid value for a parameter.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (InvalidRequestException e) {
            // You provided a parameter value that is not valid for the current state of the resource.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (ResourceNotFoundException e) {
            // We can't find the resource that you asked for.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        }

        // Decrypts secret using the associated KMS CMK.
        // Depending on whether the secret is a string or binary, one of these fields will be populated.
        if (getSecretValueResult.getSecretString() != null) {
            secret = getSecretValueResult.getSecretString();
        }
        else {
            secret = new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary().asReadOnlyBuffer()).array());
        }

        return gson.fromJson(secret, NewRelicSecret.class);
    }
}
