/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.grpc;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractIdleService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContextBuilder;
import io.pravega.common.LoggerHelpers;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.rpc.auth.PravegaAuthManager;
import io.pravega.controller.server.rpc.grpc.v1.ControllerServiceImpl;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManagerFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

/**
 * gRPC based RPC Server for the Controller.
 */
@Slf4j
public class GRPCServer extends AbstractIdleService {

    private final String objectId;
    private final Server server;
    private final GRPCServerConfig config;
    @Getter
    private final PravegaAuthManager pravegaAuthManager;

    /**
     * Create gRPC server on the specified port.
     *
     * @param controllerService The controller service implementation.
     * @param serverConfig      The RPC Server config.
     */
    public GRPCServer(ControllerService controllerService, GRPCServerConfig serverConfig) {
        this.objectId = "gRPCServer";
        this.config = serverConfig;
        ServerBuilder<?> builder = ServerBuilder
                .forPort(serverConfig.getPort())
                .addService(new ControllerServiceImpl(controllerService, serverConfig.getTokenSigningKey(), serverConfig.isAuthorizationEnabled()));
        if (serverConfig.isAuthorizationEnabled()) {
            this.pravegaAuthManager = new PravegaAuthManager(serverConfig);
            this.pravegaAuthManager.registerInterceptors(builder);
        } else {
            this.pravegaAuthManager = null;
        }

        if (serverConfig.isTlsEnabled() && !Strings.isNullOrEmpty(serverConfig.getTlsKeyStoreFile())) {
            KeyStore keyStore;
            try (final InputStream is = new FileInputStream(serverConfig.getTlsKeyStoreFile())) {
                keyStore = KeyStore.getInstance("JKS");
                String password = getPasswordFromFile(serverConfig.getTlsKeyPasswordFile());
                keyStore.load(is, password.toCharArray());
                final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
                        .getDefaultAlgorithm());
                kmf.init(keyStore, password.toCharArray());
                builder = ((NettyServerBuilder) builder).sslContext(GrpcSslContexts.configure(SslContextBuilder.forServer(kmf)).build());
            } catch (IOException | CertificateException | UnrecoverableKeyException
                    | NoSuchAlgorithmException | KeyStoreException e) {
                log.warn("Error setting up SSL for server. Falling back to non SSL connection", e);
            }
        }
        this.server = builder.build();
    }

    private String getPasswordFromFile(String tlsKeyPasswordFile) throws IOException {
        byte[] pwd;

        if (Strings.isNullOrEmpty(tlsKeyPasswordFile)) {
            return "";
        }
        File passwdFile = new File(tlsKeyPasswordFile);
        if (passwdFile.length() == 0) {
            return "";
        }
        return new String(FileUtils.readFileToByteArray(passwdFile)).trim();
    }

    /**
     * Start gRPC server.
     */
    @Override
    protected void startUp() throws Exception {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "startUp");
        try {
            log.info("Starting gRPC server listening on port: {}", this.config.getPort());
            this.server.start();
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "startUp", traceId);
        }
    }

    /**
     * Gracefully stop gRPC server.
     */
    @Override
    protected void shutDown() throws Exception {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "shutDown");
        try {
            log.info("Stopping gRPC server listening on port: {}", this.config.getPort());
            this.server.shutdown();
            log.info("Awaiting termination of gRPC server");
            this.server.awaitTermination();
            log.info("gRPC server terminated");
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "shutDown", traceId);
        }
    }
}
