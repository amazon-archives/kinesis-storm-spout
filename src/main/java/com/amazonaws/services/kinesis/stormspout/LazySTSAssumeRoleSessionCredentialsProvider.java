package com.amazonaws.services.kinesis.stormspout;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;

public class LazySTSAssumeRoleSessionCredentialsProvider implements AWSCredentialsProvider {
  private final AWSCredentialsProvider baseProvider;
  private final String roleArn;
  private final String roleSessionName;

  private STSAssumeRoleSessionCredentialsProvider provider = null;

  public LazySTSAssumeRoleSessionCredentialsProvider(AWSCredentialsProvider baseProvider, String roleArn, String roleSessionName) {
    this.baseProvider = baseProvider;
    this.roleArn = roleArn;
    this.roleSessionName = roleSessionName;
  }

  private synchronized AWSCredentialsProvider getProvider() {
    if (provider == null) {
      provider = new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, roleSessionName)
              .withStsClient(AWSSecurityTokenServiceClientBuilder.standard()
                      .withCredentials(new DefaultAWSCredentialsProviderChain())
                      .build())
              .build();
    }

    return provider;
  }

  @Override
  public AWSCredentials getCredentials() {
    return getProvider().getCredentials();
  }

  @Override
  public void refresh() {
    getProvider().refresh();
  }
}
