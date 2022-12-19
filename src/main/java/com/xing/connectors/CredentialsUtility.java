package com.xing.connectors;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.Builder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.util.StringUtils;

public class CredentialsUtility {

  public static AWSCredentialsProvider createCredentials(String regionName, String roleARN,
      String roleExternalID, String roleSessionName,
      int roleDurationSeconds) {
    if (StringUtils.isNullOrEmpty(roleARN)) {
      return new DefaultAWSCredentialsProviderChain();
    }

    // Use STS if a role is given; assume-role
    final AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder
        .standard()
        .withCredentials(new DefaultAWSCredentialsProviderChain())
        .withRegion(regionName)
        .build();

    STSAssumeRoleSessionCredentialsProvider.Builder providerBuilder = new Builder(roleARN,
        roleSessionName).withStsClient(stsClient);

    if (!StringUtils.isNullOrEmpty(roleExternalID)) {
      providerBuilder.withExternalId(roleExternalID);
    }
    if (roleDurationSeconds > 0) {
      providerBuilder.withRoleSessionDurationSeconds(roleDurationSeconds);
    }
    return providerBuilder.build();
  }

}
