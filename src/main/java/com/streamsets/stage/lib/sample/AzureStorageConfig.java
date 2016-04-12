package com.streamsets.stage.lib.sample;

/*
  Azure storage configuration properties
 */
public class AzureStorageConfig {

    private String account;
    private String accessKey;
    private String connectionString;
    private String directoryDelimiter;
    private String container;
    private String path;

    /*
      Getter and setters
     */

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }


    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public String getDirectoryDelimiter() {
        return directoryDelimiter;
    }

    public void setDirectoryDelimiter(String directoryDelimiter) {
        this.directoryDelimiter = directoryDelimiter;
    }

    public String getContainer() {
        return container;
    }

    public void setContainer(String container) {
        this.container = container;
    }
    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
