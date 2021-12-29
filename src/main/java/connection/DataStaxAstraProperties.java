package connection;

import java.io.File;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datastax.astra")
public class DataStaxAstraProperties {

    private File secureConnectBundle;

    /**
     * @return the secureConnectBundle
     */
    public File getSecureConnectBundle() {
        return secureConnectBundle;
    }

    /**
     * @param secureConnectBundle the secureConnectBundle to set
     */
    public void setSecureConnectBundle(File secureConnectBundle) {
        this.secureConnectBundle = secureConnectBundle;
    }
    

}
