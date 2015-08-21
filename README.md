# New Relic Aerospike Plugin - Java

Trend and monitor Aerospike statistics using the New Relic Aerospike plugin. Plugin gathers metrics from your Aerospike and sends the metrics to the New Relic Platform.
Collection and visualization of various metric from Aerospike includes :

- Node Statistics
- Latency
- Namespaces Statistics
- Throughput

**Note** - Namespace tab is not present on the default dashboards. User can visualize namespace metrics as per their namespaces.

Example: 

```
Component/aerospike/{cluster_name}/{namespace_name}/{some_namespace_metric[]}
```

----

## Requirements

- A New Relic account.
- Java Runtime (JRE) environment Version 1.6 or later
- Running Aerospike Server.
- Network access to New Relic.

----

## Installation

This plugin can be installed one of the following ways:

* [Option 1 - New Relic Platform Installer](#option-1--install-with-the-new-relic-platform-installer)
* [Option 2 - Manual Install](#option-2--install-manually)

### Option 1 - Install with the New Relic Platform Installer

The New Relic Platform Installer (NPI) is a simple, lightweight command line tool that helps you easily download, configure and manage New Relic Platform Plugins.  To learn more simply go to [new relic forum category](https://discuss.newrelic.com/category/platform-plugins/platform-installer) and checkout the ['Getting Started' section](https://discuss.newrelic.com/t/getting-started-for-the-platform-installer/842).

Once you've installed the NPI tool, run the following command:

```
	./npi install com.aerospike.newrelic.connector
```	

This command will take care of the creation of `newrelic.json` and `plugin.json` configuration files.  See the [configuration information](#configuration-information) section for more information.

### Option 2 - Install Manually (Non-standard)

#### Step 1 - Downloading and Extracting the Plugin

The latest version of the plugin can be downloaded [here](https://github.com/aerospike/newrelic-plugin/tree/master/NewRelic/dist) .Once the plugin is on your box, extract it to a location of your choosing.

**Note** - This plugin is distributed in tar.gz format and can be extracted with the following command on Unix-based systems (Windows users will need to download a third-party extraction tool or use the [New Relic Platform Installer](https://discuss.newrelic.com/t/getting-started-with-the-platform-installer/842)):

```
	tar -xvzf newrelic-aerospike-plugin-X.Y.Z.tar.gz
```

#### Step 2 - Configuring the Plugin

Check out the [configuration information](#configuration-information) section for details on configuring your plugin. 

#### Step 3 - Running the Plugin

To run the plugin, execute the following command from a terminal or command window (assuming Java is installed and on your path):

```
	java -Xmx128m -jar plugin.jar
```

**Note:** - Though it is not necessary, the '-Xmx128m' flag is highly recommended due to the fact that when running the plugin on a server class machine, the `java` command will start a JVM that may reserve up to one quarter (25%) of available memory, but the '-Xmx128m' flag will limit heap allocation to a more reasonable 128MBs.  

For more information on JVM server class machines and the `-Xmx` JVM argument, see: 

 - [http://docs.oracle.com/javase/6/docs/technotes/guides/vm/server-class.html](http://docs.oracle.com/javase/6/docs/technotes/guides/vm/server-class.html)
 - [http://docs.oracle.com/cd/E22289_01/html/821-1274/configuring-the-default-jvm-and-java-arguments.html](http://docs.oracle.com/cd/E22289_01/html/821-1274/configuring-the-default-jvm-and-java-arguments.html)

----

## Configuration Information

### Configuration Files

You will need to modify two configuration files in order to set this plugin up to run.  The first (`newrelic.json`) contains configurations used by all Platform plugins (e.g. license key, logging information, proxy settings) and can be shared across your plugins.  The second (`plugin.json`) contains data specific to each plugin such as a list of hosts and port combination for what you are monitoring.  Templates for both of these files should be located in the '`config`' directory in your extracted plugin folder.

#### Configuring the `plugin.json` file: 

The `plugin.json` file has a provided template in the `config` directory named `plugin.template.json`.  If you are installing manually, make a copy of this template file and rename it to `plugin.json` (the New Relic Platform Installer will automatically handle creation of configuration files for you).  

Below is an example of the `plugin.json` file's contents :

```
{{
  "agents": [
  	{
      "host" : "ip",
      "port" : "port",
      "user" : "username",
      "password" : "password",
      "clusterName": "Cluster Name"
    }
  ]
}
```

**Note** 
- If running community/non-secure Aerospike server then use user/password as **n/a**.
- The `clusterName` attribute is used as base for metric naming convention in the New Relic UI. 

#### Configuring the `newrelic.json` file: 

The `newrelic.json` file also has a provided template in the `config` directory named `newrelic.template.json`.  If you are installing manually, make a copy of this template file and rename it to `newrelic.json` (again, the New Relic Platform Installer will automatically handle this for you).  

The `newrelic.json` is a standardized file containing configuration information that applies to any plugin (e.g. license key, logging, proxy settings), so going forward you will be able to copy a single `newrelic.json` file from one plugin to another.  Below is a list of the configuration fields that can be managed through this file:

##### Configuring your New Relic License Key

Your New Relic license key is the only required field in the `newrelic.json` file as it is used to determine what account you are reporting to.  If you do not know what your license key is, you can learn about it [here](https://newrelic.com/docs/subscriptions/license-key).

Example: 

```
{
  "license_key": "YOUR_LICENSE_KEY_HERE"
}
```

##### Logging configuration

By default Platform plugins will have their logging turned on; however, you can manage these settings with the following configurations:

`log_level` - The log level. Valid values: [`debug`, `info`, `warn`, `error`, `fatal`]. Defaults to `info`.

`log_file_name` - The log file name. Defaults to `newrelic_plugin.log`.

`log_file_path` - The log file path. Defaults to `logs`.

`log_limit_in_kbytes` - The log file limit in kilobytes. Defaults to `25600` (25 MB). If limit is set to `0`, the log file size would not be limited.

Example:

```
{
  "license_key": "YOUR_LICENSE_KEY_HERE"
  "log_level": "info",
  "log_file_path": "/var/logs/newrelic"
}
```

##### Proxy configuration

If you are running your plugin from a machine that runs outbound traffic through a proxy, you can use the following optional configurations in your `newrelic.json` file:

`proxy_host` - The proxy host (e.g. `webcache.example.com`)

`proxy_port` - The proxy port (e.g. `8080`).  Defaults to `80` if a `proxy_host` is set

`proxy_username` - The proxy username

`proxy_password` - The proxy password

Example:

```
{
  "license_key": "YOUR_LICENSE_KEY_HERE",
  "proxy_host": "proxy.mycompany.com",
  "proxy_port": 9000
}
```

----

## Support

Plugin support and troubleshooting assistance can be obtained by visiting [here](https://discuss.aerospike.com/)


