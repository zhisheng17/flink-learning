### flink-metrics-prometheus

compile the module and move the target `flink-metrics-prometheus.jar` to flink lib folder, and add metrics reporter configuration in the `flink-config.xml`. eg:

```xml
#==============================================================================
# Metrics Reporter
#==============================================================================

metrics.reporter.promgateway.class: org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter

metrics.reporter.promgateway.host: k8s
# metrics.reporter.promgateway.host: localhost

metrics.reporter.promgateway.port: 9091

metrics.reporter.promgateway.clusterMode: k8s

metrics.reporter.promgateway.jobName: flink-job

metrics.reporter.promgateway.randomJobNameSuffix: false

metrics.reporter.promgateway.deleteOnShutdown: true

metrics.reporter.promgateway.interval: 5 SECONDS

```

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/2022-05-08-072327.png)