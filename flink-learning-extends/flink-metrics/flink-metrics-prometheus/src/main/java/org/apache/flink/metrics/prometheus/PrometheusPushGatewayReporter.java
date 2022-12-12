/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.prometheus;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.*;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via Prometheus {@link PushGateway}.
 */
@PublicEvolving
@InstantiateViaFactory(factoryClassName = "org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterFactory")
public class PrometheusPushGatewayReporter extends AbstractPrometheusReporter implements Scheduled {

	private PushGateway pushGateway;
	private String jobName;
	private boolean deleteOnShutdown;
	private Map<String, String> groupingKey;
	private String appId;
	private String taskName;
	private String taskId;

	@Override
	public void open(MetricConfig config) {
		super.open(config);

		String host = config.getString(HOST.key(), HOST.defaultValue());
		int port = config.getInteger(PORT.key(), PORT.defaultValue());
		String clusterMode = config.getString(CLUSTER_MODE.key(), CLUSTER_MODE.defaultValue());
		String configuredJobName = config.getString(JOB_NAME.key(), JOB_NAME.defaultValue());
		boolean randomSuffix = config.getBoolean(RANDOM_JOB_NAME_SUFFIX.key(), RANDOM_JOB_NAME_SUFFIX.defaultValue());
		deleteOnShutdown = config.getBoolean(DELETE_ON_SHUTDOWN.key(), DELETE_ON_SHUTDOWN.defaultValue());
		groupingKey = parseGroupingKey(config.getString(GROUPING_KEY.key(), GROUPING_KEY.defaultValue()));

		if (host == null || host.isEmpty() || port < 1) {
			throw new IllegalArgumentException(
				"Invalid host/port configuration. Host: " + host + " Port: " + port);
		}

		Properties properties = System.getProperties();
		taskName = properties.getProperty("taskName", null);
		taskId = properties.getProperty("taskId", null);

		String jobNamePrefix = "";
		if (!StringUtils.isNullOrWhitespaceOnly(clusterMode) && clusterMode
			.toUpperCase()
			.equals(ClusterMode.YARN.name())) {
			//yarn cluster
			appId = System.getenv("_APP_ID");
			if (!StringUtils.isNullOrWhitespaceOnly(appId)) {
				jobNamePrefix = appId + "_jobmanager";
			} else {
				String pwd = System.getenv("PWD");
				String[] values = pwd.split(File.separator);
				String containerId = "";
				if (values.length >= 2) {
					appId = values[values.length - 2];
					containerId = values[values.length - 1];
				}
				jobNamePrefix = appId + "_taskmanager_" + containerId;
			}
		} else if (!StringUtils.isNullOrWhitespaceOnly(clusterMode) && clusterMode.toUpperCase().equals(ClusterMode.K8S.name())) {
			//K8s cluster
			Map<String, String> envs = System.getenv();
			appId = envs.get("CLUSTER_ID");

			if ("k8s".equalsIgnoreCase(host)) {
				// 每台 node 上的 pod 监控指标只发往该 node 上部署的 pushgateway
				host = envs.get("_HOST_IP_ADDRESS");
				log.info("the pod is on K8s cluster, the host ip is {}", host);
			}

			if (appId != null) {
				String hostname = envs.get("HOSTNAME");
				if (hostname.contains("taskmanager")) {
					//taskmanager
					jobNamePrefix = appId + "_taskmanager_" + hostname;
				} else {
					//jobmanager
					jobNamePrefix = appId + "_jobmanager";
				}
			}
		} else {
			jobNamePrefix = configuredJobName;
		}

		if (randomSuffix) {
			this.jobName = jobNamePrefix + "_" + new AbstractID();
		} else {
			this.jobName = jobNamePrefix;
		}

		pushGateway = new PushGateway(host + ':' + port);
		log.info("Configured PrometheusPushGatewayReporter with {host:{}, port:{}, jobName:{}, randomJobNameSuffix:{}, deleteOnShutdown:{}, groupingKey:{}}", host, port, jobName, randomSuffix, deleteOnShutdown, groupingKey);
	}

	Map<String, String> parseGroupingKey(final String groupingKeyConfig) {
		if (!groupingKeyConfig.isEmpty()) {
			Map<String, String> groupingKey = new HashMap<>();
			String[] kvs = groupingKeyConfig.split(";");
			for (String kv : kvs) {
				int idx = kv.indexOf("=");
				if (idx < 0) {
					log.warn("Invalid prometheusPushGateway groupingKey:{}, will be ignored", kv);
					continue;
				}

				String labelKey = kv.substring(0, idx);
				String labelValue = kv.substring(idx + 1);
				if (StringUtils.isNullOrWhitespaceOnly(labelKey)
					|| StringUtils.isNullOrWhitespaceOnly(labelValue)) {
					log.warn(
						"Invalid groupingKey {labelKey:{}, labelValue:{}} must not be empty",
						labelKey,
						labelValue);
					continue;
				}
				groupingKey.put(labelKey, labelValue);
			}

			return groupingKey;
		}
		return Collections.emptyMap();
	}

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
		List<String> dimensionKeys = new LinkedList<>();
		List<String> dimensionValues = new LinkedList<>();

		Map<String, String> allVariables = group.getAllVariables();

		//给每条监控数据增加标签，yarn 为作业的 application id，k8s 则为 cluster id，另外增加实时平台的 task id 和 task name
		if (appId != null) {
			allVariables.put("<app_id>", appId);
		}
		if (taskName != null) {
			allVariables.put("<platform_task_name>", taskName);
		}
		if (taskId != null) {
			allVariables.put("<platform_task_id>", taskId);
		}

		for (final Map.Entry<String, String> dimension : allVariables.entrySet()) {
			final String key = dimension.getKey();
			dimensionKeys.add(CHARACTER_FILTER.filterCharacters(key.substring(1, key.length() - 1)));
			dimensionValues.add(labelValueCharactersFilter.filterCharacters(dimension.getValue()));
		}

		final String scopedMetricName = getScopedName(metricName, group);
		final String helpString = metricName + " (scope: " + getLogicalScope(group) + ")";

		final Collector collector;
		Integer count = 0;

		synchronized (this) {
			if (collectorsWithCountByMetricName.containsKey(scopedMetricName)) {
				final AbstractMap.SimpleImmutableEntry<Collector, Integer> collectorWithCount = collectorsWithCountByMetricName.get(scopedMetricName);
				collector = collectorWithCount.getKey();
				count = collectorWithCount.getValue();
			} else {
				collector = createCollector(metric, dimensionKeys, dimensionValues, scopedMetricName, helpString);
				try {
					collector.register();
				} catch (Exception e) {
					log.warn("There was a problem registering metric {}.", metricName, e);
				}
			}
			addMetric(metric, dimensionValues, collector);
			collectorsWithCountByMetricName.put(scopedMetricName, new AbstractMap.SimpleImmutableEntry<>(collector, count + 1));
		}
	}


	@Override
	public void report() {
		try {
			pushGateway.push(CollectorRegistry.defaultRegistry, jobName, groupingKey);
		} catch (Exception e) {
//			log.warn("Failed to push metrics to PushGateway with jobName {}, groupingKey {}.", jobName, groupingKey, e);
		}
	}

	@Override
	public void close() {
		if (deleteOnShutdown && pushGateway != null) {
			try {
				pushGateway.delete(jobName, groupingKey);
			} catch (IOException e) {
				log.warn("Failed to delete metrics from PushGateway with jobName {}, groupingKey {}.", jobName, groupingKey, e);
			}
		}
		super.close();
	}
}
