package org.apache.flink.kubernetes.kubeclient.decorators;


import io.fabric8.kubernetes.api.model.HasMetadata;

import org.apache.flink.kubernetes.kubeclient.FlinkPod;

import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;

import org.apache.flink.kubernetes.utils.Constants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Mount the custom Hive Configuration to the JobManager(s)/TaskManagers. We provide one options:
 * 1. Create and mount a dedicated ConfigMap containing the custom Hive configuration from a local directory
 *    specified via the HIVE_CONF_DIR or HIVE_HOME environment variable.
 */
public class HiveConfMountDecorator extends AbstractKubernetesStepDecorator {

	private static final Logger LOG = LoggerFactory.getLogger(HadoopConfMountDecorator.class);

	private final AbstractKubernetesParameters kubernetesParameters;

	public HiveConfMountDecorator(AbstractKubernetesParameters kubernetesParameters) {
		this.kubernetesParameters = checkNotNull(kubernetesParameters);
	}


	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
		return super.decorateFlinkPod(flinkPod);
	}

	@Override
	public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
		return super.buildAccompanyingKubernetesResources();
	}


	private List<File> getHiveConfigurationFileItems(String localHiveConfigurationDirectory) {
		final List<String> expectedFileNames = new ArrayList<>();
		expectedFileNames.add("hive-site.xml");

		final File directory = new File(localHiveConfigurationDirectory);
		if (directory.exists() && directory.isDirectory()) {
			return Arrays.stream(Objects.requireNonNull(directory.listFiles()))
				.filter(file -> file.isFile() && expectedFileNames.stream().anyMatch(name -> file.getName().equals(name)))
				.collect(Collectors.toList());
		} else {
			return Collections.emptyList();
		}
	}

	public static String getHiveConfigMapName(String clusterId) {
		return Constants.HIVE_CONF_CONFIG_MAP_PREFIX + clusterId;
	}
}
