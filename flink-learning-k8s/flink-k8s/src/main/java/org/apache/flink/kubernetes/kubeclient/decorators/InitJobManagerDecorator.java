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

package org.apache.flink.kubernetes.kubeclient.decorators;

import io.fabric8.kubernetes.api.model.*;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesToleration;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.utils.Constants.*;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An initializer for the JobManager {@link org.apache.flink.kubernetes.kubeclient.FlinkPod}.
 */
public class InitJobManagerDecorator extends AbstractKubernetesStepDecorator {

	private final KubernetesJobManagerParameters kubernetesJobManagerParameters;

	public InitJobManagerDecorator(KubernetesJobManagerParameters kubernetesJobManagerParameters) {
		this.kubernetesJobManagerParameters = checkNotNull(kubernetesJobManagerParameters);
	}

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
		final Pod basicPod = new PodBuilder(flinkPod.getPod())
			.withApiVersion(API_VERSION)
			.editOrNewMetadata()
				.withLabels(kubernetesJobManagerParameters.getLabels())
				.withAnnotations(kubernetesJobManagerParameters.getAnnotations())
				.endMetadata()
			.editOrNewSpec()
				.withServiceAccountName(kubernetesJobManagerParameters.getServiceAccount())
				.withHostNetwork(kubernetesJobManagerParameters.isHostNetworkEnabled())
				.withDnsPolicy(
					kubernetesJobManagerParameters.isHostNetworkEnabled()
						? DNS_PLOICY_HOSTNETWORK
						: DNS_PLOICY_DEFAULT)
				.withImagePullSecrets(kubernetesJobManagerParameters.getImagePullSecrets())
				.withNodeSelector(kubernetesJobManagerParameters.getNodeSelector())
				.withTolerations(kubernetesJobManagerParameters.getTolerations().stream()
					.map(e -> KubernetesToleration.fromMap(e).getInternalResource())
					.collect(Collectors.toList()))
				.endSpec()
			.build();

		final Container basicMainContainer = decorateMainContainer(flinkPod.getMainContainer());

		return new FlinkPod.Builder(flinkPod)
			.withPod(basicPod)
			.withMainContainer(basicMainContainer)
			.build();
	}

	private Container decorateMainContainer(Container container) {
		//JM 资源描述
		final ResourceRequirements requirements = KubernetesUtils.getResourceRequirements(
				kubernetesJobManagerParameters.getJobManagerMemoryMB(),
				kubernetesJobManagerParameters.getJobManagerMemoryRequestFactor(),
				kubernetesJobManagerParameters.getJobManagerCPU(),
				kubernetesJobManagerParameters.getJobManagerCPURequestFactor(),
				Collections.emptyMap());

		return new ContainerBuilder(container)
				.withName(kubernetesJobManagerParameters.getJobManagerMainContainerName())
				.withImage(kubernetesJobManagerParameters.getImage())
				.withImagePullPolicy(kubernetesJobManagerParameters.getImagePullPolicy().name())
				.withResources(requirements)
				.withPorts(getContainerPorts())
				.withEnv(getCustomizedEnvs())
				.addNewEnv()
					.withName(ENV_FLINK_HOST_IP_ADDRESS)
					.withValueFrom(new EnvVarSourceBuilder()
						.withNewFieldRef(API_VERSION, HOST_IP_FIELD_PATH)
						.build())
					.endEnv()
				.addNewEnv()
					.withName(ENV_FLINK_POD_IP_ADDRESS)
					.withValueFrom(new EnvVarSourceBuilder()
						.withNewFieldRef(API_VERSION, POD_IP_FIELD_PATH)
						.build())
					.endEnv()
				.addNewEnv()
					.withName("CLUSTER_ID")
					.withValue(kubernetesJobManagerParameters.getClusterId())
					.endEnv()
				.build();
	}

	private List<ContainerPort> getContainerPorts() {
		if (kubernetesJobManagerParameters.isHostNetworkEnabled()) {
			return Collections.emptyList();
		}
		return Arrays.asList(
			new ContainerPortBuilder()
				.withName(Constants.REST_PORT_NAME)
				.withContainerPort(kubernetesJobManagerParameters.getRestPort())
				.build(),
			new ContainerPortBuilder()
				.withName(Constants.JOB_MANAGER_RPC_PORT_NAME)
				.withContainerPort(kubernetesJobManagerParameters.getRPCPort())
				.build(),
			new ContainerPortBuilder()
				.withName(Constants.BLOB_SERVER_PORT_NAME)
				.withContainerPort(kubernetesJobManagerParameters.getBlobServerPort())
				.build());
	}

	private List<EnvVar> getCustomizedEnvs() {
		return kubernetesJobManagerParameters.getEnvironments()
			.entrySet()
			.stream()
			.map(kv -> new EnvVarBuilder()
					.withName(kv.getKey())
					.withValue(kv.getValue())
					.build())
			.collect(Collectors.toList());
	}
}
