TAG    			:= $$(git describe --tags)
REGISTRY		:= registry.nersc.gov
PROJECT 		:= als
REGISTRY_NAME	:= ${REGISTRY}/${PROJECT}/${IMG}

TAG_BASE  		:= splash_flows_globus
LOCAL_TAG    	:= ${TAG_BASE}:${TAG}
REGISTRY_TAG	:= ${REGISTRY}/${PROJECT}/${TAG_BASE}:${TAG}



.PHONY: build

hello:
	@echo "Hello" ${REGISTRY}

build:
	@echo "TAG " ${TAG}
	@podman build -t ${TAG} . --platform=linux/amd64
	@echo "tagging to: " ${TAG}    ${REGISTRY_TAG}
	@podman tag ${TAG} ${REGISTRY_TAG}
 
push:
	@echo "Pushing " ${REGISTRY_TAG}
	@podman push ${REGISTRY_TAG}

