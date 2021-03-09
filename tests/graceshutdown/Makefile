# Copyright 2020 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

<<<<<<< HEAD
BASE_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)
OUT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/bin)

include $(BASE_DIR)/Makefile.common
=======
BASE_DIR := $(abspath ${CURDIR}/../..)
OUT_DIR := $(abspath ${CURDIR}/bin)

GO              := GO111MODULE=on go
GOBUILD         := $(GO) build $(BUILD_FLAG) -tags codes
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1

.PHONY: server buildsucc

default: server buildsucc

buildsucc:
	@echo "Build TiDB Server successfully!"

server:
<<<<<<< HEAD
ifeq ($(TARGET), "")
	cd ${BASE_DIR} && \
	CGO_ENABLED=1 $(GOBUILD) -ldflags '$(LDFLAGS)' -o $(OUT_DIR)/tidb-server tidb-server/main.go
else
	cd ${BASE_DIR} && \
	CGO_ENABLED=1 $(GOBUILD) -ldflags '$(LDFLAGS)' -o '$(TARGET)' tidb-server/main.go
endif
=======
	cd ${BASE_DIR} && \
	CGO_ENABLED=1 $(GOBUILD) -ldflags '$(LDFLAGS)' -o $(OUT_DIR)/tidb-server tidb-server/main.go
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
