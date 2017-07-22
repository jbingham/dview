# Copyright 2017 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#		 http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ABS_PATH := $(shell dirname	$(realpath $(lastword $(MAKEFILE_LIST))))
ABS_PATH_ESCAPED := $(subst /,\/,$(ABS_PATH))

PYTHON_VERSION := $(wordlist 2,4,$(subst ., ,$(shell python --version 2>&1)))
PYTHON_MAJOR_VERSION := $(word 1,${PYTHON_VERSION})
PYTHON_MINOR_VERSION := $(word 2,${PYTHON_VERSION})
PIP_VERSION := $(wordlist 2,4,$(subst ., ,$(shell pip --version)))
PIP_MAJOR_VERSION := "$(word 1, ${PIP_VERSION})"

all: checkversions clone virtualenv path

checkversions:
ifneq "$(PYTHON_MAJOR_VERSION).$(PYTHON_MINOR_VERSION)" "2.7"
	$(error Bad python version. Please install 2.7.x)
endif
ifeq ($(PIP_MAJOR_VERSION),$(filter $(PIP_MAJOR_VERSION),"1" "2" "3" "4" "5" "6"))
	$(error Bad pip version $(PIP_MAJOR_VERSION). Please install >= 7.0.0)
endif
	@echo Python and pip versions are valid

clone:
	mkdir -p install
	cd install && \
		git clone https://github.com/googlegenomics/dsub.git

virtualenv:
	pip install --upgrade virtualenv
	virtualenv install/local_env
	source install/local_env/bin/activate && \
		cd install/dsub && \
		python setup.py install && \
		python setup.py sdist
	source install/local_env/bin/activate && \
		python setup.py install && \
		python setup.py sdist

path:
	touch ~/.bash_profile && \
		echo "export PATH=\$$PATH:$(ABS_PATH)/bin" >> ~/.bash_profile

clean:
	rm -rf install
	sed -i -e '/$(ABS_PATH_ESCAPED)/d' ~/.bash_profile

