#--------------------------------------------------------------------
# Makefile for vmq_kafka_plugin
#
# Provides targets for:
#   - Development build (with debug info)
#   - Production build (optimized, no debug info)
#   - Cleaning build artifacts
#   - Deploying the plugin to a VerneMQ instance
#
# Assumes:
#   - rebar3 is installed and available in PATH
#   - VerneMQ is installed and vmq-admin is in PATH
#--------------------------------------------------------------------

# Project name (used for release folder naming)
APP_NAME = vmq_kafka_plugin

# Version (must match the one in src/$(APP_NAME).app.src)
APP_VSN  = 0.1.0

# Path to VerneMQ plugin directory (adjust to your environment)
VMQ_PLUGIN_DIR = /usr/lib/vernemq/plugins

#--------------------------------------------------------------------
# Default target: development build
#--------------------------------------------------------------------
.PHONY: all
all: dev

#--------------------------------------------------------------------
# Development build
# - Keeps debug_info for better stack traces and debugging
# - Uses the default profile
# - Output goes to _build/default
#--------------------------------------------------------------------
.PHONY: dev
dev:
	rebar3 clean --all
	rebar3 compile

#--------------------------------------------------------------------
# Production build
# - Removes debug_info, enables optimizations (as per prod profile in rebar.config)
# - Compiles everything in the prod profile
# - Output goes to _build/prod
# - Generates a self-contained OTP release
#--------------------------------------------------------------------
.PHONY: prod
prod:
	rebar3 clean --all
	rebar3 as prod tar

#--------------------------------------------------------------------
# Clean all build artifacts (both dev and prod)
#--------------------------------------------------------------------
.PHONY: clean
clean:
	rebar3 clean --all

#--------------------------------------------------------------------
# Deploy plugin to VerneMQ (production build)
# - Copies the compiled plugin and its dependencies to VerneMQ's plugin dir
# - Enables the plugin via vmq-admin
#--------------------------------------------------------------------
.PHONY: deploy
deploy: prod
	# Copy plugin and dependencies to VerneMQ plugin directory
	cp -r _build/prod/rel/$(APP_NAME)/lib/* $(VMQ_PLUGIN_DIR)/
	# Enable the plugin in VerneMQ
	vmq-admin plugin enable --name $(APP_NAME) --path $(VMQ_PLUGIN_DIR)

#--------------------------------------------------------------------
# Remove plugin from VerneMQ
#--------------------------------------------------------------------
.PHONY: undeploy
undeploy:
	vmq-admin plugin disable --name $(APP_NAME)
	rm -rf $(VMQ_PLUGIN_DIR)/$(APP_NAME)-$(APP_VSN)

