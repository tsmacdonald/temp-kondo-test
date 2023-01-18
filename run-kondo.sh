#!/usr/bin/env sh
clj-kondo --config .clj-kondo/config.edn --config-dir .clj-kondo/ --lint src/
